"""真实语义/向量检索（P15）：可装配嵌入提供方 + SQLite 向量索引。

边界（与指示一致）：
- 嵌入是**独立向量 API**（官方 OpenAI 兼容 `POST {base_url}/embeddings` 协议），
  与聊天 thinking 约束无关；聊天/整理/总结仍全部 disabled。
- `model` 不猜：启用时必须显式配置（settings 校验）；默认关闭、默认不联网
  建索引；未配置/失败如实区分，不伪造向量、不把哈希桶包装成语义结果。
- 索引块带 (provider, model, dimension, text_sha256, record_id)：不同模型
  向量不混用；文本版本变化即重写；不索引 Key/临时结果/Base64/文件全文。
- 检索**先按 Agent/会话作用域过滤，再有界近邻**；分数与来源记录可追溯。
- 默认降级路径：不可用/未配置/失败时明确降级关键词检索，绝不声称
  "已执行语义搜索"。
"""

from __future__ import annotations

import hashlib
import logging
import math
from typing import Any, Dict, List, Mapping, Optional, Sequence

import httpx
import json

from pero.core.settings import EmbeddingsSettings
from pero.memory.repository import SqliteMemoryRepository

logger = logging.getLogger(__name__)

#: 检索状态（与关键词检索同一状态家族，另加语义专用项）。
SEMANTIC_OK = "ok"
SEMANTIC_DISABLED = "semantic_disabled"
SEMANTIC_NOT_CONFIGURED = "semantic_not_configured"
SEMANTIC_FAILED = "semantic_failed"
SEMANTIC_NO_HITS = "no_hits"


class EmbeddingError(RuntimeError):
    def __init__(self, code: str, detail: str = "") -> None:
        self.code = code
        super().__init__(code)


class EmbeddingClient:
    """按官方 OpenAI 兼容协议调用 `POST {base_url}/embeddings`（真实向量 API）。

    P15-R01 约束：Key 为空 → not_configured（**不发网络**）；响应流式读取、
    字节硬上限；严格校验条数/顺序（data[].index）/数值类型（拒绝 bool/NaN/
    非有限值）/维度/非零范数。
    """

    def __init__(self, settings: EmbeddingsSettings) -> None:
        self._settings = settings

    @property
    def configured(self) -> bool:
        """启用 + base_url + model + **Key 非空**才算配置完成（缺 Key 不发网络）。"""
        return bool(
            self._settings.enabled
            and self._settings.base_url
            and self._settings.model
            and self._key()
        )

    @property
    def provider_model(self) -> str:
        return f"{self._settings.base_url.rstrip('/')}|{self._settings.model}" if self.configured else ""

    def _key(self) -> str:
        import os

        return os.environ.get(self._settings.api_key_env, "")

    async def embed(self, texts: Sequence[str]) -> List[List[float]]:
        if not self.configured:
            raise EmbeddingError(
                "not_configured",
                "嵌入提供方未配置（enabled/base_url/model/Key 任一缺失）",
            )
        capped = [t[: self._settings.index_max_chars] for t in texts]
        payload = {"model": self._settings.model, "input": capped}
        try:
            async with httpx.AsyncClient(timeout=self._settings.timeout_seconds) as client:
                async with client.stream(
                    "POST",
                    self._settings.base_url.rstrip("/") + "/embeddings",
                    headers={"Authorization": f"Bearer {self._key()}"},
                    json=payload,
                ) as response:
                    if response.status_code != 200:
                        # 不回传上游错误体，只记状态码与短码。
                        raise EmbeddingError(
                            "api_error", f"嵌入 API 返回 {response.status_code}"
                        )
                    buffer = bytearray()
                    async for chunk in response.aiter_bytes():
                        buffer.extend(chunk)
                        if len(buffer) > self._settings.max_response_bytes:
                            raise EmbeddingError(
                                "response_too_large",
                                f"嵌入响应超过 {self._settings.max_response_bytes} 字节上限",
                            )
        except httpx.TimeoutException as exc:
            raise EmbeddingError("timeout", "嵌入请求超时") from exc
        except httpx.HTTPError as exc:
            raise EmbeddingError("http_error", f"嵌入请求失败（{type(exc).__name__}）") from exc
        vectors = self._parse_response(bytes(buffer), len(capped))
        return vectors

    def _parse_response(self, body: bytes, expected: int) -> List[List[float]]:
        import math

        try:
            data = json.loads(body.decode("utf-8"))
            rows = data["data"]
            if not isinstance(rows, list) or len(rows) != expected:
                raise ValueError("条数与请求不一致")
            vectors: List[List[float]] = []
            for position, item in enumerate(rows):
                if item.get("index", position) != position:
                    # 响应顺序必须与输入对应，不按"返回顺序猜"。
                    raise ValueError("index 与输入顺序不一致")
                raw = item["embedding"]
                if not isinstance(raw, list) or len(raw) != self._settings.dimension:
                    raise ValueError(
                        f"嵌入维度应为 {self._settings.dimension}"
                    )
                vector: List[float] = []
                for value in raw:
                    if isinstance(value, bool) or not isinstance(value, (int, float)):
                        raise ValueError("向量含非数值（bool/字符串等）")
                    number = float(value)
                    if not math.isfinite(number):
                        raise ValueError("向量含 NaN/Inf")
                    vector.append(number)
                norm = math.sqrt(sum(x * x for x in vector))
                if norm <= 0:
                    raise ValueError("向量为零范数")
                vectors.append(vector)
        except EmbeddingError:
            raise
        except Exception as exc:  # noqa: BLE001 - 形状/类型不对就是协议错误
            raise EmbeddingError("invalid_response", f"嵌入响应校验失败：{exc}") from exc
        return vectors


class SemanticIndex:
    """增量向量索引 + 有界近邻；元数据区分存在/启用/就绪/连通未验证。"""

    def __init__(
        self,
        settings: EmbeddingsSettings,
        repository: SqliteMemoryRepository,
    ) -> None:
        self._settings = settings
        self._repository = repository
        self._client = EmbeddingClient(settings)

    @property
    def configured(self) -> bool:
        return self._client.configured

    def status(self) -> Dict[str, Any]:
        state = "disabled"
        if self._settings.enabled:
            state = "not_configured" if not self._client.configured else "configured"
        return {
            "state": state,
            "provider_model": self._client.provider_model,
            "connectivity": "not_verified",
            "note": "未连真实嵌入 API；不可用时明确降级关键词检索，不声称已执行语义搜索",
        }

    async def index_texts(
        self,
        *,
        agent_id: str,
        conversation_id: str,
        items: Sequence[Mapping[str, Any]],
    ) -> int:
        """增量索引：items = [{record_id, role, text}]；幂等（同键重写）。

        过滤：空文本 / 超长截断到 index_max_chars（不索引文件全文/附件）。
        """
        if not self.configured:
            return 0
        rows = [
            {
                "record_id": str(item.get("record_id") or "")[:80],
                "role": str(item.get("role") or "user")[:16],
                "text": str(item.get("text") or "")[: self._settings.index_max_chars],
            }
            for item in items
        ]
        rows = [r for r in rows if r["record_id"] and r["text"].strip()]
        if not rows:
            return 0
        rows = rows[: self._settings.index_batch_size]
        try:
            vectors = await self._client.embed([r["text"] for r in rows])
        except EmbeddingError as exc:
            # 失败如实记录（日志短码），不伪造索引完成。
            logger.warning("semantic_index_failed code=%s", exc.code)
            return 0
        written = 0
        for row, vector in zip(rows, vectors):
            written += await self._repository.vector_upsert(
                agent_id=agent_id,
                conversation_id=conversation_id,
                record_id=row["record_id"],
                role=row["role"],
                text=row["text"],
                provider=self._settings.base_url.rstrip("/"),
                model=self._settings.model,
                dimension=self._settings.dimension,
                vector=vector,
            )
        return written

    async def search(
        self,
        *,
        agent_id: str,
        conversation_ids: Sequence[str],
        query: str,
        limit: Optional[int] = None,
        exclude_record_ids: Sequence[str] = (),
        identity_ids: Sequence[str] = (),
    ) -> Dict[str, Any]:
        """先作用域（会话 + 记录类型的真实人物关联）SQL 过滤取有界候选，再算相似度。

        `max_candidates` 传给 SQL 作为取回上限（实际约束运算量）；身份过滤
        **按真实记录类型**在存储层落实（P15-R02）：轮次块按全部原文参与者、
        L3 按 subject、L4 按明确 subject 或来源证据；归属未知的候选不混入
        指定人物的命中。语义未启用/缺 Key/失败 → 明确降级关键词（状态返回），
        不为过滤禁用关键词工具，也不假称命中不存在的归属。
        """
        if not self._settings.enabled:
            return {"state": SEMANTIC_DISABLED, "hits": [], "detail": "语义检索未启用"}
        if not self._client.configured:
            return {"state": SEMANTIC_NOT_CONFIGURED, "hits": [], "detail": "嵌入提供方未配置"}
        limit = max(1, min(int(limit or self._settings.max_candidates), self._settings.max_candidates))
        try:
            query_vectors = await self._client.embed([query])
        except EmbeddingError as exc:
            return {"state": SEMANTIC_FAILED, "hits": [], "detail": f"语义检索失败（{exc.code}）"}
        query_vector = query_vectors[0]
        rows = await self._repository.vector_fetch_scope(
            agent_id=agent_id,
            conversation_ids=list(conversation_ids),
            provider=self._settings.base_url.rstrip("/"),
            model=self._settings.model,
            dimension=self._settings.dimension,
            limit=self._settings.max_candidates,
            identity_ids=tuple(identity_ids),
        )
        excluded = set(exclude_record_ids)
        scored: List[Dict[str, Any]] = []
        for row in rows:
            if row["record_id"] in excluded:
                continue
            score = _cosine(query_vector, row["vector"])
            scored.append({
                "record_id": row["record_id"],
                "role": row["role"],
                "conversation_id": row["conversation_id"],
                "text_sha256": row["text_sha256"],
                "score": round(score, 6),
            })
        scored.sort(key=lambda item: item["score"], reverse=True)
        hits = scored[:limit]
        if not hits:
            return {"state": SEMANTIC_NO_HITS, "hits": [], "detail": "语义检索没有命中"}
        return {"state": SEMANTIC_OK, "hits": hits, "detail": ""}


def _cosine(a: Sequence[float], b: Sequence[float]) -> float:
    if len(a) != len(b) or not a:
        return 0.0
    dot = sum(x * y for x, y in zip(a, b))
    na = math.sqrt(sum(x * x for x in a))
    nb = math.sqrt(sum(y * y for y in b))
    if na <= 0 or nb <= 0:
        return 0.0
    return dot / (na * nb)
