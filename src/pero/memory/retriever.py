"""回答前检索（本阶段的唯一实现：SQLite 参数化关键词检索）。

边界：
- 只读，不写库、不生成长期记忆、不调用模型；
- 身份先行：先把查询里的名字当别名做精确匹配，命中唯一个体才按该身份过滤，
  同名多人时**不挑一个**，而是标记 `identity_ambiguous` 并保留"未按单人过滤"的说明；
- 命中一律带层、记录 ID、时间、来源引用与有界预览，便于模型和前端追溯；
- 没命中、不可用、失败是三种不同状态，调用方不得当成"从未发生"。
"""

from __future__ import annotations

from typing import Any, Dict, Mapping, Optional, Sequence

from pero.core.settings import MemorySettings
from pero.memory.keywords import tokenize
from pero.memory.models import (
    RETRIEVAL_FAILED,
    RETRIEVAL_IDENTITY_AMBIGUOUS,
    RETRIEVAL_NO_HITS,
    RETRIEVAL_OK,
    RETRIEVAL_UNAVAILABLE,
    RetrievalHit,
    RetrievalResult,
)
from pero.memory.repository import SqliteMemoryRepository

#: 组件标识（配置 `agents[].composition.retriever` 引用）。
RETRIEVER_KEY = "sqlite_keyword"


class SqliteKeywordRetriever:
    """把"身份解析 + 有界关键词检索"包装成一个可注册的检索组件。

    P15：可选合并真实语义/向量命中（`set_semantic` 注入，装配层按开关注入）；
    合并有界去重，语义不可用/未配置/失败时**明确降级关键词**，绝不声称
    "已执行语义搜索"。
    """

    def __init__(
        self, repository: SqliteMemoryRepository, settings: Optional[MemorySettings] = None
    ) -> None:
        self._repository = repository
        self._settings = settings or repository.settings
        self._semantic = None

    def set_semantic(self, index) -> None:
        """注入语义索引（`memory/semantic.py::SemanticIndex`）；None = 关闭。"""
        self._semantic = index

    @property
    def available(self) -> bool:
        """记忆存储不可用时检索也不可用（不读空库、不假装没命中）。"""

        return self._repository.available

    async def retrieve(
        self,
        *,
        agent_id: str,
        conversation_id: Optional[str],
        account_id: Optional[str],
        query: str,
        limit: Optional[int] = None,
    ) -> RetrievalResult:
        """按当前 Agent/会话范围检索；返回带状态与说明的结果。"""

        if not self._repository.available:
            return RetrievalResult(
                state=RETRIEVAL_UNAVAILABLE,
                query=query,
                detail="记忆存储未启用，本轮没有进行检索。",
            )

        tokens = tokenize(query)
        identity_id: Optional[str] = None
        ambiguous: Sequence[str] = ()
        note = ""
        try:
            matches = await self._repository.match_alias_tokens(
                agent_id=agent_id,
                account_id=account_id,
                conversation_id=conversation_id,
                tokens=tokens,
            )
            distinct = sorted({str(item["identity_id"]) for item in matches})
            if len(distinct) == 1:
                identity_id = distinct[0]
            elif len(distinct) > 1:
                ambiguous = tuple(distinct)
                note = "查询里的名字对应当前会话中的多个成员，未按单人过滤。"
        except Exception as exc:  # noqa: BLE001 - 检索失败必须如实上报，不能静默
            return RetrievalResult(
                state=RETRIEVAL_FAILED,
                query=query,
                detail=f"身份解析失败（{type(exc).__name__}），本轮没有可用检索结果。",
            )

        try:
            hits, detail = await self._repository.search(
                agent_id=agent_id,
                conversation_id=conversation_id,
                identity_ids=(identity_id,) if identity_id else (),
                query=query,
                limit=limit,
            )
        except Exception as exc:  # noqa: BLE001 - 检索失败要能被上层计数，而不是伪装成没命中
            return RetrievalResult(
                state=RETRIEVAL_FAILED,
                query=query,
                identity_id=identity_id,
                detail=f"检索失败（{type(exc).__name__}），本轮没有可用检索结果。",
            )
        if ambiguous:
            return RetrievalResult(
                state=RETRIEVAL_IDENTITY_AMBIGUOUS,
                query=query,
                hits=tuple(hits),
                detail=note + (f" 检索说明：{detail}" if detail else ""),
                ambiguous_candidates=tuple(ambiguous),
            )

        # P15-R01：关键词 + 语义有界合并去重；**总量受 retrieval_limit 约束**；
        # 语义失败/未配置只降级，不伪装；关键词无命中且语义失败 → failed（不是
        # 纯 no_hits），降级/失败说明保留在 detail。
        semantic_note = ""
        semantic_state = ""
        total_limit = max(1, int(self._settings.retrieval_limit))
        if self._semantic is not None:
            try:
                semantic = await self._semantic.search(
                    agent_id=agent_id,
                    conversation_ids=[conversation_id] if conversation_id else [],
                    query=query,
                    identity_ids=[identity_id] if identity_id else (),
                )
                semantic_state = semantic.get("state") or ""
                if semantic.get("state") == "ok":
                    extra = await self._semantic_hits(
                        agent_id=agent_id, hits=semantic.get("hits") or [],
                        seen={hit.record_id for hit in hits},
                        remaining=max(0, total_limit - len(hits)),
                    )
                    if extra:
                        hits = list(hits) + extra
                        semantic_note = f" 语义命中 {len(extra)} 条（合并去重后）。"
                    else:
                        semantic_note = " 语义检索无新增命中。"
                elif semantic.get("state") in {"semantic_failed", "semantic_not_configured"}:
                    semantic_note = f" 语义检索不可用（{semantic.get('detail')}），已降级关键词。"
            except Exception as exc:  # noqa: BLE001 - 语义合并失败不拖垮关键词结果
                semantic_state = "semantic_failed"
                semantic_note = f" 语义合并失败（{type(exc).__name__}），已降级关键词。"

        if not hits:
            if semantic_state == "semantic_failed":
                # 关键词无命中且语义失败：如实 failed，不变成纯 no_hits。
                return RetrievalResult(
                    state=RETRIEVAL_FAILED,
                    query=query,
                    identity_id=identity_id,
                    detail=(detail or "没有命中。") + semantic_note,
                )
            return RetrievalResult(
                state=RETRIEVAL_NO_HITS,
                query=query,
                identity_id=identity_id,
                detail=(detail or "没有命中。") + semantic_note,
            )
        return RetrievalResult(
            state=RETRIEVAL_OK,
            query=query,
            hits=tuple(hits[:total_limit]),
            identity_id=identity_id,
            detail=detail + semantic_note,
        )

    async def _semantic_hits(
        self, *, agent_id: str, hits: Sequence[Mapping], seen: set, remaining: int
    ) -> list:
        """把语义命中（record_id + score）转成 RetrievalHit（带层/预览/来源）。

        有界：只并入 `remaining` 条（总 retrieval_limit 约束）；
        详情缺失的命中跳过（不造预览）。
        """
        extra = []
        for item in list(hits)[: max(0, int(remaining))]:
            record_id = str(item.get("record_id") or "")
            if not record_id or record_id in seen:
                continue
            record = await self._repository.get_record(
                agent_id=agent_id, record_id=record_id
            )
            if record is None:
                continue
            preview = str(record.get("preview") or record.get("text") or "")[: self._settings.retrieval_item_chars]
            extra.append(
                RetrievalHit(
                    layer=str(record.get("layer") or "raw"),
                    record_id=record_id,
                    occurred_at=str(record.get("occurred_at") or record.get("stored_at") or ""),
                    preview=preview,
                    score=int(round(float(item.get("score") or 0.0) * 1000)),
                    source_refs=(f"semantic:{item.get('text_sha256', '')[:16]}",),
                )
            )
        return extra

    def describe(self) -> Dict[str, Any]:
        """组件描述（注册表与接口使用；不暴露数据库路径）。"""

        return {
            "key": RETRIEVER_KEY,
            "name": "SQLite 关键词检索",
            "kind": "retriever",
            "persistent": True,
            "status": "available" if self._repository.available else "unavailable",
            "description": (
                "回答前按 Agent/会话范围检索 L4 明确保存、事件日记与原文来源；"
                "英文词 + 中文二元组关键词，任一命中，命中数与时间为排序依据。"
            ),
        }
