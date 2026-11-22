"""桥文件字节补传（P13-R02）：**接收侧有界入队** + 独立 worker + 来源状态机。

拓扑（P13-R01/R02 指示）：
- 入队发生在**接收/媒体预处理阶段**：通道在账号归属校验 + 去重之后同步调用
  `offer_from_event`——可信账号、Agent/账号/目标路由验证**且**当前会话属于
  `allowed_conversations`（**空集合 = 全部拒绝**，与编排/图片接收同一定义；
  routes 与 allowed 必须同时满足，任一不是另一个的隐式授权）。文件单独到达
  / 无模型凭据 / 自动处理关闭时，只要补传显式开启仍会保存原件；**不调用
  模型、不开窗**；禁用补传（enabled/file_fetch_enabled 任一关闭）时既不
  入队也不查询、更不入库。模型批次无第二入口。
- 接收协程只入队，绝不 `await` 需要本连接 echo 的查询；查询/校验/入库都在
  独立 worker 任务里调 `server.query_file_bytes`（统一 QueryResult 信封：
  status=ok 只代表查到数据，不代表任何消息 sent，独立 query_* 计数，
  不进 receipt_* 计数）。
- **来源状态机（P13-R02）**：queued →（worker 取出）processing →
  registered / failed（含 integrity/scope/catalog 细分，记在 detail）；
  取消/停止：仍在排队项记 `aborted`，正在处理项记 `unknown`
  （查询可能已在桥执行，无法确认）。aclose 覆盖**活动项与仍排队项**，
  不遗留 queued/processing。
- **同源失败不自动重试**：records 里已有该键（任意状态）的重复 offer 直接
  跳过；只有真正的新来源（新 local_id / 新版本哈希 / 新桥 session）才构成
  新键，而不是清空失败状态。
- **幂等键** = Agent + 完整会话 + 可信来源范围（桥 session，经
  `dedupe_scope`）+ 明确本地来源限定（ext `source_local_id`，缺则省略）+
  文件句柄 + 可得版本（offer sha256 为强版本；只有 size 可用时为**弱版本**，
  如实标记 `weak_version`——同 session 同大小换源无法识别，不保证
  "句柄复用必然变键"，不伪造平台 ID、不读旧库回填）。
- **记录淘汰保护**：状态表有界（LRU）但只淘汰终态记录（registered/failed/
  rejected/aborted/unknown），在途（queued/processing）不被误删导致同源
  并行；无终态可淘汰时新记录不落表（`record_full` 计数），处理照常受
  队列上限约束，满则明确拒绝。
- 硬校验不变（P13-R01 已认可）：Base64 解码前限长、解码后限字节；file_id /
  size / 哈希与 offer 预期及响应逐项比对；响应 group_wxid 与 offer 可信群
  不符即 scope_mismatch 拒绝；缺哈希如实记 hash_unknown。
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import logging
from collections import OrderedDict
from typing import Any, Dict, Optional, Tuple

from pero.core.contracts import QueryStatus
from pero.core.settings import BridgeContextSettings
from pero.resources.catalog import ResourceCatalog, ResourceCatalogError

_logger = logging.getLogger(__name__)

#: 结果记录容量上限（有界；只淘汰终态记录，见 _remember）。
_RECORD_MAX = 128

#: 整块（Base64）回退的硬上限：旧桥无分块能力时，超过该大小明确失败，
#: 不把大文件塞进单帧（避免内存与帧上限问题）。
_WHOLE_MAX_BYTES = 8 * 1024 * 1024

#: 终态：不会再变化，可被 LRU 淘汰；重复 offer 也不再入队（不自动重试）。
_TERMINAL_STATES = frozenset(
    {"registered", "failed", "rejected", "aborted", "unknown"}
)
#: 在途状态：淘汰保护对象（queued/processing 不被 LRU 误删）。
_IN_FLIGHT_STATES = frozenset({"queued", "processing"})


class BridgeFileIngress:
    """按 (Agent, 会话, 桥 session 范围, 本地来源, 句柄, 版本) 限定的只读补传队列。"""

    def __init__(
        self,
        settings: BridgeContextSettings,
        catalog: ResourceCatalog,
        server: Any,
        routes: Optional[Dict[Tuple[str, str, str], str]] = None,
        allowed_conversations: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.settings = settings
        self.catalog = catalog
        self.server = server
        #: {(账号, 会话类型, 目标数字 ID) → agent_id}；与图片接收同一套路由表。
        self._routes: Dict[Tuple[str, str, str], str] = dict(routes or {})
        #: 允许的会话键集合；**空集合 = 全部拒绝**（与编排/图片接收一致）。
        self._allowed = set(allowed_conversations or ())
        self._queue: "asyncio.Queue[Dict[str, Any]]" = asyncio.Queue(maxsize=settings.queue_max)
        self._worker: Optional[asyncio.Task] = None
        #: 来源状态表（key=幂等键；不含字节内容）。
        self._records: "OrderedDict[str, Dict[str, Any]]" = OrderedDict()
        self._counters = {
            "offered": 0,
            "queue_full": 0,
            "route_rejected": 0,
            "fetched": 0,
            "failed": 0,
            "integrity_error": 0,
            "scope_mismatch": 0,
            "duplicate": 0,
            "aborted": 0,
            "record_full": 0,
            "chunk_fallback": 0,
        }

    def metadata(self) -> Dict[str, Any]:
        return {
            "enabled": bool(self.settings.enabled),
            "file_fetch_enabled": bool(self.settings.file_fetch_enabled),
            "file_query_action": self.settings.file_query_action,
            "quote_image_fetch_enabled": bool(self.settings.quote_image_fetch_enabled),
            "quote_image_action": self.settings.quote_image_action,
            "queue_max": int(self.settings.queue_max),
            "max_file_bytes": int(self.settings.max_file_bytes),
            "file_chunk_action": self.settings.file_chunk_action,
            "chunk_bytes": int(self.settings.chunk_bytes),
            "whole_fallback_max_bytes": _WHOLE_MAX_BYTES,
            "bridge_extension": "not_verified",
        }

    def status(self) -> Dict[str, Any]:
        """只报计数与最近结果状态，不含字节/路径。"""

        recent = [
            {key: record.get(key) for key in
             ("file_id", "name", "state", "detail", "resource_id", "weak_version")}
            for record in list(self._records.values())[-8:]
        ]
        return {"counters": dict(self._counters), "recent": recent}

    # ------------------------------------------------------------------ #
    # 接收侧入队（同步、有界、无网络 IO）
    # ------------------------------------------------------------------ #

    def offer_from_event(self, event) -> None:
        """通道接收阶段调用：可信账号/路由/**允许会话**验证后同步入队。

        routes 与 allowed 必须同时满足；allowed 为空集合时全部拒绝
        （不因接收不调用模型而取消用户配置边界）。不影响消息本身投递。
        """

        if not self.settings.enabled or not self.settings.file_fetch_enabled:
            # 禁用补传：不入队、不查询、不入库。
            return
        message = getattr(event, "message", None)
        if message is None:
            return
        offer = getattr(message, "file_offer", None)
        if not offer:
            return
        private = message.conversation_type == 'private'
        access = getattr(self, '_private_access', None)
        grant = access.grant_for(message) if private and access else None
        agent_id = grant['agent_id'] if grant else self._routes.get(
            (message.account_id, message.conversation_type, message.target_ref)
        )
        # 无条件要求会话属于允许集合（空集合全拒）；路由验证保留。
        if (private and not grant) or not agent_id or (not private and message.conversation_id not in self._allowed):
            self._counters["route_rejected"] += 1
            return
        file_id = str(offer.get("file_id") or "")
        group_wxid = str(offer.get("group_wxid") or "")
        # 可信来源范围：桥 session 由 dedupe_scope 承载
        # （bridge_session:<session>；无 ext session 时为 bridge_process）。
        scope = str(getattr(message, "dedupe_scope", "") or "bridge_process")
        # 明确本地来源限定：桥入库记录的消息 local_id（可信 DB 元数据）；
        # 缺则省略该分量——此时同 session 句柄复用不保证换键（弱版本）。
        local_id = str(offer.get("source_local_id") or "")[:64]
        expect_sha = str(offer.get("sha256") or "").strip().lower()
        size = int(offer.get("size") or 0)
        if size <= 0 or size > self.settings.max_file_bytes:
            self._counters["route_rejected"] += 1
            self._remember(
                self._key(agent_id, message.conversation_id, scope,
                          local_id, file_id, expect_sha, size),
                offer, "rejected", "size_out_of_bounds",
                weak=not expect_sha,
            )
            return
        item = {
            "private_grant": (grant['grant_id'], grant['status_revision']) if grant else None,
            "agent_id": agent_id[:120],
            "conversation_id": message.conversation_id[:200],
            "event_id": str(message.event_id or "")[:512],
            "account_id": message.account_id[:80],
            "target_ref": str(message.target_ref or "")[:40],
            "file_id": file_id[:40],
            "name": str(offer.get("name") or file_id)[:200],
            "expect_sha256": expect_sha[:64],
            "expect_size": size,
            "expect_group_wxid": group_wxid[:64],
            "scope": scope[:120],
            "source_local_id": local_id,
        }
        key = self._key(agent_id, message.conversation_id, scope,
                        local_id, file_id, expect_sha, size)
        if key in self._records:
            # 同源幂等（任意状态，含 failed/rejected/aborted/unknown）：
            # 重复 offer 不自动重试；新来源会构成新键，而不是清空失败状态。
            return
        try:
            self._queue.put_nowait(item)
        except asyncio.QueueFull:
            self._counters["queue_full"] += 1
            self._remember(key, offer, "rejected", "queue_full",
                           weak=not expect_sha)
            return
        self._counters["offered"] += 1
        self._remember(key, offer, "queued", "", weak=not expect_sha)

    @staticmethod
    def _key(
        agent_id: str,
        conversation_id: str,
        scope: str,
        local_id: str,
        file_id: str,
        sha256: str,
        size: int,
    ) -> str:
        """幂等键：Agent + 完整会话 + 桥 session 范围 + 本地来源 + 句柄 + 版本。

        版本分量：offer sha256 存在 → 强版本（`sha:<前32>`）；只有 size 可用
        → 弱版本（`size:<n>`），如实标注，不保证同 session 同大小换源换键。
        """

        local = local_id[:24] if local_id else "-"
        version = f"sha:{sha256[:32]}" if sha256 else f"size:{size}"
        return f"{agent_id}:{conversation_id}:{scope}:{local}:{file_id}:{version}"

    def _remember(
        self, key: str, offer: Dict[str, Any], state: str, detail: str,
        resource_id: str = "", weak: bool = False,
    ) -> None:
        """有界状态表写入；**只淘汰终态记录**，在途项不被误删。

        无终态可淘汰且 key 是新键时：不落表（record_full 计数），处理照常；
        状态有界、队列有界，满则明确拒绝，不静默丢状态。
        """

        if key not in self._records:
            evictable = [
                existing for existing, record in self._records.items()
                if record.get("state") in _TERMINAL_STATES
            ]
            if len(self._records) >= _RECORD_MAX and evictable:
                self._records.pop(evictable[0], None)
            elif len(self._records) >= _RECORD_MAX:
                self._counters["record_full"] += 1
                return
        self._records[key] = {
            "file_id": str(offer.get("file_id") or "")[:40],
            "name": str(offer.get("name") or "")[:120],
            "state": state,
            "detail": detail[:120],
            "resource_id": resource_id,
            "weak_version": bool(weak),
        }

    def _item_key(self, item: Dict[str, Any]) -> str:
        return self._key(
            item.get("agent_id", ""), item.get("conversation_id", ""),
            item.get("scope", ""), item.get("source_local_id", ""),
            item.get("file_id", ""), item.get("expect_sha256", ""),
            int(item.get("expect_size") or 0),
        )

    # ------------------------------------------------------------------ #
    # worker 生命周期与来源状态机
    # ------------------------------------------------------------------ #

    async def start(self) -> None:
        """启动 worker（幂等；生命周期由 lifespan 管理）。"""

        if self._worker is not None:
            return
        self._worker = asyncio.create_task(self._worker_loop())

    async def aclose(self) -> None:
        """停止 worker：取消活动项（unknown）+ 清空仍排队项（aborted），
        不遗留 queued/processing。"""

        task = self._worker
        self._worker = None
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        while not self._queue.empty():
            item = self._queue.get_nowait()
            key = self._item_key(item)
            record = self._records.get(key)
            if record is None or record.get("state") in _IN_FLIGHT_STATES:
                self._remember(key, item, "aborted", "channel_stopped",
                               weak=not item.get("expect_sha256"))
                self._counters["aborted"] += 1
            self._queue.task_done()

    async def _worker_loop(self) -> None:
        current: Optional[Dict[str, Any]] = None
        try:
            while True:
                item = await self._queue.get()
                current = item
                try:
                    # 显式 processing：取出到终态之间的在途状态可见、可取消。
                    self._remember(self._item_key(item), item, "processing", "",
                                   weak=not item.get("expect_sha256"))
                    if not self._private_item_allowed(item):
                        self._remember(self._item_key(item), item, 'aborted', 'private_authorization_changed', weak=not item.get('expect_sha256'))
                        continue
                    await self._fetch_one(item)
                except asyncio.CancelledError:
                    # 活动项被取消：查询可能已在桥执行，无法确认 → 记 unknown
                    # （终态，不自动重试），不留 processing。
                    self._remember(self._item_key(item), item, "unknown",
                                   "worker_cancelled",
                                   weak=not item.get("expect_sha256"))
                    raise
                except Exception as exc:  # noqa: BLE001 - 单条失败不拖垮 worker
                    self._counters["failed"] += 1
                    self._remember(
                        self._item_key(item), item, "failed",
                        f"worker_exception:{type(exc).__name__}",
                        weak=not item.get("expect_sha256"),
                    )
                    _logger.warning(
                        "bridge 文件补传失败 file=%s error=%s",
                        item.get("file_id"),
                        type(exc).__name__,
                    )
                finally:
                    current = None
                    self._queue.task_done()
        except asyncio.CancelledError:
            if current is not None:
                # 在 queue.get() 上被取消：该项仍在队列里，由 aclose 统一记 aborted。
                pass
            raise

    def _private_item_allowed(self, item):
        if ':private:' not in item['conversation_id']:
            return True
        service = getattr(self, '_private_access', None)
        grant = service.grant_for_conversation(item['conversation_id']) if service else None
        return bool(grant and (grant['grant_id'], grant['status_revision']) == item.get('private_grant'))

    async def _fetch_one(self, item: Dict[str, Any]) -> None:
        # 优先有界分块（大文件端到端可传）；桥不支持时退回整块协议并如实记录。
        if str(self.settings.file_chunk_action or "").strip() and int(self.settings.chunk_bytes) > 0:
            handled = await self._fetch_chunked(item)
            if handled:
                return
        await self._fetch_whole(item)

    async def _fetch_chunked(self, item: Dict[str, Any]) -> bool:
        """分块补传；返回 False 表示桥不支持该 action（调用方回退整块）。"""

        import shutil
        import tempfile
        from pathlib import Path

        key = self._item_key(item)
        chunk_bytes = int(self.settings.chunk_bytes)
        timeout = self.settings.fetch_timeout_seconds
        action = self.settings.file_chunk_action
        query_base = f"filec-{item['event_id'][-36:]}"
        meta, meta_data = await self.server.query_file_chunk(
            action=action,
            query_id=f"{query_base}-meta",
            account_id=item["account_id"],
            target_ref=item["target_ref"],
            conversation_type="private" if ":private:" in item["conversation_id"] else "group",
            file_id=item["file_id"],
            offset=0,
            length=0,
            timeout=timeout,
        )
        if meta.status is not QueryStatus.OK:
            # 旧桥没有该 action / 明确拒绝：只回退一次整块协议，不循环重试。
            self._counters["chunk_fallback"] += 1
            return False
        total = int(meta_data.get("size") or 0)
        if total <= 0 or total > self.settings.max_file_bytes:
            self._counters["failed"] += 1
            self._remember(key, item, "rejected", "meta_size_out_of_bounds",
                           weak=not item["expect_sha256"])
            return True
        if total != int(item["expect_size"]):
            self._counters["integrity_error"] += 1
            self._remember(key, item, "failed", "size_mismatch",
                           weak=not item["expect_sha256"])
            return True
        provider_sha = str(meta_data.get("sha256") or "").strip().lower()
        if item["expect_sha256"] and provider_sha and provider_sha != item["expect_sha256"]:
            self._counters["integrity_error"] += 1
            self._remember(key, item, "failed", "hash_mismatch",
                           weak=False)
            return True
        name = str(meta_data.get("name") or item["name"])[:200]
        tmpdir = Path(tempfile.mkdtemp(prefix="pero-file-"))
        payload_path = tmpdir / "payload"
        digest = hashlib.sha256()
        written = 0
        try:
            with payload_path.open("wb") as out:
                while written < total:
                    if not self._private_item_allowed(item):
                        self._remember(key, item, 'aborted', 'private_authorization_changed', weak=not item.get('expect_sha256'))
                        return True
                    length = min(chunk_bytes, total - written)
                    result, data = await self.server.query_file_chunk(
                        action=action,
                        query_id=f"{query_base}-{written}",
                        account_id=item["account_id"],
                        target_ref=item["target_ref"],
            conversation_type="private" if ":private:" in item["conversation_id"] else "group",
                        file_id=item["file_id"],
                        offset=written,
                        length=length,
                        timeout=timeout,
                    )
                    if result.status is not QueryStatus.OK:
                        self._counters["failed"] += 1
                        self._remember(
                            key, item, "failed",
                            f"chunk_query:{result.evidence or result.status.value}",
                            weak=not item["expect_sha256"],
                        )
                        return True
                    try:
                        raw = self._verify_chunk(item, data, written, length, total)
                    except ResourceCatalogError as exc:
                        if exc.code == "scope_mismatch":
                            self._counters["scope_mismatch"] += 1
                        else:
                            self._counters["integrity_error"] += 1
                        self._remember(key, item, "failed", exc.code,
                                       weak=not item["expect_sha256"])
                        return True
                    out.write(raw)
                    digest.update(raw)
                    written += len(raw)
            actual_sha = digest.hexdigest()
            expected_sha = item["expect_sha256"] or provider_sha
            if written != total or (expected_sha and actual_sha != expected_sha):
                self._counters["integrity_error"] += 1
                self._remember(key, item, "failed", "integrity_error",
                               weak=not item["expect_sha256"])
                return True
            try:
                if not self._private_item_allowed(item):
                    self._remember(key, item, 'aborted', 'private_authorization_changed', weak=not item.get('expect_sha256'))
                    return True
                meta_item = await self.catalog.ingest_file(
                    agent_id=item["agent_id"],
                    conversation_id=item["conversation_id"],
                    name=name,
                    source_path=payload_path,
                    sha256=actual_sha,
                    size=written,
                    event_id=item["event_id"],
                )
            except ResourceCatalogError as exc:
                self._counters["failed"] += 1
                self._remember(key, item, "failed", f"catalog_{exc.code}",
                               weak=not item["expect_sha256"])
                return True
        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)
        if meta_item.get("duplicate"):
            self._counters["duplicate"] += 1
        else:
            self._counters["fetched"] += 1
        detail = "" if item["expect_sha256"] else "hash_unknown"
        self._remember(key, item, "registered", detail,
                       resource_id=str(meta_item.get("resource_id") or ""),
                       weak=not item["expect_sha256"])
        return True

    def _verify_chunk(
        self, item: Dict[str, Any], data: Dict[str, Any],
        expected_offset: int, expected_length: int, total: int,
    ) -> bytes:
        """单块硬校验：句柄/群/偏移/长度/eof/Base64，缺一项都不写入。"""

        if str(data.get("file_id") or "") != item["file_id"]:
            raise ResourceCatalogError("file_id_mismatch")
        response_group = str(data.get("group_wxid") or "")
        expect_group = item.get("expect_group_wxid") or ""
        if not expect_group or not response_group or response_group != expect_group:
            raise ResourceCatalogError("scope_mismatch")
        if int(data.get("offset") or 0) != expected_offset:
            raise ResourceCatalogError("chunk_offset_mismatch")
        content_b64 = str(data.get("content_base64") or "")
        if not content_b64:
            raise ResourceCatalogError("empty_bytes")
        try:
            raw = base64.b64decode(content_b64, validate=True)
        except Exception as exc:
            raise ResourceCatalogError("decode_invalid") from exc
        if len(raw) != expected_length:
            raise ResourceCatalogError("chunk_length_mismatch")
        if int(data.get("size") or 0) != total:
            raise ResourceCatalogError("integrity_error")
        if expected_offset + len(raw) > total:
            raise ResourceCatalogError("integrity_error")
        eof = bool(data.get("eof"))
        if eof != (expected_offset + len(raw) >= total):
            raise ResourceCatalogError("chunk_eof_mismatch")
        return raw

    async def _fetch_whole(self, item: Dict[str, Any]) -> None:
        key = self._item_key(item)
        # 整块协议的内存/帧风险：超过该阈值不再尝试（旧桥不支持分块时明确报错）。
        if int(item["expect_size"]) > _WHOLE_MAX_BYTES:
            self._counters["failed"] += 1
            self._remember(key, item, "failed", "chunk_unsupported_large_file",
                           weak=not item["expect_sha256"])
            return
        result, data = await self.server.query_file_bytes(
            action=self.settings.file_query_action,
            query_id=f"fileq-{item['event_id'][-40:]}",
            account_id=item["account_id"],
            target_ref=item["target_ref"],
            conversation_type="private" if ":private:" in item["conversation_id"] else "group",
            file_id=item["file_id"],
            timeout=self.settings.fetch_timeout_seconds,
        )
        if result.status is not QueryStatus.OK:
            # 只读查询失败：无连接/超时/断连（unknown）与桥明确失败（failed）
            # 分别受控记录；不重试、不回退本地读取。
            self._counters["failed"] += 1
            self._remember(key, item, "failed", str(result.evidence or result.status.value),
                           weak=not item["expect_sha256"])
            return

        def _decode_verify() -> bytes:
            return self._verify_bytes(item, data)

        try:
            payload = await asyncio.to_thread(_decode_verify)
        except ResourceCatalogError as exc:
            if exc.code == "scope_mismatch":
                self._counters["scope_mismatch"] += 1
            else:
                self._counters["integrity_error"] += 1
            self._remember(key, item, "failed", exc.code,
                           weak=not item["expect_sha256"])
            return
        except Exception as exc:  # noqa: BLE001
            self._counters["failed"] += 1
            self._remember(key, item, "failed", f"decode_error:{type(exc).__name__}",
                           weak=not item["expect_sha256"])
            return
        try:
            if not self._private_item_allowed(item):
                self._remember(key, item, 'aborted', 'private_authorization_changed', weak=not item.get('expect_sha256'))
                return
            meta = await self.catalog.ingest(
                agent_id=item["agent_id"],
                conversation_id=item["conversation_id"],
                name=str(data.get("name") or item["name"]),
                data=payload,
                event_id=item["event_id"],
            )
        except ResourceCatalogError as exc:
            # 存储失败 / 版本冲突 / 超限等入库失败：明确记录，不调用模型猜内容。
            self._counters["failed"] += 1
            self._remember(key, item, "failed", f"catalog_{exc.code}",
                           weak=not item["expect_sha256"])
            return
        if meta.get("duplicate"):
            self._counters["duplicate"] += 1
        else:
            self._counters["fetched"] += 1
        # 缺哈希时如实标注 unknown（其余约束已校验），不伪造校验成功。
        detail = "" if item["expect_sha256"] else "hash_unknown"
        self._remember(key, item, "registered", detail,
                       resource_id=str(meta.get("resource_id") or ""),
                       weak=not item["expect_sha256"])

    def _verify_bytes(self, item: Dict[str, Any], data: Dict[str, Any]) -> bytes:
        """worker 线程内的硬校验（P13-R01 已认可，未改动语义）。

        逐项比对：响应 file_id ↔ 请求句柄；响应 group_wxid ↔ offer 可信群；
        响应 size ↔ 解码字节 ↔ offer 预期大小；哈希以 offer 预期为硬约束，
        缺哈希时用响应 sha256 兜底校验，再缺则记 unknown 并保留其余约束。
        """

        if str(data.get("file_id") or "") != item["file_id"]:
            raise ResourceCatalogError("file_id_mismatch")
        # 作用域：响应群必须与 offer 可信群完全一致；任一方缺失都无法核验，
        # 一律拒绝，不把"换了个群保存"当成跨群校验成功。
        response_group = str(data.get("group_wxid") or "")
        expect_group = item.get("expect_group_wxid") or ""
        if not expect_group or not response_group or response_group != expect_group:
            raise ResourceCatalogError("scope_mismatch")
        content_b64 = str(data.get("content_base64") or "")
        if not content_b64:
            raise ResourceCatalogError("empty_bytes")
        # Base64 解码前限长（解码后约 3/4），不在解码前分配大内存。
        max_b64 = ((self.settings.max_file_bytes + 2) // 3) * 4
        if len(content_b64) > max_b64:
            raise ResourceCatalogError("size_out_of_bounds")
        try:
            raw = base64.b64decode(content_b64, validate=True)
        except Exception as exc:
            raise ResourceCatalogError("decode_invalid") from exc
        if not raw or len(raw) > self.settings.max_file_bytes:
            raise ResourceCatalogError("size_out_of_bounds")
        response_size = int(data.get("size") or 0)
        if response_size != len(raw) or len(raw) != item["expect_size"]:
            # 响应 size / 实际字节 / offer 预期三方一致；不能以响应字段覆盖预期。
            raise ResourceCatalogError("integrity_error")
        digest = hashlib.sha256(raw).hexdigest()
        if item["expect_sha256"]:
            if digest != item["expect_sha256"]:
                raise ResourceCatalogError("integrity_error")
        else:
            response_sha = str(data.get("sha256") or "").strip().lower()
            if response_sha and digest != response_sha:
                raise ResourceCatalogError("integrity_error")
            # 两者都缺：哈希 unknown，其余约束照常（调用方据 offer 记 hash_unknown）。
        return raw
