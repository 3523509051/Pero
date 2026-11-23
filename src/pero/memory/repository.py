"""持久记忆的异步门面（注册表组件 `memory.sqlite`）。

职责：
- 打开/迁移独立 SQLite，报告初始化状态（**不暴露服务器路径**）；
- 把同步存储调用放到线程里执行，避免阻塞事件循环；
- 给编排提供：写入原文/轮次、收尾投递与工具证据、按轮次预算重建历史；
- 给只读接口提供：层级目录、记录列表/详情、别名身份查询。

失败语义：存储不可用时抛 `MemoryUnavailableError`；写入失败抛 `MemoryWriteError`。
调用方必须如实计数/记录，不得报告"已保存"。
"""

from __future__ import annotations

import asyncio
import logging
import sqlite3
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from pero.core.settings import MemorySettings
from pero.memory.models import (
    ConversationRef,
    ExplicitSaveRequest,
    HistoryLoad,
    InboundRecord,
    LAYER_JOURNAL,
    LAYER_L2,
    LAYER_L20,
    LAYER_L4,
    LAYER_RAW,
    QUERYABLE_LAYERS,
    RetrievalHit,
    SaveOutcome,
    ToolEvidenceInput,
    TurnHandle,
)
from pero.memory.schema import SCHEMA_VERSION
from pero.memory.store import MemoryQueryError, MemoryStoreError, SqliteMemoryStore

logger = logging.getLogger(__name__)

#: 层级目录：真实实现的层按运行配置动态标记状态（available/disabled/…）；
#: P15 修正旧描述——L1 已实现（进程内存）、L2.5 是归档/整理过程态、L3 是
#: 持久人物事实层。内存态层重启即空，查询端必须如实表达。
#: (key, 名称, 持久化, 基线状态, 说明, kind)
LAYER_CATALOG: Tuple[Tuple[str, str, bool, str, str, str], ...] = (
    (
        LAYER_RAW,
        "入站原文",
        True,
        "available",
        "按 source/event ID 幂等保存的逐条原文，是 L2 的来源记录（不是长期记忆层）。",
        "source",
    ),
    (
        "L1",
        "即时背景（近期语境）",
        False,
        "available",
        "进程内存有界背景缓冲（P11 起）：允许群消息未唤醒也更新，重启即空；"
        "不写 SQLite，重启后为空不是故障。",
        "layer",
    ),
    (
        "L10",
        "深缓冲（承接 L1 溢出）",
        False,
        "disabled",
        "独立有界深缓冲（P15）：承接 L1 溢出的事件行，按 Agent+会话隔离、去重、"
        "TTL 与容量；按任务预算选用，不全部塞模型；重启即空。",
        "layer",
    ),
    (
        LAYER_L2,
        "轮次记录",
        True,
        "available",
        "每轮用户原文 + 发送状态；只有 sent 轮次进入已发言历史。",
        "layer",
    ),
    (
        LAYER_L20,
        "工具证据",
        True,
        "available",
        "工具名/Call ID/成功失败/受控摘要/资源引用，与 L2 按 turn_id 联合加载。",
        "layer",
    ),
    (
        "L3",
        "人物事实",
        True,
        "disabled",
        "持久人物事实层（P15）：只收来源可核对的明确陈述/偏好（带来源记录 ID、"
        "证据类型与状态）；identity/alias 不是人物事实，不凭语气/头像推断人格。",
        "layer",
    ),
    (
        LAYER_L4,
        "长期记录（明确保存与自动归档）",
        True,
        "available",
        "用户明确保存与 archive_summary 分类型保存；自动摘要带来源，不冒充用户保存请求。",
        "layer",
    ),
    (
        LAYER_JOURNAL,
        "事件日记",
        True,
        "available",
        "按已收尾轮次确定性生成的短事件条目（时间/参与者/状态/来源），不调模型整理。",
        "layer",
    ),
    (
        "L2.5",
        "归档/整理过程",
        True,
        "disabled",
        "实际归档/整理任务的过程状态（P07 archive_task）：它是处理过程，不是独立"
        "记忆库；入口 /api/v1/agents/{agent_id}/memory/archive。归档摘要不冒充"
        "新用户原话，不能直接晋升确定事实。",
        "process",
    ),
    (
        "vector",
        "语义/向量索引",
        True,
        "disabled",
        "可装配嵌入提供方的向量索引（P15）：带来源记录 ID/文本哈希/维度/模型版本；"
        "默认关闭、默认不联网建索引；不可用时明确降级关键词检索。",
        "index",
    ),
)


class MemoryUnavailableError(RuntimeError):
    """存储未初始化/不可用：调用方应明确拒绝，而不是假装读写成功。"""


class MemoryWriteError(RuntimeError):
    """写入事务失败：没有提交，不能对外宣称已保存。"""


def _describe_error(exc: BaseException) -> str:
    """把初始化异常收敛成不含路径的短原因。"""

    if isinstance(exc, sqlite3.OperationalError):
        text = str(exc).lower()
        if "unable to open" in text:
            return "无法打开数据库文件（检查数据目录权限）"
        if "readonly" in text or "read-only" in text:
            return "数据目录只读"
        if "locked" in text:
            return "数据库被其它进程占用（本程序只支持单进程）"
        return "SQLite 打开失败"
    if isinstance(exc, OSError):
        return "数据目录不可写"
    if isinstance(exc, MemoryStoreError):
        return str(exc)
    return "存储初始化失败"


def _truncate(text: Optional[str], limit: int) -> Tuple[Optional[str], bool]:
    if text is None:
        return None, False
    if len(text) <= limit:
        return text, False
    return text[:limit], True


class SqliteMemoryRepository:
    """独立 SQLite 记忆仓库（单进程）。"""

    repository_id = "memory.sqlite"

    def __init__(self, settings: MemorySettings) -> None:
        self._settings = settings
        self._store: Optional[SqliteMemoryStore] = None
        self._error: Optional[str] = None
        try:
            self._store = SqliteMemoryStore(settings.database_path())
        except Exception as exc:  # noqa: BLE001 - 初始化失败要如实报告
            self._error = _describe_error(exc)
            logger.error(
                "memory 存储初始化失败：%s（%s）", self._error, type(exc).__name__
            )

    # ------------------------------------------------------------------ #
    # 状态
    # ------------------------------------------------------------------ #

    @property
    def available(self) -> bool:
        return self._store is not None

    @property
    def settings(self) -> MemorySettings:
        return self._settings

    @property
    def error_detail(self) -> Optional[str]:
        return self._error

    @property
    def schema_version(self) -> Optional[int]:
        return self._store.schema_version if self._store is not None else None

    def status(self) -> Dict[str, Any]:
        """脱敏状态：不返回数据库路径、不返回任何正文。"""

        return {
            "component_id": self.repository_id,
            "backend": "sqlite",
            "available": self.available,
            "schema_version": self.schema_version,
            "expected_schema_version": SCHEMA_VERSION,
            "single_process": True,
            "path_exposed": False,
            "error": self._error,
        }

    async def aclose(self) -> None:
        store = self._store
        self._store = None
        if store is not None:
            await asyncio.to_thread(store.close)

    async def close(self) -> None:  # 与协议的异步风格保持一致
        await self.aclose()

    def _require(self) -> SqliteMemoryStore:
        if self._store is None:
            raise MemoryUnavailableError(self._error or "存储未初始化")
        return self._store

    async def _read(self, fn, *args, **kwargs):
        store = self._require()
        try:
            return await asyncio.to_thread(fn, store, *args, **kwargs)
        except MemoryStoreError:
            raise
        except sqlite3.Error as exc:
            logger.error("memory 读取失败：%s", type(exc).__name__)
            raise MemoryWriteError("读取失败") from exc

    async def _write(self, fn, *args, **kwargs):
        store = self._require()
        try:
            return await asyncio.to_thread(fn, store, *args, **kwargs)
        except MemoryStoreError:
            raise
        except sqlite3.Error as exc:
            logger.error("memory 写入失败：%s", type(exc).__name__)
            raise MemoryWriteError("写入事务未提交") from exc

    async def archive_conversations(self, **kwargs):
        return await self._read(lambda store: store.archive_conversations(**kwargs))

    async def prepare_archive(self, **kwargs):
        return await self._write(lambda store: store.prepare_archive(**kwargs))

    async def claim_archive(self, **kwargs):
        return await self._write(lambda store: store.claim_archive(**kwargs))

    async def settle_archive(self, **kwargs):
        return await self._write(lambda store: store.settle_archive(**kwargs))

    async def recover_archives(self, **kwargs):
        return await self._write(lambda store: store.recover_archives(**kwargs))

    async def archive_status(self, **kwargs):
        return await self._read(lambda store: store.archive_status(**kwargs))

    async def archive_history(self, **kwargs):
        return await self._read(lambda store: store.archive_history(**kwargs))

    # ------------------------------------------------------------------ #
    # v6 聊天流水（规范视图）与归档批次
    # ------------------------------------------------------------------ #

    async def chat_append_member(self, **kwargs):
        return await self._write(lambda store: store.chat_append_member(**kwargs))

    async def chat_append_assistant(self, **kwargs):
        return await self._write(lambda store: store.chat_append_assistant(**kwargs))

    async def chat_window(self, **kwargs):
        return await self._read(lambda store: store.chat_window(**kwargs))

    async def chat_snapshot(self, **kwargs):
        return await self._read(lambda store: store.chat_snapshot(**kwargs))

    async def chat_link_assistant_echo(self, **kwargs):
        return await self._write(lambda store: store.chat_link_assistant_echo(**kwargs))

    async def chat_task_begin(self, **kwargs):
        return await self._write(lambda store: store.chat_task_begin(**kwargs))

    async def chat_task_fail(self, **kwargs):
        return await self._write(lambda store: store.chat_task_fail(**kwargs))

    async def chat_task_success(self, **kwargs):
        return await self._write(lambda store: store.chat_task_success(**kwargs))

    async def chat_l3_eligible(self, **kwargs):
        return await self._read(lambda store: store.chat_l3_eligible(**kwargs))

    async def chat_task_pending(self, **kwargs):
        return await self._read(lambda store: store.chat_task_pending(**kwargs))

    async def chat_recovery_scopes(self, **kwargs):
        return await self._read(lambda store: store.chat_recovery_scopes(**kwargs))

    async def chat_task_manual_retry(self, **kwargs):
        return await self._write(lambda store: store.chat_task_manual_retry(**kwargs))

    async def chat_task_list(self, **kwargs):
        return await self._read(lambda store: store.chat_task_list(**kwargs))

    async def chat_l3_list(self, **kwargs):
        return await self._read(lambda store: store.chat_l3_list(**kwargs))

    async def l3_set_status(self, **kwargs):
        return await self._write(lambda store: store.l3_set_status(**kwargs))

    async def chat_history(self, **kwargs):
        return await self._read(lambda store: store.chat_history(**kwargs))

    async def chat_counts(self, **kwargs):
        return await self._read(lambda store: store.chat_counts(**kwargs))

    async def chat_cursor_get(self, **kwargs):
        return await self._read(lambda store: store.chat_cursor_get(**kwargs))

    async def chat_cursor_set(self, **kwargs):
        return await self._write(lambda store: store.chat_cursor_set(**kwargs))

    async def chat_after(self, **kwargs):
        return await self._read(lambda store: store.chat_after(**kwargs))

    async def chat_link_record(self, **kwargs):
        return await self._write(lambda store: store.chat_link_record(**kwargs))

    async def chat_search(self, **kwargs):
        return await self._read(lambda store: store.chat_search(**kwargs))

    async def chat_identity_for_record(self, **kwargs):
        return await self._read(lambda store: store.chat_identity_for_record(**kwargs))

    async def chat_archive_commit(self, **kwargs):
        return await self._write(lambda store: store.chat_archive_commit(**kwargs))

    async def chat_archive_plan(self, **kwargs):
        return await self._write(lambda store: store.chat_archive_plan(**kwargs))

    async def chat_archive_claim(self, **kwargs):
        return await self._write(lambda store: store.chat_archive_claim(**kwargs))

    async def chat_archive_settle(self, **kwargs):
        return await self._write(lambda store: store.chat_archive_settle(**kwargs))

    async def chat_archive_advance(self, **kwargs):
        return await self._write(lambda store: store.chat_archive_advance(**kwargs))

    async def chat_archive_list(self, **kwargs):
        return await self._read(lambda store: store.chat_archive_list(**kwargs))

    async def chat_archive_status(self, **kwargs):
        return await self._read(lambda store: store.chat_archive_status(**kwargs))

    # ------------------------------------------------------------------ #
    # L2 独立交互（M01/M03）：未归档交互窗口 + 归档批次 + 会话级配置
    # ------------------------------------------------------------------ #
    async def l2_link_member(self, **kwargs):
        return await self._write(lambda store: store.l2_link_member(**kwargs))

    async def l3_from_archive(self, **kwargs):
        return await self._write(lambda store: store.l3_from_archive(**kwargs))

    async def l2_link_assistant(self, **kwargs):
        return await self._write(lambda store: store.l2_link_assistant(**kwargs))

    async def l2_window(self, **kwargs):
        return await self._read(lambda store: store.l2_window(**kwargs))

    async def l2_snapshot(self, **kwargs):
        return await self._read(lambda store: store.l2_snapshot(**kwargs))

    async def l2_list(self, **kwargs):
        return await self._read(lambda store: store.l2_list(**kwargs))

    async def l2_counts(self, **kwargs):
        return await self._read(lambda store: store.l2_counts(**kwargs))

    async def l2_recovery_scopes(self, **kwargs):
        return await self._read(lambda store: store.l2_recovery_scopes(**kwargs))

    async def l2_latest_summary(self, **kwargs):
        return await self._read(lambda store: store.l2_latest_summary(**kwargs))

    async def l2_archive_plan(self, **kwargs):
        return await self._write(lambda store: store.l2_archive_plan(**kwargs))

    async def l2_archive_claim(self, **kwargs):
        return await self._write(lambda store: store.l2_archive_claim(**kwargs))

    async def l2_archive_settle(self, **kwargs):
        return await self._write(lambda store: store.l2_archive_settle(**kwargs))

    async def l2_archive_commit(self, **kwargs):
        return await self._write(lambda store: store.l2_archive_commit(**kwargs))

    async def l2_archive_status(self, **kwargs):
        return await self._read(lambda store: store.l2_archive_status(**kwargs))

    async def l2_config_get(self, **kwargs):
        return await self._read(lambda store: store.l2_config_get(**kwargs))

    async def l2_config_set(self, **kwargs):
        return await self._write(lambda store: store.l2_config_set(**kwargs))

    async def memory_event_upsert(self, **kwargs):
        return await self._write(lambda store: store.memory_event_upsert(**kwargs))

    async def l3_candidate_upsert(self, **kwargs):
        return await self._write(lambda store: store.l3_candidate_upsert(**kwargs))

    async def l4_summary_upsert(self, **kwargs):
        return await self._write(lambda store: store.l4_summary_upsert(**kwargs))

    async def memory_event_list(self, **kwargs):
        return await self._read(lambda store: store.memory_event_list(**kwargs))

    # ------------------------------------------------------------------ #
    # 编排侧：写入
    # ------------------------------------------------------------------ #

    async def begin_turn(
        self,
        *,
        ref: ConversationRef,
        messages: Sequence[InboundRecord],
    ) -> TurnHandle:
        """写入原文与轮次；返回的 `user_text` 已去掉被其它轮次占用的重复来源。"""

        return await self._write(
            lambda store: store.begin_turn(ref=ref, messages=messages)
        )

    async def mark_interrupted(
        self, *, turn_id: str, agent_id: str, reason: str
    ) -> bool:
        """把遗留 pending 标成 interrupted（证据保留，不自动重试）。"""

        settings = self._settings
        return await self._write(
            lambda store: store.mark_interrupted(
                turn_id=turn_id,
                agent_id=agent_id,
                reason=reason,
                journal_summary_chars=settings.journal_summary_chars,
            )
        )

    async def append_plugin_job_evidence(self,**kwargs):
        return await self._write(lambda store:store.append_plugin_job_evidence(**kwargs))

    async def finish_turn(
        self,
        *,
        turn_id: str,
        agent_id: str,
        run_id: Optional[str],
        status: str,
        assistant_text: Optional[str],
        delivery_status: str,
        action_id: Optional[str],
        delivery_evidence: str,
        delivery_text: Optional[str],
        tools: Sequence[ToolEvidenceInput],
        images: Sequence[Mapping[str,Any]] = (),
    ) -> bool:
        settings = self._settings
        return await self._write(
            lambda store: store.finish_turn(
                turn_id=turn_id,
                agent_id=agent_id,
                run_id=run_id,
                status=status,
                assistant_text=assistant_text,
                delivery_status=delivery_status,
                action_id=action_id,
                delivery_evidence=delivery_evidence,
                delivery_text=delivery_text,
                tools=tools,
                images=images,
                journal_summary_chars=settings.journal_summary_chars,
            )
        )

    async def record_wxid_evidence(
        self,
        *,
        identity_id: str,
        wxid: str,
        source: str,
        evidence_ref: Optional[str] = None,
    ) -> bool:
        return await self._write(
            lambda store: store.record_wxid_evidence(
                identity_id=identity_id,
                wxid=wxid,
                source=source,
                evidence_ref=evidence_ref,
            )
        )

    async def save_explicit_memory(
        self, *, ref: ConversationRef, request: ExplicitSaveRequest
    ) -> SaveOutcome:
        """写入一条 L4 明确保存记录；返回真实记录 ID 或明确的未保存状态。"""

        settings = self._settings
        return await self._write(
            lambda store: store.save_explicit_memory(
                ref=ref, request=request, max_chars=settings.save_max_chars
            )
        )

    # ------------------------------------------------------------------ #
    # 编排侧：读取
    # ------------------------------------------------------------------ #

    async def load_history(
        self, *, agent_id: str, conversation_id: str
    ) -> HistoryLoad:
        """按配置预算重建历史；返回受控原因，调用方据此计数而不是静默塞满。"""

        settings = self._settings
        return await self._read(
            lambda store: store.load_history(
                agent_id=agent_id,
                conversation_id=conversation_id,
                turn_limit=settings.load_turn_limit,
                char_budget=settings.load_char_budget,
                evidence_per_turn=settings.load_tool_evidence_per_turn,
            )
        )

    async def search(
        self,
        *,
        agent_id: str,
        conversation_id: Optional[str],
        identity_ids: Sequence[str] = (),
        query: str,
        layers: Optional[Sequence[str]] = None,
        limit: Optional[int] = None,
    ) -> Tuple[List[RetrievalHit], str]:
        """在 L4 / 日记 / 原文里做有界关键词检索，返回命中与说明。"""

        settings = self._settings
        size = min(
            max(int(limit or settings.retrieval_limit), 1),
            settings.record_max_limit,
        )
        wanted = tuple(layers) if layers else (LAYER_L4, LAYER_JOURNAL, LAYER_RAW)
        return await self._read(
            lambda store: store.search(
                agent_id=agent_id,
                conversation_id=conversation_id,
                identity_ids=tuple(identity_ids),
                query=query,
                layers=wanted,
                limit=size,
                item_chars=settings.retrieval_item_chars,
            )
        )

    async def match_alias_tokens(
        self,
        *,
        agent_id: str,
        account_id: Optional[str],
        conversation_id: Optional[str],
        tokens: Sequence[str],
    ) -> List[Dict[str, Any]]:
        """把查询 token 当别名做精确匹配（用于检索前稳定身份）。"""

        return await self._read(
            lambda store: store.match_alias_tokens(
                agent_id=agent_id,
                account_id=account_id,
                conversation_id=conversation_id,
                tokens=tuple(tokens),
            )
        )

    # ------------------------------------------------------------------ #
    # 只读接口
    # ------------------------------------------------------------------ #

    async def counts(self) -> Dict[str, int]:
        return await self._read(lambda store: store.counts())

    async def layer_counts(self, agent_id: str) -> Dict[str, int]:
        """per-Agent 各层真实计数（不是全库数）。"""
        return await self._read(lambda store: store.layer_counts(agent_id))

    async def query_records(
        self,
        *,
        agent_id: str,
        layer: str,
        conversation_id: Optional[str] = None,
        identity_id: Optional[str] = None,
        since: Optional[str] = None,
        until: Optional[str] = None,
        limit: Optional[int] = None,
        offset: int = 0,
        keyword: Optional[str] = None,
    ) -> Dict[str, Any]:
        settings = self._settings
        size = min(
            max(int(limit or settings.record_default_limit), 1),
            settings.record_max_limit,
        )
        items, total = await self._read(
            lambda store: store.query_records(
                agent_id=agent_id,
                layer=layer,
                conversation_id=conversation_id,
                identity_id=identity_id,
                since=since,
                until=until,
                limit=size,
                offset=max(int(offset), 0),
                preview_chars=settings.preview_chars,
                keyword=keyword,
            )
        )
        return {
            "layer": layer,
            "items": items,
            "total": total,
            "limit": size,
            "offset": max(int(offset), 0),
            "preview_chars": settings.preview_chars,
            "next_offset": (
                max(int(offset), 0) + len(items) if len(items) == size else None
            ),
        }

    async def get_record(
        self, *, agent_id: str, record_id: str
    ) -> Optional[Dict[str, Any]]:
        record = await self._read(
            lambda store: store.get_record(agent_id=agent_id, record_id=record_id)
        )
        if record is None:
            return None
        limit = self._settings.detail_max_chars
        text, truncated = _truncate(record.get("text"), limit)
        record["text"] = text
        record["text_truncated"] = truncated
        record["detail_max_chars"] = limit
        if "assistant_text" in record:
            assistant, assistant_truncated = _truncate(
                record.get("assistant_text"), limit
            )
            record["assistant_text"] = assistant
            record["assistant_truncated"] = assistant_truncated
        return record

    async def find_identities(
        self,
        *,
        agent_id: str,
        alias: str,
        account_id: Optional[str] = None,
        conversation_id: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> Dict[str, Any]:
        settings = self._settings
        size = min(
            max(int(limit or settings.record_default_limit), 1),
            settings.record_max_limit,
        )
        items, total = await self._read(
            lambda store: store.find_identities(
                agent_id=agent_id,
                alias=alias,
                account_id=account_id,
                conversation_id=conversation_id,
                limit=size,
            )
        )
        return {
            "alias": alias,
            "account_id": account_id,
            "conversation_id": conversation_id,
            "candidates": items,
            # 歧义按**总命中数**判定：limit=1 时实际有多个同名身份仍算歧义。
            "ambiguous": total > 1,
            "total": total,
            "has_more": total > len(items),
            "limit": size,
            "wxid_note": (
                "桥当前不提供 wxid；身份按 channel+account+桥用户 ID 稳定标识，"
                "昵称只是带作用域与时间的别名，不参与身份主键。"
            ),
        }

    async def layers(
        self, runtime_states: Optional[Mapping[str, Any]] = None
    ) -> Dict[str, Any]:
        """层级目录。`runtime_states`（key → {state, detail, count}）由装配层
        按实际开关/内存状态给出，覆盖基线状态；没有真实实现的层不得仅为页面
        标 available。"""
        counts = await self.counts()
        runtime_states = dict(runtime_states or {})
        layers = []
        for key, name, persistent, status, detail, kind in LAYER_CATALOG:
            override = runtime_states.get(key) or {}
            state = str(override.get("state") or status)
            count = override.get("count")
            if count is None:
                count = counts.get(key, 0) if state == "available" else 0
            layers.append({
                "key": key,
                "name": name,
                "kind": kind,
                "persistent": persistent,
                "status": state,
                "detail": str(override.get("detail") or detail),
                "record_count": int(count),
            })
        vector_state = runtime_states.get("vector") or {}
        return {
            "storage": self.status(),
            "layers": layers,
            "counts": counts,
            "queryable_layers": list(QUERYABLE_LAYERS),
            "vector": {
                "status": vector_state.get("state", "disabled"),
                "provider_model": vector_state.get("provider_model", ""),
                "chunks": vector_state.get("count", 0),
                "note": "连通性未实机验证；不可用时明确降级关键词检索",
            },
        }

    # ------------------------------------------------------------------ #
    # MemoryRepository 协议（内部状态用；前端请用带 agent_id 的接口）
    # ------------------------------------------------------------------ #

    async def layer_overview(self) -> Mapping[str, Any]:
        return await self.layers()

    # ------------------------------------------------------------------ #
    # P15：持久会话目录 / 回放 / L3 / 向量（与渠道侧同一存储，线程内执行）
    # ------------------------------------------------------------------ #

    async def session_upsert(self, **kwargs):
        return await self._write(lambda store: store.session_upsert(**kwargs))

    async def session_touch(self, **kwargs):
        return await self._write(lambda store: store.session_touch(**kwargs))

    async def session_get(self, *, agent_id: str, session_id: str):
        return await self._read(
            lambda store: store.session_get(agent_id, session_id)
        )

    async def session_find(self, *, session_id: str):
        return await self._read(lambda store: store.session_find(session_id))

    async def session_list(self, *, agent_id: str, limit: int, cursor: str = ""):
        return await self._read(
            lambda store: store.session_list(agent_id, limit=limit, cursor=cursor)
        )

    async def session_replay(self, *, agent_id: str, conversation_id: str,
                             limit: int, cursor: str = ""):
        return await self._read(
            lambda store: store.session_replay(
                agent_id, conversation_id, limit=limit, cursor=cursor)
        )

    async def session_history_pairs(self, *, agent_id: str, conversation_id: str,
                                    limit: int = 200):
        return await self._read(
            lambda store: store.session_history_pairs(
                agent_id, conversation_id, limit=limit)
        )

    async def identities_list(self, *, agent_id: str, limit: int, cursor: str = ""):
        return await self._read(
            lambda store: store.identities_list(agent_id, limit=limit, cursor=cursor)
        )

    async def l3_upsert(self, **kwargs):
        return await self._write(lambda store: store.l3_upsert(**kwargs))

    async def l3_list(self, *, agent_id: str, identity_id: str = "",
                      conversation_id: str = "", status: str = "",
                      limit: int, cursor: str = ""):
        return await self._read(
            lambda store: store.l3_list(
                agent_id, conversation_id=conversation_id,
                identity_id=identity_id, status=status, limit=limit, cursor=cursor)
        )

    async def vector_upsert(self, **kwargs):
        return await self._write(lambda store: store.vector_upsert(**kwargs))

    async def vector_fetch_scope(self, **kwargs):
        return await self._read(lambda store: store.vector_fetch_scope(**kwargs))

    async def vector_stats(self):
        return await self._read(lambda store: store.vector_stats())

    async def query(
        self,
        *,
        layer: str,
        limit: int = 20,
        cursor: Optional[str] = None,
    ) -> Mapping[str, Any]:
        """协议方法：只返回计数，不返回正文，也不跨 Agent 暴露记录内容。"""

        if layer not in QUERYABLE_LAYERS:
            raise MemoryStoreError(f"不支持的记录层：{layer}")
        counts = await self.counts()
        return {
            "layer": layer,
            "count": counts.get(layer, 0),
            "items": [],
            "next_cursor": None,
            "note": "按 Agent 的记录查询请使用 /api/v1/agents/{agent_id}/memory/records",
        }


__all__ = [
    "LAYER_CATALOG",
    "MemoryUnavailableError",
    "MemoryWriteError",
    "SqliteMemoryRepository",
]
