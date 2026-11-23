"""SQLite 存储核心（同步）：原文、身份别名、L2 轮次、L20 工具证据。

线程模型：
- **单进程、单连接**：`check_same_thread=False` + `RLock` 串行化；每个事务都很短。
- 异步调用方必须经 `asyncio.to_thread` 使用（见 `repository.py`），
  不允许在事件循环里直接做磁盘 IO。

写入语义：
- 原文按 `(channel, account, event_id)` 幂等；**来源归属**决定是否建新轮次：
  已被其它轮次占用的原文不会再被第二批次消费（批次怎么分组都不重发）。
- 只有事务提交成功才认为"已保存"；失败向上抛出，由调用方如实记录，不静默降级。
- 工具证据按 `(turn_id, call_id)` 唯一，重复写入被忽略。
- `pending` 只是"写入后、收尾前"的中间态，**不代表可以安全重试**；中断必须显式收尾。
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Sequence, Tuple, Mapping

from pero.memory.keywords import like_clause, normalize, score, tokenize
from pero.memory.models import (
    ConversationRef,
    ExplicitSaveRequest,
    HISTORY_CHAR_BUDGET,
    HISTORY_NEWEST_OVER_BUDGET,
    HISTORY_NO_TURNS,
    HISTORY_OK,
    HistoryLoad,
    InboundRecord,
    LAYER_JOURNAL,
    LAYER_L2,
    LAYER_L20,
    LAYER_L3,
    LAYER_L4,
    LAYER_RAW,
    RetrievalHit,
    SAVE_DUPLICATE,
    SAVE_NEEDS_CONTENT,
    SAVE_SAVED,
    SAVE_TOO_LONG,
    SaveOutcome,
    StoredEvidence,
    StoredTurn,
    TURN_INTERRUPTED,
    TURN_PENDING,
    TURN_SENT,
    TURN_SETTLED_STATUSES,
    ToolEvidenceInput,
    TurnHandle,
    WXID_TRUSTED_SOURCES,
    new_evidence_id,
    new_journal_id,
    new_memory_id,
    new_record_id,
    new_turn_id,
    render_source_line,
    stable_identity_id,
)
from pero.memory.schema import SCHEMA_VERSION, migrate
from pero.memory.archive_store import ArchiveStoreMixin
from pero.memory.chat_stream import ChatStreamMixin
from pero.memory.p15_store import FACT_ACTIVE, P15StoreMixin, MemoryQueryErrorP15


class MemoryStoreError(RuntimeError):
    """存储层可预期的错误（未知轮次、只读/配置问题等）。"""


class MemoryQueryError(MemoryStoreError):
    """只读查询的输入问题（非法会话标识、作用域冲突）。调用方应回 422。"""


def parse_conversation_id(conversation_id: str) -> Tuple[str, str, str, str]:
    """严格解析 `channel:account:type:target`，不合法就报错，不做"全库兜底"。"""

    parts = (conversation_id or "").split(":")
    if len(parts) != 4 or not all(part.strip() for part in parts):
        raise MemoryQueryError(
            "conversation_id 必须是 channel:account:group|private:target"
        )
    channel_id, account_id, conversation_type, target_ref = (p.strip() for p in parts)
    if conversation_type not in {"group", "private"}:
        raise MemoryQueryError(
            "conversation_id 的类型段必须是 group 或 private"
        )
    return channel_id, account_id, conversation_type, target_ref


def _now() -> str:
    """业务时间统一北京时间（aware，明确 +08:00）。"""

    from pero.core.timebase import now_iso

    return now_iso()


def _require_epoch(value: Any, label: str) -> float:
    """时间窗边界 → 绝对秒；解析失败直接报错（不静默改范围）。"""

    from pero.core.timebase import require_epoch

    return require_epoch(value, label)


def _participant_clause(turn_ref: str) -> str:
    """轮次"参与者"过滤：整轮所有来源的发送者，而不是只看首位 `sender_identity_id`。

    合并批次里非首位成员的身份过滤/查询都必须走这条路径，否则会漏掉他们；
    `turn_ref` 由调用方给出（如 `l2_turn.turn_id` / `t.turn_id`），只接受内部常量，
    不接受外部输入拼进 SQL。
    """

    return (
        "EXISTS (SELECT 1 FROM l2_turn_source s"
        " JOIN raw_message m ON m.record_id = s.record_id"
        f" WHERE s.turn_id = {turn_ref} AND m.sender_identity_id = ?)"
    )


class SqliteMemoryStore(ChatStreamMixin, ArchiveStoreMixin, P15StoreMixin):
    """一个数据库文件 = 一个连接；只允许本进程访问。"""

    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self._path = path
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(str(path), check_same_thread=False, timeout=5.0)
        self._conn.row_factory = sqlite3.Row
        with self._lock:
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA synchronous=FULL")
            self._conn.execute("PRAGMA foreign_keys=ON")
            self._conn.execute("PRAGMA busy_timeout=5000")
            # 时间筛选/排序统一走绝对时刻，避免旧 UTC 与 +08:00 混排导致漏召回。
            from pero.core.timebase import register_sql

            register_sql(self._conn)
            try:
                self._schema_version = migrate(self._conn)
                self._conn.commit()
            except BaseException:
                self._conn.rollback()
                self._conn.close()
                raise

    # ------------------------------------------------------------------ #
    # 基础设施
    # ------------------------------------------------------------------ #

    @property
    def schema_version(self) -> int:
        return self._schema_version

    @property
    def expected_schema_version(self) -> int:
        return SCHEMA_VERSION

    def close(self) -> None:
        with self._lock:
            try:
                self._conn.close()
            except sqlite3.Error:  # 已关闭或磁盘不可用：关闭阶段只忽略
                pass

    @contextmanager
    def _txn(self) -> Iterator[sqlite3.Cursor]:
        with self._lock:
            cursor = self._conn.cursor()
            try:
                yield cursor
                self._conn.commit()
            except BaseException:
                self._conn.rollback()
                raise
            finally:
                cursor.close()

    def counts(self) -> Dict[str, int]:
        with self._txn() as cur:
            return {
                "conversations": int(
                    cur.execute("SELECT COUNT(*) FROM conversation").fetchone()[0]
                ),
                "identities": int(
                    cur.execute("SELECT COUNT(*) FROM identity").fetchone()[0]
                ),
                "aliases": int(
                    cur.execute("SELECT COUNT(*) FROM identity_alias").fetchone()[0]
                ),
                LAYER_RAW: int(
                    cur.execute("SELECT COUNT(*) FROM raw_message").fetchone()[0]
                ),
                LAYER_L2: int(
                    cur.execute("SELECT COUNT(*) FROM l2_turn").fetchone()[0]
                ),
                LAYER_L20: int(
                    cur.execute("SELECT COUNT(*) FROM l20_tool_evidence").fetchone()[0]
                ),
                LAYER_L4: int(
                    cur.execute("SELECT COUNT(*) FROM l4_memory").fetchone()[0]
                ),
                LAYER_JOURNAL: int(
                    cur.execute("SELECT COUNT(*) FROM event_journal").fetchone()[0]
                ),
                "attachments": int(
                    cur.execute("SELECT COUNT(*) FROM l2_attachment").fetchone()[0]
                ),
                "deliveries": int(
                    cur.execute("SELECT COUNT(*) FROM delivery_record").fetchone()[0]
                ),
            }

    # ------------------------------------------------------------------ #
    # 写入：原文 + 身份 + 轮次
    # ------------------------------------------------------------------ #

    def begin_turn(
        self,
        *,
        ref: ConversationRef,
        messages: Sequence[InboundRecord],
    ) -> TurnHandle:
        """把一批入站原文与轮次一起写入（单事务）。

        同源防重按**原文归属**而不是批次 hash：
        1. 先把每条来源幂等写入 `raw_message`；
        2. 查这些来源已归属哪个轮次（可能来自不同批次的组合）；
        3. 只用一个轮次消费**尚未归属**的来源；已被占用的来源记入 `skipped_record_ids`，
           不再进入本轮 user_text，也不会被再次发送；
        4. 全部来源都已被占用 → `created=False` + 既有状态（pending → incomplete）。
        """

        if not messages:
            raise MemoryStoreError("轮次必须至少包含一条原文")
        now = _now()
        with self._txn() as cur:
            cur.execute(
                "INSERT INTO conversation ("
                " conversation_id, agent_id, channel_id, account_id,"
                " conversation_type, target_ref, created_at, updated_at"
                ") VALUES (?,?,?,?,?,?,?,?) "
                "ON CONFLICT(conversation_id) DO UPDATE SET updated_at=excluded.updated_at",
                (
                    ref.conversation_id,
                    ref.agent_id,
                    ref.channel_id,
                    ref.account_id,
                    ref.conversation_type,
                    ref.target_ref,
                    now,
                    now,
                ),
            )

            record_ids: List[str] = []
            attachments_by_record = {}
            for message in messages:
                identity_id = self._ensure_identity(cur, ref, message, now)
                record_id = self._insert_raw(cur, ref, message, identity_id, now)
                record_ids.append(record_id)
                attachments_by_record[record_id]=tuple(message.attachments)[:10]
            unique_ids = list(dict.fromkeys(record_ids))

            owners = self._source_owners(cur, ref.agent_id, unique_ids)
            claimed = [rid for rid in unique_ids if rid not in owners]
            skipped = tuple(rid for rid in unique_ids if rid in owners)

            if not claimed:
                pending_owner = next(
                    (owner for owner in owners.values() if owner[1] == TURN_PENDING),
                    None,
                )
                owner = pending_owner or next(iter(owners.values()))
                return TurnHandle(
                    turn_id=owner[0],
                    created=False,
                    status=owner[1],
                    record_ids=(),
                    dedupe_key="",
                    skipped_record_ids=skipped,
                    user_text="",
                    owner_turn_id=owner[0],
                    reason="pending" if pending_owner else "settled",
                )

            dedupe_key = hashlib.sha1(
                "\x1f".join(claimed).encode("utf-8")
            ).hexdigest()
            existing = cur.execute(
                "SELECT turn_id, status FROM l2_turn WHERE agent_id=? AND dedupe_key=?",
                (ref.agent_id, dedupe_key),
            ).fetchone()
            if existing is not None:
                return TurnHandle(
                    turn_id=str(existing["turn_id"]),
                    created=False,
                    status=str(existing["status"]),
                    record_ids=tuple(claimed),
                    dedupe_key=dedupe_key,
                    skipped_record_ids=skipped,
                    user_text="",
                    owner_turn_id=str(existing["turn_id"]),
                    reason="same_batch",
                    records=record_pairs,
                )

            rows = cur.execute(
                "SELECT record_id, source_event_id, sender_ref, sender_identity_id,"
                " display_name, text FROM raw_message WHERE record_id IN ("
                + ",".join("?" for _ in claimed)
                + ")",
                tuple(claimed),
            ).fetchall()
            by_id = {str(row["record_id"]): row for row in rows}
            ordered = [by_id[rid] for rid in claimed if rid in by_id]
            # 原文 ID ↔ 事件 ID 的权威映射（顺序与批次一致），供保存逐条定位。
            record_pairs = tuple(
                (str(row["record_id"]), str(row["source_event_id"])) for row in ordered
            )
            user_lines=[]
            for row in ordered:
                line=render_source_line(row["display_name"],str(row["sender_ref"]),str(row["text"]))
                for attachment in attachments_by_record.get(str(row["record_id"]),()):
                    if attachment.get("kind")=="image":
                        line+=f" [图片 #{str(attachment.get('handle') or '')[:100]} / {str(attachment.get('name') or '')[:180]}；目录记录不代表已看图]"
                user_lines.append(line)
            user_text="\n".join(user_lines)
            first_identity = (
                str(ordered[0]["sender_identity_id"]) if ordered else None
            )
            first_sender = str(ordered[0]["sender_ref"]) if ordered else ""

            turn_id = new_turn_id()
            cur.execute(
                "INSERT INTO l2_turn ("
                " turn_id, agent_id, conversation_id, run_id, status, user_text,"
                " user_chars, assistant_text, sender_identity_id, sender_ref,"
                " dedupe_key, created_at, updated_at, interrupt_reason"
                ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,NULL)",
                (
                    turn_id,
                    ref.agent_id,
                    ref.conversation_id,
                    None,
                    TURN_PENDING,
                    user_text,
                    len(user_text),
                    None,
                    first_identity,
                    first_sender,
                    dedupe_key,
                    now,
                    now,
                ),
            )
            for position, record_id in enumerate(claimed):
                cur.execute(
                    "INSERT OR IGNORE INTO l2_turn_source (turn_id, record_id, position)"
                    " VALUES (?,?,?)",
                    (turn_id, record_id, position),
                )
                for attachment in attachments_by_record.get(record_id,()):
                    if attachment.get("kind")!="image":
                        continue
                    handle=str(attachment.get("handle") or "")[:100]
                    aid="a-"+hashlib.sha256((turn_id+handle).encode()).hexdigest()[:32]
                    cur.execute("INSERT OR IGNORE INTO l2_attachment VALUES(?,?,?,?,?,?,0,NULL,?)",
                        (aid,turn_id,record_id,"image",str(attachment.get("name") or "")[:180],handle,now))
            return TurnHandle(
                turn_id=turn_id,
                created=True,
                status=TURN_PENDING,
                record_ids=tuple(claimed),
                dedupe_key=dedupe_key,
                skipped_record_ids=skipped,
                user_text=user_text,
                owner_turn_id=turn_id,
                reason="new",
                records=record_pairs,
            )

    def _source_owners(
        self, cur: sqlite3.Cursor, agent_id: str, record_ids: Sequence[str]
    ) -> Dict[str, Tuple[str, str]]:
        """查这些来源已归属的轮次（同 Agent）。

        历史数据里同一来源可能被多个轮次链接过：优先返回**已收尾**的轮次，
        其次才是 pending，避免把已发送的来源再次当作未处理。
        """

        if not record_ids:
            return {}
        rows = cur.execute(
            "SELECT s.record_id, t.turn_id, t.status FROM l2_turn_source s"
            " JOIN l2_turn t ON t.turn_id = s.turn_id"
            " WHERE t.agent_id=? AND s.record_id IN ("
            + ",".join("?" for _ in record_ids)
            + ") ORDER BY t.created_at, t.turn_id",
            (agent_id, *record_ids),
        ).fetchall()
        owners: Dict[str, Tuple[str, str]] = {}
        for row in rows:
            record_id = str(row["record_id"])
            current = owners.get(record_id)
            status = str(row["status"])
            if current is None or (current[1] == TURN_PENDING and status != TURN_PENDING):
                owners[record_id] = (str(row["turn_id"]), status)
        return owners

    def _ensure_identity(
        self,
        cur: sqlite3.Cursor,
        ref: ConversationRef,
        message: InboundRecord,
        now: str,
    ) -> str:
        identity_id = stable_identity_id(
            ref.channel_id, ref.account_id, message.sender_ref
        )
        cur.execute(
            "INSERT INTO identity ("
            " identity_id, channel_id, account_id, bridge_user_id, wxid,"
            " wxid_source, created_at, updated_at"
            ") VALUES (?,?,?,?,NULL,'unknown',?,?) "
            "ON CONFLICT(channel_id, account_id, bridge_user_id)"
            " DO UPDATE SET updated_at=excluded.updated_at",
            (
                identity_id,
                ref.channel_id,
                ref.account_id,
                message.sender_ref,
                now,
                now,
            ),
        )
        row = cur.execute(
            "SELECT identity_id FROM identity"
            " WHERE channel_id=? AND account_id=? AND bridge_user_id=?",
            (ref.channel_id, ref.account_id, message.sender_ref),
        ).fetchone()
        resolved = str(row["identity_id"])
        wxid = (message.sender_wxid or "").strip()
        if wxid:
            # 只有通道可信扩展字段给出 wxid 时才登记证据；
            # 冲突不覆盖已有值，两条证据都留痕。
            cur.execute(
                "INSERT OR IGNORE INTO identity_wxid_evidence ("
                " identity_id, wxid, source, evidence_ref, observed_at"
                ") VALUES (?,?,?,?,?)",
                (
                    resolved,
                    wxid,
                    "bridge_extension",
                    f"event:{message.source_event_id}",
                    now,
                ),
            )
            cur.execute(
                "UPDATE identity SET wxid=?, wxid_source='bridge_extension', updated_at=?"
                " WHERE identity_id=? AND (wxid IS NULL OR wxid='')",
                (wxid, now, resolved),
            )
        alias = (message.display_name or "").strip()
        if alias:
            if ref.conversation_type == "group":
                scope_type, scope_ref = "group", ref.target_ref
            else:
                scope_type, scope_ref = "account", ref.account_id
            cur.execute(
                "INSERT INTO identity_alias ("
                " identity_id, alias, alias_type, scope_type, scope_ref, source, observed_at"
                ") VALUES (?,?,?,?,?,?,?) "
                "ON CONFLICT(identity_id, alias, alias_type, scope_type, scope_ref)"
                " DO UPDATE SET observed_at=excluded.observed_at, source=excluded.source",
                (
                    resolved,
                    alias,
                    "display_name",
                    scope_type,
                    scope_ref,
                    "channel_display_name",
                    now,
                ),
            )
        return resolved

    def _insert_raw(
        self,
        cur: sqlite3.Cursor,
        ref: ConversationRef,
        message: InboundRecord,
        identity_id: str,
        now: str,
    ) -> str:
        record_id = new_record_id()
        text = message.text or ""
        cur.execute(
            "INSERT INTO raw_message ("
            " record_id, agent_id, channel_id, account_id, conversation_id,"
            " conversation_type, target_ref, sender_ref, sender_identity_id,"
            " display_name, text, text_chars, mentions_json, quoted_event_id,"
            " received_at, stored_at, dedupe_scope, source_event_id"
            ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) "
            "ON CONFLICT(channel_id, account_id, source_event_id) DO NOTHING",
            (
                record_id,
                ref.agent_id,
                ref.channel_id,
                ref.account_id,
                ref.conversation_id,
                ref.conversation_type,
                ref.target_ref,
                message.sender_ref,
                identity_id,
                message.display_name,
                text,
                len(text),
                json.dumps(list(message.mentions), ensure_ascii=False),
                message.quoted_event_id,
                message.received_at,
                now,
                message.dedupe_scope,
                message.source_event_id,
            ),
        )
        row = cur.execute(
            "SELECT record_id FROM raw_message"
            " WHERE channel_id=? AND account_id=? AND source_event_id=?",
            (ref.channel_id, ref.account_id, message.source_event_id),
        ).fetchone()
        return str(row["record_id"])

    def append_plugin_job_evidence(self,*,turn_id,agent_id,conversation_id,run_id,evidence,images=()):
        now=_now()
        with self._txn() as cur:
            row=cur.execute('SELECT 1 FROM l2_turn WHERE turn_id=? AND agent_id=? AND conversation_id=?',(turn_id,agent_id,conversation_id)).fetchone()
            if row is None:return False
            cur.execute('INSERT INTO l20_tool_evidence (evidence_id,turn_id,agent_id,run_id,call_id,tool_name,status,error_code,duration_ms,started_at,finished_at,summary,summary_truncated,resource_refs_json,content_persisted,plugin_id,cache_hit,artifact_refs_json,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(turn_id,call_id) DO NOTHING',
                (new_evidence_id(),turn_id,agent_id,run_id,evidence.call_id,evidence.tool_name,evidence.status,evidence.error_code,evidence.duration_ms,evidence.started_at,evidence.finished_at,evidence.summary[:200],0,json.dumps(list(evidence.resource_refs)[:16]),0,evidence.plugin_id,int(bool(evidence.cache_hit)),json.dumps(list(evidence.artifact_refs)[:16]),now))
            for image in images[:10]:
                handle=str(image.get('resource_id') or '')[:100]
                attachment='a-'+hashlib.sha256((turn_id+handle).encode()).hexdigest()[:32]
                state='provided_to_model' if image.get('provided_to_model') else 'mounted_unconfirmed'
                cur.execute("INSERT INTO l2_attachment VALUES(?,?,NULL,'image',?,?,0,?,?) ON CONFLICT(attachment_id) DO NOTHING",(attachment,turn_id,str(image.get('name') or '')[:180],handle,state,now))
            return True

    def finish_turn(
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
        journal_summary_chars: int = 160,
    ) -> bool:
        """收尾：状态、投递记录、工具证据与事件日记一次提交。

        已结束的轮次不再改状态（幂等）；投递记录与工具证据按唯一键忽略重复；
        事件日记按 `turn_id` 幂等，失败时整笔事务回滚并如实计数（不重发、不谎报）。
        """

        now = _now()
        with self._txn() as cur:
            row = cur.execute(
                "SELECT status, conversation_id, user_text, created_at"
                " FROM l2_turn WHERE turn_id=? AND agent_id=?",
                (turn_id, agent_id),
            ).fetchone()
            if row is None:
                raise MemoryStoreError(f"未知轮次：{turn_id}")
            updated = False
            if str(row["status"]) == TURN_PENDING:
                cur.execute(
                    "UPDATE l2_turn SET status=?, run_id=?, assistant_text=?, updated_at=?"
                    " WHERE turn_id=? AND agent_id=? AND status=?",
                    (status, run_id, assistant_text, now, turn_id, agent_id, TURN_PENDING),
                )
                updated = cur.rowcount > 0

            cur.execute(
                "INSERT INTO delivery_record ("
                " turn_id, agent_id, run_id, status, action_id, evidence, text,"
                " text_chars, created_at"
                ") VALUES (?,?,?,?,?,?,?,?,?) ON CONFLICT(turn_id) DO NOTHING",
                (
                    turn_id,
                    agent_id,
                    run_id,
                    delivery_status,
                    action_id,
                    delivery_evidence,
                    delivery_text,
                    len(delivery_text) if delivery_text is not None else None,
                    now,
                ),
            )

            for item in tools:
                cur.execute(
                    "INSERT INTO l20_tool_evidence ("
                    " evidence_id, turn_id, agent_id, run_id, call_id, tool_name,"
                    " status, error_code, duration_ms, started_at, finished_at,"
                    " summary, summary_truncated, resource_refs_json,"
                    " content_persisted, plugin_id, cache_hit, artifact_refs_json, created_at"
                    ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) "
                    "ON CONFLICT(turn_id, call_id) DO NOTHING",
                    (
                        new_evidence_id(),
                        turn_id,
                        agent_id,
                        run_id,
                        item.call_id,
                        item.tool_name,
                        item.status,
                        item.error_code,
                        int(item.duration_ms),
                        item.started_at,
                        item.finished_at,
                        item.summary,
                        0,
                        json.dumps(list(item.resource_refs), ensure_ascii=False),
                        1 if item.content_persisted else 0,
                        (str(item.plugin_id)[:80] if item.plugin_id else None),
                        (1 if item.cache_hit else 0) if item.cache_hit is not None else None,
                        json.dumps(list(item.artifact_refs)[:16], ensure_ascii=False),
                        now,
                    ),
                )

            for image in images[:10]:
                handle=str(image.get("resource_id") or "")[:100]
                aid="a-"+hashlib.sha256((turn_id+handle).encode()).hexdigest()[:32]
                state="provided_to_model" if image.get("provided_to_model") else "mounted_unconfirmed"
                cur.execute("INSERT INTO l2_attachment VALUES(?,?,NULL,'image',?,?,0,?,?) "
                    "ON CONFLICT(attachment_id) DO UPDATE SET recognition_result=excluded.recognition_result",
                    (aid,turn_id,str(image.get("name") or "")[:180],handle,state,now))

            # 事件日记：确定性短条目（只用原文与状态，不调模型、不复制工具正文）。
            self._write_journal(
                cur,
                agent_id=agent_id,
                conversation_id=str(row["conversation_id"]),
                turn_id=turn_id,
                status=status,
                user_text=str(row["user_text"]),
                assistant_present=assistant_text is not None,
                occurred_at=str(row["created_at"]),
                summary_chars=journal_summary_chars,
            )
            return updated

    def mark_interrupted(
        self,
        *,
        turn_id: str,
        agent_id: str,
        reason: str,
        journal_summary_chars: int = 160,
    ) -> bool:
        """把遗留的 pending 轮次标成 interrupted（保留中断证据，**不自动重试**）。"""

        now = _now()
        with self._txn() as cur:
            row = cur.execute(
                "SELECT conversation_id, user_text, created_at FROM l2_turn"
                " WHERE turn_id=? AND agent_id=? AND status=?",
                (turn_id, agent_id, TURN_PENDING),
            ).fetchone()
            cur.execute(
                "UPDATE l2_turn SET status=?, interrupt_reason=?, updated_at=?"
                " WHERE turn_id=? AND agent_id=? AND status=?",
                (TURN_INTERRUPTED, reason[:200], now, turn_id, agent_id, TURN_PENDING),
            )
            marked = cur.rowcount > 0
            if marked and row is not None:
                self._write_journal(
                    cur,
                    agent_id=agent_id,
                    conversation_id=str(row["conversation_id"]),
                    turn_id=turn_id,
                    status=TURN_INTERRUPTED,
                    user_text=str(row["user_text"]),
                    assistant_present=False,
                    occurred_at=str(row["created_at"]),
                    summary_chars=journal_summary_chars,
                )
            return marked

    def record_wxid_evidence(
        self,
        *,
        identity_id: str,
        wxid: str,
        source: str,
        evidence_ref: Optional[str],
        observed_at: Optional[str] = None,
    ) -> bool:
        """登记可信 wxid 补充；只接受白名单来源，其余明确拒绝。

        桥当前不提供 wxid，本方法只是**接口预留**：没有可信证据时身份保持 unknown。
        """

        if source not in WXID_TRUSTED_SOURCES:
            raise MemoryStoreError(
                f"不接受不可信的 wxid 来源：{source}（允许：{', '.join(WXID_TRUSTED_SOURCES)}）"
            )
        if not wxid.strip():
            raise MemoryStoreError("wxid 不能为空")
        now = observed_at or _now()
        with self._txn() as cur:
            row = cur.execute(
                "SELECT identity_id FROM identity WHERE identity_id=?",
                (identity_id,),
            ).fetchone()
            if row is None:
                return False
            cur.execute(
                "INSERT OR IGNORE INTO identity_wxid_evidence ("
                " identity_id, wxid, source, evidence_ref, observed_at"
                ") VALUES (?,?,?,?,?)",
                (identity_id, wxid.strip(), source, evidence_ref, now),
            )
            cur.execute(
                "UPDATE identity SET wxid=?, wxid_source=?, updated_at=?"
                " WHERE identity_id=?",
                (wxid.strip(), source, now, identity_id),
            )
            return True

    # ------------------------------------------------------------------ #
    # 写入：L4 明确保存 + 事件日记
    # ------------------------------------------------------------------ #

    def save_explicit_memory(
        self,
        *,
        ref: ConversationRef,
        request: ExplicitSaveRequest,
        max_chars: int,
    ) -> SaveOutcome:
        """把一次"明确保存"写进 L4（单事务）；返回真实记录 ID 或明确的未保存状态。

        - 幂等以**请求槽位**为核心（`request_slot`，缺省回退到来源原文 ID）：
          同一请求改写内容只会拿到原记录（`duplicate`），不能新建第二份；
        - 内容为空 → `needs_content`，**不编造**、也不留空记录；
        - 内容超上限 → `too_long`，**不静默截断后报成功**；
        - 事务失败由调用方按 `MemoryWriteError` 计数，不谎称已保存。
        """

        content = (request.content or "").strip()
        record_ids = tuple(dict.fromkeys(rid for rid in request.record_ids if rid))
        if not content:
            return SaveOutcome(
                status=SAVE_NEEDS_CONTENT,
                record_ids=record_ids,
                turn_id=request.turn_id,
                detail="没有可核对的内容：需要用户补充要记住的事实。",
            )
        limit = max(int(max_chars), 1)
        if len(content) > limit:
            return SaveOutcome(
                status=SAVE_TOO_LONG,
                record_ids=record_ids,
                turn_id=request.turn_id,
                detail=(
                    f"要保存的内容有 {len(content)} 字符，超过 {limit} 字符上限，"
                    "本次没有保存；请用户确认要保留的部分。"
                ),
            )

        slot = (request.request_slot or "").strip()
        dedupe_basis = slot or "\x1f".join(record_ids)
        dedupe_key = hashlib.sha1(
            "\x1f".join([ref.agent_id, ref.conversation_id, dedupe_basis]).encode("utf-8")
        ).hexdigest()
        occurred_at = request.occurred_at or _now()
        now = _now()

        with self._txn() as cur:
            existing = cur.execute(
                "SELECT memory_id FROM l4_memory WHERE agent_id=? AND dedupe_key=?",
                (ref.agent_id, dedupe_key),
            ).fetchone()
            if existing is not None:
                return SaveOutcome(
                    status=SAVE_DUPLICATE,
                    memory_id=str(existing["memory_id"]),
                    content=content,
                    subject_identity_id=request.subject_identity_id,
                    subject_label=request.subject_label,
                    record_ids=record_ids,
                    turn_id=request.turn_id,
                    created=False,
                    detail="该请求已保存过，未重复写入。",
                )

            memory_id = new_memory_id()
            cur.execute(
                "INSERT INTO l4_memory ("
                " memory_id, agent_id, conversation_id, channel_id, account_id,"
                " kind, status, content, content_chars, subject_identity_id,"
                " subject_label, request_text, request_record_id, turn_id,"
                " dedupe_key, occurred_at, saved_at, updated_at"
                ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    memory_id,
                    ref.agent_id,
                    ref.conversation_id,
                    ref.channel_id,
                    ref.account_id,
                    request.trigger,
                    SAVE_SAVED,
                    content,
                    len(content),
                    request.subject_identity_id,
                    request.subject_label,
                    (request.request_text or "")[:1000],
                    record_ids[0] if record_ids else None,
                    request.turn_id,
                    dedupe_key,
                    occurred_at,
                    now,
                    now,
                ),
            )
            for position, record_id in enumerate(record_ids):
                cur.execute(
                    "INSERT OR IGNORE INTO l4_memory_source ("
                    " memory_id, source_type, source_ref, position"
                    ") VALUES (?,?,?,?)",
                    (memory_id, "raw_message", record_id, position),
                )
            if request.turn_id:
                cur.execute(
                    "INSERT OR IGNORE INTO l4_memory_source ("
                    " memory_id, source_type, source_ref, position"
                    ") VALUES (?,?,?,?)",
                    (memory_id, "l2_turn", request.turn_id, len(record_ids)),
                )

        return SaveOutcome(
            status=SAVE_SAVED,
            memory_id=memory_id,
            content=content,
            subject_identity_id=request.subject_identity_id,
            subject_label=request.subject_label,
            record_ids=record_ids,
            turn_id=request.turn_id,
            created=True,
            detail="已提交（来源：" + (",".join(record_ids) or "无") + "）",
        )

    def _write_journal(
        self,
        cur: sqlite3.Cursor,
        *,
        agent_id: str,
        conversation_id: str,
        turn_id: str,
        status: str,
        user_text: str,
        assistant_present: bool,
        occurred_at: str,
        summary_chars: int,
    ) -> bool:
        """按已收尾轮次确定性生成一条事件日记（不调模型、不复制正文全文）。

        参与者与来源都从 `l2_turn_source → raw_message` 取，幂等键是 `turn_id`：
        重复收尾只忽略，不新增条目。
        """

        limit = max(int(summary_chars), 1)
        summary = " ".join((user_text or "").split())
        if len(summary) > limit:
            summary = summary[:limit] + "…"
        rows = cur.execute(
            "SELECT DISTINCT m.sender_identity_id, m.record_id FROM l2_turn_source s"
            " JOIN raw_message m ON m.record_id = s.record_id"
            " WHERE s.turn_id=? ORDER BY s.position",
            (turn_id,),
        ).fetchall()
        participants = [
            str(row["sender_identity_id"])
            for row in rows
            if row["sender_identity_id"]
        ]
        source_ids = [str(row["record_id"]) for row in rows]
        cur.execute(
            "INSERT INTO event_journal ("
            " entry_id, agent_id, conversation_id, turn_id, kind, status, summary,"
            " summary_chars, participants_json, source_record_ids_json,"
            " has_assistant, occurred_at, created_at"
            ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)"
            " ON CONFLICT(turn_id) DO NOTHING",
            (
                new_journal_id(),
                agent_id,
                conversation_id,
                turn_id,
                "turn",
                status,
                summary,
                len(summary),
                json.dumps(sorted(set(participants)), ensure_ascii=False),
                json.dumps(source_ids, ensure_ascii=False),
                1 if assistant_present else 0,
                occurred_at,
                _now(),
            ),
        )
        return cur.rowcount > 0

    # ------------------------------------------------------------------ #
    # 读取：重建模型历史
    # ------------------------------------------------------------------ #

    def load_history(
        self,
        *,
        agent_id: str,
        conversation_id: str,
        turn_limit: int,
        char_budget: int,
        evidence_per_turn: int,
    ) -> HistoryLoad:
        """重建模型历史：已收尾轮次 + 有界 L20 证据，按完整轮次 + 总字符预算收集。

        - 用户原文：所有**已收尾**轮次都保留（未发送回答也是用户事实）；
        - 助手正文：只有 `sent` 才有；
        - 工具证据：每轮最多 `evidence_per_turn` 条，SQL 用窗口函数限制行数；
        - **硬预算**：字符数用 `StoredTurn.history_cost`（与 `_hydrate_history` 最终入模文本
          逐字符一致），**第一轮也要过预算**；整轮放不下就停止，不拆半轮；
        - `pending` 视为未确认，不进入历史（避免半个事实）；
        - 最新一轮就超预算时返回空历史 + `newest_over_budget`，不强行加载超限内容。
        """

        budget = max(int(char_budget), 0)
        with self._txn() as cur:
            rows = cur.execute(
                "SELECT turn_id, status, user_text, assistant_text, created_at,"
                " interrupt_reason FROM l2_turn"
                " WHERE agent_id=? AND conversation_id=? AND status<>?"
                " ORDER BY created_at DESC, turn_id DESC LIMIT ?",
                (agent_id, conversation_id, TURN_PENDING, int(turn_limit)),
            ).fetchall()
            if not rows:
                return HistoryLoad(reason=HISTORY_NO_TURNS, budget_chars=budget)
            turn_ids = [str(row["turn_id"]) for row in rows]
            evidence = self._evidence_for_turns(cur, turn_ids, evidence_per_turn)

        selected: List[StoredTurn] = []
        total = 0
        reason = HISTORY_OK
        for row in rows:
            turn_id = str(row["turn_id"])
            turn = StoredTurn(
                turn_id=turn_id,
                status=str(row["status"]),
                user_text=str(row["user_text"]),
                assistant_text=(
                    str(row["assistant_text"]) if row["assistant_text"] else None
                ),
                created_at=str(row["created_at"]),
                tool_evidence=evidence.get(turn_id, ()),
                interrupted_reason=(
                    str(row["interrupt_reason"]) if row["interrupt_reason"] else None
                ),
            )
            cost = turn.history_cost
            if total + cost > budget:
                # 整轮放不下：停止（保留更近的连续轮次），不切半轮、不超预算。
                reason = (
                    HISTORY_NEWEST_OVER_BUDGET if not selected else HISTORY_CHAR_BUDGET
                )
                break
            selected.append(turn)
            total += cost

        dropped = len(rows) - len(selected)
        if not selected:
            return HistoryLoad(
                turns=(),
                reason=HISTORY_NEWEST_OVER_BUDGET,
                budget_chars=budget,
                used_chars=0,
                dropped_turns=dropped,
            )
        selected.reverse()
        return HistoryLoad(
            turns=tuple(selected),
            reason=reason,
            budget_chars=budget,
            used_chars=total,
            dropped_turns=dropped,
        )

    def _evidence_for_turns(
        self, cur: sqlite3.Cursor, turn_ids: Sequence[str], per_turn: int
    ) -> Dict[str, Tuple[StoredEvidence, ...]]:
        """按轮次取有界工具证据：SQL 侧用 `ROW_NUMBER` 限行，避免全量读出再截断。"""

        if not turn_ids or per_turn <= 0:
            return {}
        rows = cur.execute(
            "SELECT turn_id, run_id, call_id, tool_name, status, summary,"
            " duration_ms, resource_refs_json FROM ("
            "  SELECT turn_id, run_id, call_id, tool_name, status, summary,"
            "   duration_ms, resource_refs_json,"
            "   ROW_NUMBER() OVER ("
            "     PARTITION BY turn_id ORDER BY created_at, evidence_id"
            "   ) AS rn"
            "  FROM l20_tool_evidence WHERE turn_id IN ("
            + ",".join("?" for _ in turn_ids)
            + ")"
            " ) WHERE rn <= ? ORDER BY turn_id, rn",
            (*turn_ids, int(per_turn)),
        ).fetchall()
        grouped: Dict[str, List[StoredEvidence]] = {}
        for row in rows:
            try:
                refs = tuple(json.loads(row["resource_refs_json"] or "[]"))
            except (ValueError, TypeError):
                refs = ()
            grouped.setdefault(str(row["turn_id"]), []).append(
                StoredEvidence(
                    tool_name=str(row["tool_name"]),
                    status=str(row["status"]),
                    call_id=str(row["call_id"]),
                    run_id=str(row["run_id"]) if row["run_id"] else None,
                    summary=str(row["summary"] or ""),
                    duration_ms=int(row["duration_ms"] or 0),
                    resource_refs=tuple(str(ref) for ref in refs),
                )
            )
        return {key: tuple(value) for key, value in grouped.items()}

    # ------------------------------------------------------------------ #
    # 读取：只读接口
    # ------------------------------------------------------------------ #

    def query_records(
        self,
        *,
        agent_id: str,
        layer: str,
        conversation_id: Optional[str],
        identity_id: Optional[str],
        since: Optional[str],
        until: Optional[str],
        limit: int,
        offset: int,
        preview_chars: int,
        keyword: Optional[str] = None,
    ) -> Tuple[List[Dict[str, Any]], int]:
        """按层查询记录摘要（不含整段正文），返回 ``(items, total)``。

        排序稳定：`created_at DESC, record_id DESC`；时间过滤按 ISO 8601 字符串比较。
        """

        if layer == "L3":
            # P15-R01：L3 是可查询记录层（层/人物/会话/日期/关键词筛选）。
            return self.l3_query_records(
                agent_id=agent_id,
                conversation_id=conversation_id,
                identity_id=identity_id,
                since=since,
                until=until,
                keyword=keyword,
                limit=limit,
                offset=offset,
                preview_chars=preview_chars,
            )

        if layer == LAYER_RAW:
            where = ["agent_id=?"]
            params: List[Any] = [agent_id]
            if conversation_id:
                where.append("conversation_id=?")
                params.append(conversation_id)
            if identity_id:
                where.append("sender_identity_id=?")
                params.append(identity_id)
            if since:
                where.append("bj_epoch(stored_at)>=?")
                params.append(_require_epoch(since, "since"))
            if until:
                where.append("bj_epoch(stored_at)<?")
                params.append(_require_epoch(until, "until"))
            if keyword:
                where.append("text LIKE ?")
                params.append(f"%{keyword}%")
            clause = " AND ".join(where)
            sql = (
                "SELECT record_id, conversation_id, stored_at AS created_at,"
                " text_chars AS chars, substr(text, 1, ?) AS preview,"
                " length(text) AS text_length, sender_identity_id AS identity_id,"
                " source_event_id"
                " FROM raw_message WHERE "
                + clause
                + " ORDER BY bj_epoch(created_at) DESC, record_id DESC LIMIT ? OFFSET ?"
            )
            count_sql = "SELECT COUNT(*) FROM raw_message WHERE " + clause
            with self._txn() as cur:
                rows = cur.execute(
                    sql, [int(preview_chars)] + params + [limit, offset]
                ).fetchall()
                total = int(cur.execute(count_sql, params).fetchone()[0])
            return [self._raw_item(row) for row in rows], total

        if layer == LAYER_L2:
            where = ["agent_id=?"]
            params = [agent_id]
            if conversation_id:
                where.append("conversation_id=?")
                params.append(conversation_id)
            if identity_id:
                # 合并批次：整轮任一条原文的发送者都算命中，不只是首位。
                where.append(_participant_clause("l2_turn.turn_id"))
                params.append(identity_id)
            if since:
                where.append("bj_epoch(created_at)>=?")
                params.append(_require_epoch(since, "since"))
            if until:
                where.append("bj_epoch(created_at)<?")
                params.append(_require_epoch(until, "until"))
            if keyword:
                where.append("user_text LIKE ?")
                params.append(f"%{keyword}%")
            clause = " AND ".join(where)
            sql = (
                "SELECT turn_id, conversation_id, created_at, status, user_chars,"
                " substr(user_text, 1, ?) AS preview,"
                " length(user_text) AS text_length,"
                " sender_identity_id AS identity_id,"
                " (assistant_text IS NOT NULL) AS has_assistant"
                " FROM l2_turn WHERE "
                + clause
                + " ORDER BY bj_epoch(created_at) DESC, turn_id DESC LIMIT ? OFFSET ?"
            )
            count_sql = "SELECT COUNT(*) FROM l2_turn WHERE " + clause
            with self._txn() as cur:
                rows = cur.execute(
                    sql, [int(preview_chars)] + params + [limit, offset]
                ).fetchall()
                total = int(cur.execute(count_sql, params).fetchone()[0])
            return [self._turn_item(row) for row in rows], total

        if layer == LAYER_L20:
            where = ["e.agent_id=?"]
            params = [agent_id]
            # 始终左连接轮次表：既用于过滤，也用于返回会话键。
            join = " LEFT JOIN l2_turn t ON t.turn_id = e.turn_id"
            if conversation_id:
                where.append("t.conversation_id=?")
                params.append(conversation_id)
            if identity_id:
                # 工具证据按所属轮次的参与者过滤（合并批次可能由非首位成员触发）。
                where.append(_participant_clause("t.turn_id"))
                params.append(identity_id)
            if since:
                where.append("bj_epoch(e.created_at)>=?")
                params.append(_require_epoch(since, "since"))
            if until:
                where.append("bj_epoch(e.created_at)<?")
                params.append(_require_epoch(until, "until"))
            if keyword:
                where.append("e.summary LIKE ?")
                params.append(f"%{keyword}%")
            clause = " AND ".join(where)
            sql = (
                "SELECT e.evidence_id, e.turn_id, e.tool_name, e.call_id, e.status,"
                " e.error_code, substr(e.summary, 1, ?) AS preview,"
                " length(e.summary) AS text_length, e.created_at,"
                " t.conversation_id AS conversation_id"
                " FROM l20_tool_evidence e"
                + join
                + " WHERE "
                + clause
                + " ORDER BY bj_epoch(e.created_at) DESC, e.evidence_id DESC LIMIT ? OFFSET ?"
            )
            count_sql = (
                "SELECT COUNT(*) FROM l20_tool_evidence e" + join + " WHERE " + clause
            )
            with self._txn() as cur:
                rows = cur.execute(
                    sql, [int(preview_chars)] + params + [limit, offset]
                ).fetchall()
                total = int(cur.execute(count_sql, params).fetchone()[0])
            return [self._tool_item(row) for row in rows], total

        if layer == LAYER_L4:
            where = ["l.agent_id=?"]
            params = [agent_id]
            if conversation_id:
                where.append("l.conversation_id=?")
                params.append(conversation_id)
            if identity_id:
                # 与 `search()` 相同的三路身份匹配：L2 归档优先取 chat_event 的
                # identity_id，生产链路未写时回溯 raw_message.sender_identity_id。
                summary_branch = ("(l.kind='archive_summary' AND EXISTS ("
                                  "SELECT 1 FROM l4_memory_source src JOIN l2_turn_source ts "
                                  "ON ts.turn_id=src.source_ref JOIN raw_message rm "
                                  "ON rm.record_id=ts.record_id "
                                  "WHERE src.memory_id=l.memory_id AND src.source_type='l2_turn' "
                                  "AND rm.sender_identity_id=?))")
                l2_branch = ("(l.kind='l2_archive' AND EXISTS ("
                             "SELECT 1 FROM l4_memory_source src2 JOIN chat_event ce "
                             "ON ce.event_id=src2.source_ref "
                             "WHERE src2.memory_id=l.memory_id AND src2.source_type='l2_event' "
                             "AND (ce.identity_id=? OR EXISTS ("
                             "SELECT 1 FROM l2_turn_source ts2 "
                             "JOIN raw_message rm2 ON rm2.record_id = ts2.record_id "
                             "WHERE ts2.record_id = ce.source_record_id "
                             "AND rm2.sender_identity_id=?))))")
                where.append("(l.subject_identity_id=? OR " + summary_branch
                             + " OR " + l2_branch + ")")
                params.extend([identity_id, identity_id, identity_id, identity_id])
            if since:
                where.append("l.saved_at>=?")
                params.append(since)
            if until:
                where.append("l.saved_at<?")
                params.append(until)
            if keyword:
                where.append("l.content LIKE ?")
                params.append(f"%{keyword}%")
            clause = " AND ".join(where)
            sql = (
                "SELECT l.memory_id AS record_id, l.conversation_id,"
                " l.saved_at AS created_at, l.status, l.kind, l.content_chars AS text_chars,"
                " substr(l.content, 1, ?) AS preview,"
                " length(l.content) AS text_length,"
                " l.subject_identity_id AS identity_id, l.request_record_id"
                " FROM l4_memory l WHERE "
                + clause
                + " ORDER BY bj_epoch(created_at) DESC, record_id DESC LIMIT ? OFFSET ?"
            )
            count_sql = "SELECT COUNT(*) FROM l4_memory l WHERE " + clause
            with self._txn() as cur:
                rows = cur.execute(
                    sql, [int(preview_chars)] + params + [limit, offset]
                ).fetchall()
                total = int(cur.execute(count_sql, params).fetchone()[0])
            return [self._l4_item(row) for row in rows], total

        if layer == LAYER_JOURNAL:
            where = ["j.agent_id=?"]
            params = [agent_id]
            if conversation_id:
                where.append("j.conversation_id=?")
                params.append(conversation_id)
            if identity_id:
                # 参与者存在 JSON 数组里：按带引号的精确 ID 匹配，避免前缀误命中。
                where.append("j.participants_json LIKE ?")
                params.append(f'%"{identity_id}"%')
            if since:
                where.append("bj_epoch(j.occurred_at)>=?")
                params.append(_require_epoch(since, "since"))
            if until:
                where.append("bj_epoch(j.occurred_at)<?")
                params.append(_require_epoch(until, "until"))
            if keyword:
                where.append("j.summary LIKE ?")
                params.append(f"%{keyword}%")
            clause = " AND ".join(where)
            sql = (
                "SELECT j.entry_id AS record_id, j.conversation_id,"
                " j.occurred_at AS created_at, j.status, j.summary_chars AS text_chars,"
                " substr(j.summary, 1, ?) AS preview,"
                " length(j.summary) AS text_length, j.turn_id, j.has_assistant"
                " FROM event_journal j WHERE "
                + clause
                + " ORDER BY bj_epoch(created_at) DESC, record_id DESC LIMIT ? OFFSET ?"
            )
            count_sql = "SELECT COUNT(*) FROM event_journal j WHERE " + clause
            with self._txn() as cur:
                rows = cur.execute(
                    sql, [int(preview_chars)] + params + [limit, offset]
                ).fetchall()
                total = int(cur.execute(count_sql, params).fetchone()[0])
            return [self._journal_item(row) for row in rows], total

        raise MemoryStoreError(f"不支持的记录层：{layer}")

    # ------------------------------------------------------------------ #
    # 读取：单条详情
    # ------------------------------------------------------------------ #

    def get_record(self, *, agent_id: str, record_id: str) -> Optional[Dict[str, Any]]:
        if record_id.startswith("f3-"):
            # P15-R01：L3 事实详情（来源片段/证据类型/状态），不是拼原文冒充。
            return self.l3_detail(agent_id=agent_id, fact_id=record_id)
        if record_id.startswith("m-"):
            return self._raw_detail(agent_id=agent_id, record_id=record_id)
        if record_id.startswith("t-"):
            return self._turn_detail(agent_id=agent_id, turn_id=record_id)
        if record_id.startswith("e-"):
            return self._tool_detail(agent_id=agent_id, evidence_id=record_id)
        if record_id.startswith("l4-"):
            return self._l4_detail(agent_id=agent_id, memory_id=record_id)
        if record_id.startswith("ev-"):
            return self._journal_detail(agent_id=agent_id, entry_id=record_id)
        return None

    def _l4_detail(self, *, agent_id: str, memory_id: str) -> Optional[Dict[str, Any]]:
        with self._txn() as cur:
            row = cur.execute(
                "SELECT * FROM l4_memory WHERE memory_id=? AND agent_id=?",
                (memory_id, agent_id),
            ).fetchone()
            if row is None:
                return None
            sources = cur.execute(
                "SELECT source_type, source_ref, position FROM l4_memory_source"
                " WHERE memory_id=? ORDER BY position",
                (memory_id,),
            ).fetchall()
            aliases: List[Dict[str, Any]] = []
            try:
                channel_id, account_id, conversation_type, target_ref = parse_conversation_id(
                    str(row["conversation_id"])
                )
            except MemoryQueryError:
                channel_id = ""
                account_id = ""
                conversation_type = ""
                target_ref = ""
            if channel_id and row["subject_identity_id"]:
                scope_type, scope_ref = self._scope_from_conversation(
                    conversation_type, target_ref, account_id
                )
                aliases = self._identity_aliases(
                    cur,
                    str(row["subject_identity_id"]),
                    account_id=account_id,
                    channel_id=channel_id,
                    scope_type=scope_type,
                    scope_ref=scope_ref,
                )
        return {
            "record_id": str(row["memory_id"]),
            "layer": LAYER_L4,
            "agent_id": str(row["agent_id"]),
            "conversation_id": str(row["conversation_id"]),
            "status": str(row["status"]),
            "created_at": str(row["saved_at"]),
            "updated_at": str(row["updated_at"]),
            "occurred_at": str(row["occurred_at"]),
            "text": str(row["content"]),
            "text_chars": int(row["content_chars"]),
            "kind": str(row["kind"]),
            "request": {
                "text": str(row["request_text"]),
                "record_id": row["request_record_id"],
                "turn_id": row["turn_id"],
            },
            "subject": {
                "identity_id": row["subject_identity_id"],
                "label": row["subject_label"],
                "aliases": aliases,
            },
            "sources": [
                {
                    "type": str(item["source_type"]),
                    "ref": str(item["source_ref"]),
                    "position": int(item["position"]),
                }
                for item in sources
            ],
        }

    def _journal_detail(self, *, agent_id: str, entry_id: str) -> Optional[Dict[str, Any]]:
        with self._txn() as cur:
            row = cur.execute(
                "SELECT * FROM event_journal WHERE entry_id=? AND agent_id=?",
                (entry_id, agent_id),
            ).fetchone()
            if row is None:
                return None
        return {
            "record_id": str(row["entry_id"]),
            "layer": LAYER_JOURNAL,
            "agent_id": str(row["agent_id"]),
            "conversation_id": str(row["conversation_id"]),
            "status": str(row["status"]),
            "created_at": str(row["occurred_at"]),
            "updated_at": str(row["created_at"]),
            "text": str(row["summary"]),
            "text_chars": int(row["summary_chars"]),
            "kind": str(row["kind"]),
            "turn_id": str(row["turn_id"]),
            "has_assistant": bool(row["has_assistant"]),
            "participants": json.loads(str(row["participants_json"]) or "[]"),
            "sources": json.loads(str(row["source_record_ids_json"]) or "[]"),
        }

    def _raw_detail(self, *, agent_id: str, record_id: str) -> Optional[Dict[str, Any]]:
        with self._txn() as cur:
            row = cur.execute(
                "SELECT * FROM raw_message WHERE record_id=? AND agent_id=?",
                (record_id, agent_id),
            ).fetchone()
            if row is None:
                return None
            scope_type, scope_ref = self._scope_from_conversation(
                str(row["conversation_type"]),
                str(row["target_ref"]),
                str(row["account_id"]),
            )
            aliases = self._identity_aliases(
                cur,
                str(row["sender_identity_id"]),
                account_id=str(row["account_id"]),
                channel_id=str(row["channel_id"]),
                scope_type=scope_type,
                scope_ref=scope_ref,
            )
            wxid = cur.execute(
                "SELECT wxid, wxid_source FROM identity WHERE identity_id=?",
                (row["sender_identity_id"],),
            ).fetchone()
        return {
            "record_id": str(row["record_id"]),
            "layer": LAYER_RAW,
            "agent_id": str(row["agent_id"]),
            "conversation_id": str(row["conversation_id"]),
            "conversation_type": str(row["conversation_type"]),
            "channel_id": str(row["channel_id"]),
            "account_id": str(row["account_id"]),
            "target_ref": str(row["target_ref"]),
            "status": "stored",
            "created_at": str(row["stored_at"]),
            "text": str(row["text"]),
            "text_chars": int(row["text_chars"]),
            "source": {
                "source_event_id": str(row["source_event_id"]),
                "received_at": row["received_at"],
                "dedupe_scope": str(row["dedupe_scope"]),
                "quoted_event_id": row["quoted_event_id"],
            },
            "sender": {
                "sender_ref": str(row["sender_ref"]),
                "identity_id": row["sender_identity_id"],
                "display_name": row["display_name"],
                "wxid_known": bool(wxid["wxid"]) if wxid is not None else False,
                "wxid_source": wxid["wxid_source"] if wxid is not None else "unknown",
                "aliases": aliases,
            },
        }

    def _turn_detail(self, *, agent_id: str, turn_id: str) -> Optional[Dict[str, Any]]:
        with self._txn() as cur:
            row = cur.execute(
                "SELECT * FROM l2_turn WHERE turn_id=? AND agent_id=?",
                (turn_id, agent_id),
            ).fetchone()
            if row is None:
                return None
            delivery = cur.execute(
                "SELECT status, action_id, evidence, text, created_at"
                " FROM delivery_record WHERE turn_id=?",
                (turn_id,),
            ).fetchone()
            sources = cur.execute(
                "SELECT m.record_id,m.text,m.display_name,m.sender_ref,m.received_at FROM l2_turn_source s JOIN raw_message m ON m.record_id=s.record_id WHERE s.turn_id=? ORDER BY s.position",
                (turn_id,),
            ).fetchall()
            tools = cur.execute(
                "SELECT evidence_id, tool_name, call_id, status, error_code, summary,"
                " duration_ms, started_at, finished_at"
                " FROM l20_tool_evidence WHERE turn_id=? ORDER BY created_at LIMIT 20",
                (turn_id,),
            ).fetchall()
            attachments=cur.execute("SELECT attachment_id,kind,name,handle,recognized,recognition_result "
                "FROM l2_attachment WHERE turn_id=? ORDER BY created_at,attachment_id LIMIT 10",(turn_id,)).fetchall()
            aliases: List[Dict[str, Any]] = []
            try:
                channel_id, account_id, conversation_type, target_ref = parse_conversation_id(
                    str(row["conversation_id"])
                )
            except MemoryQueryError:
                channel_id = ""
                account_id = ""
                conversation_type = ""
                target_ref = ""
            if channel_id and row["sender_identity_id"]:
                scope_type, scope_ref = self._scope_from_conversation(
                    conversation_type, target_ref, account_id
                )
                aliases = self._identity_aliases(
                    cur,
                    str(row["sender_identity_id"]),
                    account_id=account_id,
                    channel_id=channel_id,
                    scope_type=scope_type,
                    scope_ref=scope_ref,
                )
        return {
            "record_id": str(row["turn_id"]),
            "layer": LAYER_L2,
            "agent_id": str(row["agent_id"]),
            "conversation_id": str(row["conversation_id"]),
            "status": str(row["status"]),
            "created_at": str(row["created_at"]),
            "updated_at": str(row["updated_at"]),
            "run_id": row["run_id"],
            "text": str(row["user_text"]),
            "text_chars": int(row["user_chars"]),
            "assistant_text": row["assistant_text"],
            "assistant_chars": len(str(row["assistant_text"]))
            if row["assistant_text"] is not None
            else 0,
            "spoken": str(row["status"]) == TURN_SENT,
            "interrupt_reason": row["interrupt_reason"],
            # 仅展示**首个来源**的发送者：合并批次可能有多位参与者，过滤/查询请用
            # `identity_id` 参数（按参与者语义）或按 `sources` 逐条查原文。
            "sender": {
                "sender_ref": str(row["sender_ref"]),
                "identity_id": row["sender_identity_id"],
                "aliases": aliases,
            },
            "delivery": (
                {
                    "status": str(delivery["status"]),
                    "action_id": delivery["action_id"],
                    "evidence": str(delivery["evidence"]),
                    "text_chars": len(str(delivery["text"]))
                    if delivery["text"] is not None
                    else 0,
                    "created_at": str(delivery["created_at"]),
                }
                if delivery is not None
                else None
            ),
            "sources": [str(item["record_id"]) for item in sources],
            "source_messages": [dict(item) for item in sources],
            "attachments": [dict(item) | {"recognized":bool(item["recognized"]),
                "input_state":item["recognition_result"] or "catalog_only"} for item in attachments],
            "tool_evidence": [
                {
                    "evidence_id": str(item["evidence_id"]),
                    "tool_name": str(item["tool_name"]),
                    "call_id": str(item["call_id"]),
                    "status": str(item["status"]),
                    "error_code": item["error_code"],
                    "summary": str(item["summary"]),
                    "duration_ms": int(item["duration_ms"]),
                    "started_at": str(item["started_at"]),
                    "finished_at": str(item["finished_at"]),
                }
                for item in tools
            ],
        }

    def _tool_detail(
        self, *, agent_id: str, evidence_id: str
    ) -> Optional[Dict[str, Any]]:
        with self._txn() as cur:
            row = cur.execute(
                "SELECT * FROM l20_tool_evidence WHERE evidence_id=? AND agent_id=?",
                (evidence_id, agent_id),
            ).fetchone()
            if row is None:
                return None
            turn = cur.execute(
                "SELECT conversation_id, status FROM l2_turn WHERE turn_id=?",
                (row["turn_id"],),
            ).fetchone()
        return {
            "record_id": str(row["evidence_id"]),
            "layer": LAYER_L20,
            "agent_id": str(row["agent_id"]),
            "conversation_id": str(turn["conversation_id"]) if turn is not None else None,
            "status": str(row["status"]),
            "created_at": str(row["created_at"]),
            "text": str(row["summary"]),
            "text_chars": len(str(row["summary"])),
            "run_id": row["run_id"],
            "turn_id": str(row["turn_id"]),
            "turn_status": str(turn["status"]) if turn is not None else None,
            "tool": {
                "name": str(row["tool_name"]),
                "call_id": str(row["call_id"]),
                "error_code": row["error_code"],
                "duration_ms": int(row["duration_ms"]),
                "started_at": str(row["started_at"]),
                "finished_at": str(row["finished_at"]),
                "resource_refs": json.loads(str(row["resource_refs_json"]) or "[]"),
                "content_persisted": bool(row["content_persisted"]),
            },
        }

    # ------------------------------------------------------------------ #
    # 读取：关键词检索（回答前检索的存储实现）
    # ------------------------------------------------------------------ #

    def search(
        self,
        *,
        agent_id: str,
        conversation_id: Optional[str],
        identity_ids: Sequence[str],
        query: str,
        layers: Sequence[str],
        limit: int,
        item_chars: int,
    ) -> Tuple[List[RetrievalHit], str]:
        """在当前 Agent/会话可见范围内做关键词检索，返回受控命中与说明。

        - 关键词策略见 `pero.memory.keywords`（英文词 + 中文二元组，任一命中）；
        - 只查 L4 明确保存、事件日记、原文来源三层，不碰工具正文；
        - `identity_ids` 非空时按"该身份是 L4 对象 / 日记参与者 / 原文发送者"过滤；
        - 每条命中带层、记录 ID、时间、来源引用与有界预览；排序为
          `命中词数 DESC, 时间 DESC, 记录 ID DESC`，跨层合并后统一切断。
        """

        tokens = tokenize(query)
        if not tokens:
            return [], "关键词为空（没有可检索的词）。"
        per_layer = max(int(limit), 1)
        fetch = max(per_layer * 3, per_layer)
        preview_limit = max(int(item_chars), 1)
        wanted = set(layers)
        hits: List[RetrievalHit] = []

        with self._txn() as cur:
            if LAYER_L4 in wanted:
                clause, clause_params = like_clause("l4.content", tokens)
                sql = (
                    "SELECT l4.memory_id AS record_id, l4.content AS text,"
                    " l4.saved_at AS occurred_at, l4.subject_label, l4.kind,"
                    " l4.request_record_id FROM l4_memory l4"
                    " WHERE l4.agent_id=? AND l4.status=?"
                )
                params: List[Any] = [agent_id, SAVE_SAVED]
                if conversation_id:
                    sql += " AND l4.conversation_id=?"
                    params.append(conversation_id)
                if identity_ids:
                    marks = ",".join("?" for _ in identity_ids)
                    # L2 独立交互归档（kind='l2_archive'）同样可按参与者检索：
                    # 来源是 l2_event（即 chat_event.event_id）。人物身份优先取
                    # chat_event.identity_id，生产链路不写该字段时回溯原始来源
                    # （chat_event.source_record_id → l2_turn_source.record_id →
                    # raw_message.sender_identity_id）。
                    summary_branch = (
                        "(l4.kind='archive_summary' AND EXISTS (SELECT 1 "
                        "FROM l4_memory_source src "
                        "JOIN l2_turn_source ts ON ts.turn_id=src.source_ref "
                        "JOIN raw_message rm ON rm.record_id=ts.record_id "
                        "WHERE src.memory_id=l4.memory_id AND src.source_type='l2_turn' "
                        "AND rm.sender_identity_id IN (" + marks + ")))"
                    )
                    l2_branch = (
                        "(l4.kind='l2_archive' AND EXISTS (SELECT 1 "
                        "FROM l4_memory_source src2 "
                        "JOIN chat_event ce ON ce.event_id=src2.source_ref "
                        "WHERE src2.memory_id=l4.memory_id AND src2.source_type='l2_event' "
                        "AND (ce.identity_id IN (" + marks + ") OR EXISTS ("
                        "SELECT 1 FROM l2_turn_source ts2 "
                        "JOIN raw_message rm2 ON rm2.record_id = ts2.record_id "
                        "WHERE ts2.record_id = ce.source_record_id "
                        "AND rm2.sender_identity_id IN (" + marks + ")))))"
                    )
                    sql += (" AND (l4.subject_identity_id IN (" + marks + ") OR "
                            + summary_branch + " OR " + l2_branch + ")")
                    params.extend(identity_ids)
                    params.extend(identity_ids)
                    params.extend(identity_ids)
                    params.extend(identity_ids)
                sql += " AND " + clause + " ORDER BY l4.saved_at DESC LIMIT ?"
                params.extend(clause_params)
                params.append(fetch)
                for row in cur.execute(sql, params).fetchall():
                    text = str(row["text"])
                    refs = (
                        (str(row["request_record_id"]),)
                        if row["request_record_id"]
                        else ()
                    )
                    is_archive = row["kind"] == "archive_summary"
                    is_l2 = row["kind"] == "l2_archive"
                    if is_archive:
                        refs = tuple(str(source["source_ref"]) for source in cur.execute(
                            "SELECT source_ref FROM l4_memory_source WHERE memory_id=? "
                            "AND source_type='l2_turn' ORDER BY position LIMIT 8",
                            (row["record_id"],)).fetchall())
                    elif is_l2:
                        refs = tuple(str(source["source_ref"]) for source in cur.execute(
                            "SELECT source_ref FROM l4_memory_source WHERE memory_id=? "
                            "AND source_type='l2_event' ORDER BY position LIMIT 8",
                            (row["record_id"],)).fetchall())
                    preview = (
                        "[自动归档摘要，非用户明确保存] "
                        if is_archive
                        else "[系统整理前情，非用户明确保存] " if is_l2
                        else ""
                    ) + text
                    hits.append(
                        RetrievalHit(
                            layer=LAYER_L4,
                            record_id=str(row["record_id"]),
                            occurred_at=str(row["occurred_at"]),
                            preview=preview[:preview_limit],
                            score=score(text, tokens),
                            source_refs=refs,
                            subject_label=row["subject_label"],
                        )
                    )

            if LAYER_L3 in wanted:
                # L3 人物事实：只回生效事实（撤回/替代/阻断的旧行不命中），
                # 跨群不共享；身份过滤用事实主体，未解析出身份时按会话范围查。
                clause, clause_params = like_clause("f.content", tokens)
                sql = (
                    "SELECT f.fact_id AS record_id, f.content AS text,"
                    " f.occurred_at, f.display_name AS subject_label,"
                    " f.source_record_ids, f.evidence_quote"
                    " FROM l3_fact f WHERE f.agent_id=? AND f.status=?"
                )
                params = [agent_id, FACT_ACTIVE]
                if conversation_id:
                    sql += " AND f.conversation_id=?"
                    params.append(conversation_id)
                if identity_ids:
                    marks = ",".join("?" for _ in identity_ids)
                    sql += " AND f.identity_id IN (" + marks + ")"
                    params.extend(identity_ids)
                sql += (
                    " AND " + clause
                    + " ORDER BY bj_epoch(f.updated_at) DESC, f.fact_id DESC LIMIT ?"
                )
                params.extend(clause_params)
                params.append(fetch)
                for row in cur.execute(sql, params).fetchall():
                    text = str(row["text"])
                    try:
                        refs = tuple(
                            str(ref)
                            for ref in json.loads(
                                str(row["source_record_ids"]) or "[]"
                            )[:3]
                        )
                    except (TypeError, ValueError):
                        refs = ()
                    hits.append(
                        RetrievalHit(
                            layer=LAYER_L3,
                            record_id=str(row["record_id"]),
                            occurred_at=str(row["occurred_at"] or ""),
                            preview=(
                                "[已归档事实｜系统整理，非用户明确保存] " + text
                            )[:preview_limit],
                            score=score(text, tokens),
                            source_refs=refs,
                            subject_label=row["subject_label"],
                        )
                    )

            if LAYER_JOURNAL in wanted:
                clause, clause_params = like_clause("j.summary", tokens)
                sql = (
                    "SELECT j.entry_id AS record_id, j.summary AS text,"
                    " j.occurred_at, j.source_record_ids_json FROM event_journal j"
                    " WHERE j.agent_id=?"
                )
                params = [agent_id]
                if conversation_id:
                    sql += " AND j.conversation_id=?"
                    params.append(conversation_id)
                for identity_id in identity_ids:
                    sql += " AND j.participants_json LIKE ?"
                    params.append(f'%"{identity_id}"%')
                sql += (
                    " AND " + clause
                    + " ORDER BY bj_epoch(j.occurred_at) DESC, j.entry_id DESC LIMIT ?"
                )
                params.extend(clause_params)
                params.append(fetch)
                for row in cur.execute(sql, params).fetchall():
                    text = str(row["text"])
                    try:
                        refs = tuple(
                            str(ref)
                            for ref in json.loads(
                                str(row["source_record_ids_json"]) or "[]"
                            )[:3]
                        )
                    except (ValueError, TypeError):
                        refs = ()
                    hits.append(
                        RetrievalHit(
                            layer=LAYER_JOURNAL,
                            record_id=str(row["record_id"]),
                            occurred_at=str(row["occurred_at"]),
                            preview=text[:preview_limit],
                            score=score(text, tokens),
                            source_refs=refs,
                        )
                    )

            if LAYER_RAW in wanted:
                clause, clause_params = like_clause("m.text", tokens)
                sql = (
                    "SELECT m.record_id, m.text, m.stored_at AS occurred_at"
                    " FROM raw_message m WHERE m.agent_id=?"
                )
                params = [agent_id]
                if conversation_id:
                    sql += " AND m.conversation_id=?"
                    params.append(conversation_id)
                if identity_ids:
                    sql += (
                        " AND m.sender_identity_id IN ("
                        + ",".join("?" for _ in identity_ids)
                        + ")"
                    )
                    params.extend(identity_ids)
                sql += (
                    " AND " + clause
                    + " ORDER BY bj_epoch(m.stored_at) DESC, m.record_id DESC LIMIT ?"
                )
                params.extend(clause_params)
                params.append(fetch)
                for row in cur.execute(sql, params).fetchall():
                    text = str(row["text"])
                    hits.append(
                        RetrievalHit(
                            layer=LAYER_RAW,
                            record_id=str(row["record_id"]),
                            occurred_at=str(row["occurred_at"]),
                            preview=text[:preview_limit],
                            score=score(text, tokens),
                        )
                    )

        hits.sort(key=lambda hit: (hit.score, hit.occurred_at, hit.record_id), reverse=True)
        kept = hits[:per_layer]
        if not kept:
            return [], f"关键词 {('、'.join(tokens))} 没有命中。"
        detail = f"命中 {len(kept)} 条（关键词：{'、'.join(tokens)}）"
        if len(hits) > len(kept):
            detail += f"，另有 {len(hits) - len(kept)} 条未展示"
        return kept, detail

    # ------------------------------------------------------------------ #
    # 身份
    # ------------------------------------------------------------------ #

    def find_identities(
        self,
        *,
        agent_id: str,
        alias: str,
        account_id: Optional[str],
        conversation_id: Optional[str],
        limit: int,
    ) -> Tuple[List[Dict[str, Any]], int]:
        """按别名查候选身份：按 Agent 可见来源 + 完整会话作用域过滤。

        返回 ``(候选, 总数)``：总数在 `LIMIT` 之前统计，因此 `limit=1` 时
        实际存在多个同名身份仍能被判定为歧义（不再用"返回条数"代表歧义）。

        - `conversation_id` 严格解析出 channel/account/type/target，**非法标识直接报错**；
        - 显式 `account_id` 与会话里的账号不一致 → 受控输入错误；
        - 只有 `conversation_id` 时也同时限制通道与账号；
        - 候选必须在该 Agent 的对应会话里真实出现过，并且返回的别名也遵守同一作用域；
        - **参与者覆盖整轮所有人**：可见性走
          `l2_turn(agent/会话) → l2_turn_source → raw_message.sender_identity_id → identity/alias`，
          不再只看轮次首位 `l2_turn.sender_identity_id`；同一身份被多条来源重复命中时
          由 `DISTINCT` 先去重再 `LIMIT` 分页，误合并仍由"同名不同身份 → 全部候选"兜住。
        """

        alias_text = (alias or "").strip()
        if not alias_text:
            raise MemoryQueryError("alias 不能为空")

        channel_id, resolved_account, scope_type, scope_ref = self._resolve_alias_scope(
            account_id=account_id, conversation_id=conversation_id
        )

        alias_scope_sql, alias_scope_params = self._alias_scope_sql(
            account_id=resolved_account,
            channel_id=channel_id,
            scope_type=scope_type,
            scope_ref=scope_ref,
        )

        params: List[Any] = [agent_id, resolved_account, alias_text]
        sql = (
            "SELECT DISTINCT i.identity_id, i.channel_id, i.account_id,"
            " i.bridge_user_id, i.wxid, i.wxid_source"
            " FROM l2_turn t"
            " JOIN l2_turn_source s ON s.turn_id = t.turn_id"
            " JOIN raw_message m ON m.record_id = s.record_id"
            " JOIN identity i ON i.identity_id = m.sender_identity_id"
            " JOIN identity_alias a ON a.identity_id = i.identity_id"
            " WHERE t.agent_id=? AND i.account_id=? AND a.alias = ? COLLATE NOCASE"
        )
        if channel_id:
            sql += " AND i.channel_id=?"
            params.append(channel_id)
        if conversation_id:
            sql += " AND t.conversation_id=?"
            params.append(conversation_id)
        sql += " AND (" + alias_scope_sql + ")"
        params.extend(alias_scope_params)

        with self._txn() as cur:
            total = int(
                cur.execute(
                    "SELECT COUNT(*) FROM (" + sql + ")", params
                ).fetchone()[0]
            )
            rows = cur.execute(
                sql + " ORDER BY i.identity_id LIMIT ?", params + [int(limit)]
            ).fetchall()
            items = [
                {
                    "identity_id": str(row["identity_id"]),
                    "channel_id": str(row["channel_id"]),
                    "account_id": str(row["account_id"]),
                    "bridge_user_id": str(row["bridge_user_id"]),
                    "wxid_known": bool(row["wxid"]),
                    "wxid_source": str(row["wxid_source"]),
                    "aliases": self._identity_aliases(
                        cur,
                        str(row["identity_id"]),
                        account_id=resolved_account,
                        channel_id=channel_id,
                        scope_type=scope_type,
                        scope_ref=scope_ref,
                    ),
                }
                for row in rows
            ]
            return items, total

    def _resolve_alias_scope(
        self, *, account_id: Optional[str], conversation_id: Optional[str]
    ) -> Tuple[Optional[str], str, str, str]:
        """解析别名查询的完整作用域；缺作用域/账号冲突/非法会话都明确拒绝。"""

        channel_id: Optional[str] = None
        scope_type = "account"
        scope_ref = ""
        if conversation_id:
            channel_id, cid_account, conversation_type, target_ref = parse_conversation_id(
                conversation_id
            )
            if account_id and account_id.strip() != cid_account:
                raise MemoryQueryError(
                    "account_id 与 conversation_id 的账号不一致（拒绝跨账号查询）"
                )
            account_id = cid_account
            if conversation_type == "group":
                scope_type, scope_ref = "group", target_ref
            else:
                scope_type, scope_ref = "account", cid_account
        resolved_account = (account_id or "").strip()
        if not resolved_account:
            raise MemoryQueryError(
                "需要 account_id 或 conversation_id 限定作用域，不做全库别名查询"
            )
        return channel_id, resolved_account, scope_type, scope_ref

    def match_alias_tokens(
        self,
        *,
        agent_id: str,
        account_id: Optional[str],
        conversation_id: Optional[str],
        tokens: Sequence[str],
        limit: int = 20,
    ) -> List[Dict[str, Any]]:
        """把查询里的 token 当别名做精确匹配，返回去重后的身份（用于检索前稳定身份）。

        只认"在该 Agent 会话里真实出现过的身份"，与 `find_identities` 同一可见性路径；
        返回条数受 `limit` 限制，但每条都带 `identity_id`，调用方据此判断是否歧义。
        """

        needles = [token.strip() for token in tokens if token.strip()]
        if not needles:
            return []
        channel_id, resolved_account, scope_type, scope_ref = self._resolve_alias_scope(
            account_id=account_id, conversation_id=conversation_id
        )
        alias_scope_sql, alias_scope_params = self._alias_scope_sql(
            account_id=resolved_account,
            channel_id=channel_id,
            scope_type=scope_type,
            scope_ref=scope_ref,
        )
        params: List[Any] = [agent_id, resolved_account]
        sql = (
            "SELECT DISTINCT i.identity_id, i.bridge_user_id, a.alias"
            " FROM l2_turn t"
            " JOIN l2_turn_source s ON s.turn_id = t.turn_id"
            " JOIN raw_message m ON m.record_id = s.record_id"
            " JOIN identity i ON i.identity_id = m.sender_identity_id"
            " JOIN identity_alias a ON a.identity_id = i.identity_id"
            " WHERE t.agent_id=? AND i.account_id=?"
        )
        if channel_id:
            sql += " AND i.channel_id=?"
            params.append(channel_id)
        if conversation_id:
            sql += " AND t.conversation_id=?"
            params.append(conversation_id)
        sql += (
            " AND a.alias COLLATE NOCASE IN ("
            + ",".join("?" for _ in needles)
            + ")"
        )
        params.extend(needles)
        sql += " AND (" + alias_scope_sql + ")"
        params.extend(alias_scope_params)
        sql += " ORDER BY i.identity_id LIMIT ?"
        params.append(int(limit))
        with self._txn() as cur:
            return [
                {
                    "identity_id": str(row["identity_id"]),
                    "bridge_user_id": str(row["bridge_user_id"]),
                    "alias": str(row["alias"]),
                }
                for row in cur.execute(sql, params).fetchall()
            ]

    @staticmethod
    def _scope_from_conversation(
        conversation_type: str, target_ref: str, account_id: str
    ) -> Tuple[str, str]:
        """群聊按 `group/target` 作用域，私聊按 `account/account`（与写入一致）。"""

        if conversation_type == "group":
            return "group", target_ref
        return "account", account_id

    def _alias_scope_sql(
        self,
        *,
        account_id: str,
        channel_id: Optional[str],
        scope_type: str,
        scope_ref: str,
    ) -> Tuple[str, List[Any]]:
        """别名可见性：账号级别名 + 本作用域内的群别名（其它群/账号不可见）。"""

        params: List[Any] = [account_id]
        sql = "(a.scope_type='account' AND a.scope_ref=?)"
        if scope_type == "group" and scope_ref:
            sql += " OR (a.scope_type='group' AND a.scope_ref=?)"
            params.append(scope_ref)
        else:
            # 账号作用域：只放行该账号下已出现过的群，不跨账号泄露别名。
            sql += (
                " OR (a.scope_type='group' AND EXISTS ("
                "  SELECT 1 FROM conversation c"
                "  WHERE c.conversation_type='group' AND c.target_ref=a.scope_ref"
                "    AND c.account_id=?"
            )
            params.append(account_id)
            if channel_id:
                sql += " AND c.channel_id=?"
                params.append(channel_id)
            sql += "))"
        return sql, params

    def _identity_aliases(
        self,
        cur: sqlite3.Cursor,
        identity_id: Optional[str],
        *,
        account_id: str,
        channel_id: Optional[str],
        scope_type: str,
        scope_ref: str,
        limit: int = 8,
    ) -> List[Dict[str, Any]]:
        if not identity_id:
            return []
        scope_sql, scope_params = self._alias_scope_sql(
            account_id=account_id,
            channel_id=channel_id,
            scope_type=scope_type,
            scope_ref=scope_ref,
        )
        rows = cur.execute(
            "SELECT alias, alias_type, scope_type, scope_ref, source, observed_at"
            " FROM identity_alias a WHERE a.identity_id=?"
            " AND (" + scope_sql + ")"
            " ORDER BY a.observed_at DESC, a.alias LIMIT ?",
            (identity_id, *scope_params, int(limit)),
        ).fetchall()
        return [
            {
                "alias": str(row["alias"]),
                "alias_type": str(row["alias_type"]),
                "scope_type": str(row["scope_type"]),
                "scope_ref": str(row["scope_ref"]),
                "source": str(row["source"]),
                "observed_at": str(row["observed_at"]),
            }
            for row in rows
        ]

    # ------------------------------------------------------------------ #
    # 行 → 摘要
    # ------------------------------------------------------------------ #

    def _raw_item(self, row: sqlite3.Row) -> Dict[str, Any]:
        return {
            "record_id": str(row["record_id"]),
            "layer": LAYER_RAW,
            "conversation_id": str(row["conversation_id"]),
            "created_at": str(row["created_at"]),
            "status": "stored",
            "text_chars": int(row["chars"]),
            "preview": str(row["preview"]),
            "preview_truncated": int(row["text_length"]) > len(str(row["preview"])),
            "identity_id": row["identity_id"],
            "source_event_id": row["source_event_id"],
            "tool_name": None,
            "call_id": None,
        }

    def _turn_item(self, row: sqlite3.Row) -> Dict[str, Any]:
        return {
            "record_id": str(row["turn_id"]),
            "layer": LAYER_L2,
            "conversation_id": str(row["conversation_id"]),
            "created_at": str(row["created_at"]),
            "status": str(row["status"]),
            "text_chars": int(row["user_chars"]),
            "preview": str(row["preview"]),
            "preview_truncated": int(row["text_length"]) > len(str(row["preview"])),
            "identity_id": row["identity_id"],
            "source_event_id": None,
            "tool_name": None,
            "call_id": None,
            "has_assistant": bool(row["has_assistant"]),
        }

    def _tool_item(self, row: sqlite3.Row) -> Dict[str, Any]:
        return {
            "record_id": str(row["evidence_id"]),
            "layer": LAYER_L20,
            "conversation_id": row["conversation_id"],
            "created_at": str(row["created_at"]),
            "status": str(row["status"]),
            "text_chars": int(row["text_length"]),
            "preview": str(row["preview"]),
            "preview_truncated": int(row["text_length"]) > len(str(row["preview"])),
            "identity_id": None,
            "source_event_id": None,
            "tool_name": str(row["tool_name"]),
            "call_id": str(row["call_id"]),
            "error_code": row["error_code"],
            "turn_id": str(row["turn_id"]),
        }

    def _l4_item(self, row: sqlite3.Row) -> Dict[str, Any]:
        return {
            "record_id": str(row["record_id"]),
            "layer": LAYER_L4,
            "kind": str(row["kind"]),
            "conversation_id": str(row["conversation_id"]),
            "created_at": str(row["created_at"]),
            "status": str(row["status"]),
            "text_chars": int(row["text_chars"]),
            "preview": str(row["preview"]),
            "preview_truncated": int(row["text_length"]) > len(str(row["preview"])),
            "identity_id": row["identity_id"],
            "source_event_id": None,
            "tool_name": None,
            "call_id": None,
        }

    def _journal_item(self, row: sqlite3.Row) -> Dict[str, Any]:
        return {
            "record_id": str(row["record_id"]),
            "layer": LAYER_JOURNAL,
            "conversation_id": str(row["conversation_id"]),
            "created_at": str(row["created_at"]),
            "status": str(row["status"]),
            "text_chars": int(row["text_chars"]),
            "preview": str(row["preview"]),
            "preview_truncated": int(row["text_length"]) > len(str(row["preview"])),
            "identity_id": None,
            "source_event_id": None,
            "tool_name": None,
            "call_id": None,
            "turn_id": str(row["turn_id"]),
            "has_assistant": bool(row["has_assistant"]),
        }


__all__ = ["MemoryStoreError", "MemoryQueryError", "SqliteMemoryStore"]
