"""规范聊天流水（v6）：一条真实群消息 = 一条记录。

与旧表的关系（执行说明）：
- `raw_message` 仍是**入站原文**的幂等存储（begin_turn 写入），保留不动；
- `l2_turn` / `l20_tool_evidence` 仍是**轮次与工具证据**，不再是聊天窗口单位；
- 本模块的 `chat_event` 是规范化视图：群友消息（含未唤醒/静默）与**已确认发送**
  的机器人回复各占一行，带会话内单调 `seq`，供 L1/L10/L2 三个窗口共用。

不存：工具正文、文件/网页全文、Base64、凭据、模型思考。
"""

from __future__ import annotations

import hashlib
import json
import time
import uuid
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

#: 聊天事件角色。
ROLE_MEMBER = "member"
ROLE_ASSISTANT = "assistant"

#: 事件类型（桥已知类型；不由扩展名猜测）。
KIND_TEXT = "text"
KIND_IMAGE = "image"
KIND_STICKER = "sticker"
KIND_FILE = "file"
KIND_CALL = "call"
KIND_SYSTEM = "system"

VALID_ROLES = frozenset({ROLE_MEMBER, ROLE_ASSISTANT})
VALID_KINDS = frozenset(
    {KIND_TEXT, KIND_IMAGE, KIND_STICKER, KIND_FILE, KIND_CALL, KIND_SYSTEM}
)

#: 归档任务状态。
ARCHIVE_QUEUED = "queued"
ARCHIVE_RUNNING = "running"
ARCHIVE_COMPLETED = "completed"
ARCHIVE_FAILED = "failed"
ARCHIVE_INTERRUPTED = "interrupted"


#: 群聊时间统一按用户时区（Asia/Shanghai）存为不带偏移的 ISO；
#: 检索边界同用该时区，避免宿主机时区（可能 UTC）盲猜。
USER_TZ = "Asia/Shanghai"


def _user_tz():
    """Asia/Shanghai 时区；Windows 无 tzdata 时回退固定 UTC+8（中国无夏令时）。"""

    try:
        from zoneinfo import ZoneInfo

        return ZoneInfo(USER_TZ)
    except Exception:  # noqa: BLE001 - 缺少 tzdata 时不能让流水写入崩溃
        from datetime import timedelta, timezone

        return timezone(timedelta(hours=8), USER_TZ)


def _now_iso() -> str:
    from datetime import datetime

    return datetime.now(_user_tz()).replace(tzinfo=None).isoformat(timespec="seconds")


def _normalize_occurred(value: Any) -> str:
    """把入站时间规范为 Asia/Shanghai naïve ISO；无法解析时保留前 19 字符。"""

    raw = str(value or "").strip()
    if not raw:
        return _now_iso()
    try:
        from datetime import datetime

        stamp = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if stamp.tzinfo is not None:
            stamp = stamp.astimezone(_user_tz()).replace(tzinfo=None)
        return stamp.isoformat(timespec="seconds")
    except Exception:  # noqa: BLE001
        return raw[:19]


def _stale(updated: str, seconds: float) -> bool:
    """判断时间戳是否过期；无法解析时按过期处理（允许恢复，不永久卡住）。"""

    try:
        from datetime import datetime

        stamp = datetime.fromisoformat(str(updated))
        return (datetime.now(stamp.tzinfo) - stamp).total_seconds() > float(seconds)
    except Exception:  # noqa: BLE001
        return True


def _clean_media(items: Iterable[Any]) -> List[Dict[str, Any]]:
    """媒体引用只保留受控元数据（不含 Base64/路径）。"""

    result: List[Dict[str, Any]] = []
    for item in list(items or ())[:20]:
        if isinstance(item, dict):
            entry = {
                "kind": str(item.get("kind") or "")[:20],
                "name": str(item.get("name") or "")[:120],
                "handle": str(item.get("handle") or item.get("resource_id") or "")[:120],
                "size": int(item.get("size") or 0),
                "sha256": str(item.get("sha256") or "")[:64],
            }
            if item.get("asset_id"):
                entry["asset_id"] = str(item.get("asset_id"))[:80]
            if item.get("state"):
                entry["state"] = str(item.get("state"))[:20]
            result.append(entry)
        elif isinstance(item, str) and item.strip():
            result.append({"kind": "text", "name": item.strip()[:120]})
    return result


class ChatStreamMixin:
    """聊天流水、窗口、游标、归档批次与事件条目的同步实现（由 store 的 _txn 串行）。"""

    # ------------------------------------------------------------------ #
    # 写入
    # ------------------------------------------------------------------ #

    def chat_append_member(self, *, agent_id: str, conversation_id: str,
                           records: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """写入群友消息（幂等）。返回每条的实际状态（inserted/existing）。"""

        return self._chat_append(
            agent_id=agent_id, conversation_id=conversation_id,
            role=ROLE_MEMBER, records=records,
        )

    def chat_append_assistant(self, *, agent_id: str, conversation_id: str,
                              turn_id: str, action_id: str, text: str,
                              occurred_at: str, sender_ref: str = "",
                              display_name: str = "", media: Sequence[Any] = (),
                              dedupe_key: str = "") -> Dict[str, Any]:
        """写入机器人**已确认发送**的一条发言（未确认不写）。"""

        dedupe = dedupe_key or f"assistant:{turn_id}"
        records = [{
            "text": text,
            "occurred_at": occurred_at or _now_iso(),
            "sender_ref": sender_ref or "assistant",
            "display_name": display_name or "",
            "kind": KIND_TEXT,
            "turn_id": turn_id,
            "delivery_action_id": action_id,
            "media": list(media or ()),
            "dedupe_key": dedupe,
            "source_event_id": "",
            "identity_id": None,
            "quote_event_id": "",
        }]
        return self._chat_append(
            agent_id=agent_id, conversation_id=conversation_id,
            role=ROLE_ASSISTANT, records=records,
        )[0]

    def _chat_append(self, *, agent_id: str, conversation_id: str, role: str,
                     records: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
        if role not in VALID_ROLES:
            raise ValueError("invalid_chat_role")
        if not agent_id or not conversation_id:
            raise ValueError("invalid_chat_scope")
        results: List[Dict[str, Any]] = []
        with self._txn() as cur:
            next_seq = int(cur.execute(
                "SELECT COALESCE(MAX(seq),0) FROM chat_event "
                "WHERE agent_id=? AND conversation_id=?",
                (agent_id, conversation_id),
            ).fetchone()[0])
            for record in list(records)[:200]:
                text = str(record.get("text") or "")
                kind = str(record.get("kind") or KIND_TEXT)
                if kind not in VALID_KINDS:
                    kind = KIND_TEXT
                if not text.strip():
                    if kind == KIND_TEXT:
                        continue
                    # 纯媒体也占一条：用已知类型占位，不猜内容。
                    text = {
                        KIND_IMAGE: "[图片]",
                        KIND_STICKER: "[表情]",
                        KIND_FILE: "[文件]",
                        KIND_CALL: "[通话]",
                        KIND_SYSTEM: "[系统消息]",
                    }.get(kind, "[消息]")
                dedupe = str(record.get("dedupe_key") or "").strip()
                if not dedupe:
                    source = str(record.get("source_event_id") or "")
                    dedupe = f"{role}:{source or hashlib.sha256(text.encode()).hexdigest()[:24]}"
                existing = cur.execute(
                    "SELECT event_id, seq FROM chat_event WHERE agent_id=? AND dedupe_key=?",
                    (agent_id, dedupe),
                ).fetchone()
                if existing is not None:
                    results.append({"state": "existing", "event_id": existing[0],
                                    "seq": int(existing[1])})
                    continue
                next_seq += 1
                event_id = "ce-" + uuid.uuid4().hex
                now = _now_iso()
                cur.execute(
                    "INSERT INTO chat_event("
                    "event_id,agent_id,conversation_id,seq,role,identity_id,sender_ref,"
                    "display_name,kind,text,text_chars,occurred_at,stored_at,source_event_id,"
                    "source_record_id,turn_id,delivery_action_id,quote_event_id,media_json,dedupe_key"
                    ") VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        event_id, agent_id, conversation_id, next_seq, role,
                        record.get("identity_id"), str(record.get("sender_ref") or "")[:120],
                        str(record.get("display_name") or "")[:120], kind, text, len(text),
                        _normalize_occurred(record.get("occurred_at")), now,
                        str(record.get("source_event_id") or "")[:250],
                        record.get("source_record_id"),
                        record.get("turn_id"), record.get("delivery_action_id"),
                        str(record.get("quote_event_id") or "")[:250],
                        json.dumps(_clean_media(record.get("media") or ()), ensure_ascii=False),
                        dedupe[:250],
                    ),
                )
                results.append({"state": "inserted", "event_id": event_id, "seq": next_seq})
        return results

    # ------------------------------------------------------------------ #
    # 窗口查询（同一份流水）
    # ------------------------------------------------------------------ #

    def chat_window(self, *, agent_id: str, conversation_id: str, limit: int,
                    before_seq: Optional[int] = None,
                    exclude_source_event_ids: Sequence[str] = (),
                    include_tools: bool = False) -> List[Dict[str, Any]]:
        """取最后 limit 条（正序返回）；可按 seq 截止或排除当前触发消息。"""

        limit = max(1, min(int(limit), 200))
        clauses = ["agent_id=?", "conversation_id=?"]
        params: List[Any] = [agent_id, conversation_id]
        if before_seq is not None:
            clauses.append("seq<=?")
            params.append(int(before_seq))
        excluded = [str(item)[:250] for item in (exclude_source_event_ids or ()) if str(item)]
        if excluded:
            placeholders = ",".join("?" * len(excluded))
            clauses.append(f"(source_event_id='' OR source_event_id NOT IN ({placeholders}))")
            params.extend(excluded)
        sql = (
            "SELECT * FROM chat_event WHERE " + " AND ".join(clauses) +
            " ORDER BY seq DESC LIMIT ?"
        )
        params.append(limit)
        with self._txn() as cur:
            rows = cur.execute(sql, params).fetchall()
            items = [self._chat_row(row) for row in reversed(rows)]
            if include_tools:
                self._attach_tool_evidence(cur, agent_id, conversation_id, items)
        return items

    def chat_snapshot(self, *, agent_id: str, conversation_id: str, limit: int,
                      exclude_source_event_ids: Sequence[str] = ()) -> Dict[str, Any]:
        """**单事务**取窗口与截止/计数：避免"先 window 再 counts"边界漂移。"""

        limit = max(1, min(int(limit), 200))
        clauses = ["agent_id=?", "conversation_id=?"]
        params: List[Any] = [agent_id, conversation_id]
        excluded = [str(item)[:250] for item in (exclude_source_event_ids or ()) if str(item)]
        if excluded:
            placeholders = ",".join("?" * len(excluded))
            clauses.append(f"(source_event_id='' OR source_event_id NOT IN ({placeholders}))")
            params.extend(excluded)
        with self._txn() as cur:
            rows = cur.execute(
                "SELECT * FROM chat_event WHERE " + " AND ".join(clauses) +
                " ORDER BY seq DESC LIMIT ?",
                [*params, limit],
            ).fetchall()
            items = [self._chat_row(row) for row in reversed(rows)]
            counts = cur.execute(
                "SELECT COUNT(*), COALESCE(MAX(seq),0), "
                "SUM(CASE WHEN role='assistant' THEN 1 ELSE 0 END) "
                "FROM chat_event WHERE agent_id=? AND conversation_id=?",
                (agent_id, conversation_id),
            ).fetchone()
        return {
            "items": items,
            "last_seq": int(counts[1] or 0),
            "counts": {
                "messages": int(counts[0] or 0),
                "last_seq": int(counts[1] or 0),
                "assistant_messages": int(counts[2] or 0),
            },
            "snapshot_seq": int(items[-1]["seq"]) if items else 0,
        }

    def _attach_tool_evidence(self, cur, agent_id: str, conversation_id: str,
                              items: List[Dict[str, Any]]) -> None:
        turn_ids = [
            str(item.get("turn_id")) for item in items
            if item.get("role") == "assistant" and item.get("turn_id")
        ][:50]
        if not turn_ids:
            return
        placeholders = ",".join("?" * len(turn_ids))
        rows = cur.execute(
            "SELECT e.turn_id,e.tool_name,e.status,e.duration_ms,e.summary,e.error_code,e.created_at,e.started_at,e.finished_at "
            "FROM l20_tool_evidence e JOIN l2_turn t ON t.turn_id=e.turn_id AND t.agent_id=e.agent_id "
            "WHERE e.agent_id=? AND t.conversation_id=? "
            f"AND e.turn_id IN ({placeholders}) ORDER BY e.created_at ASC,e.evidence_id ASC",
            (agent_id, conversation_id, *turn_ids),
        ).fetchall()
        grouped: Dict[str, List[Dict[str, Any]]] = {}
        for row in rows:
            grouped.setdefault(str(row[0]), []).append({
                "tool_name": row[1], "status": row[2], "duration_ms": row[3],
                "summary": row[4], "error_code": row[5], "created_at": row[6],
                "started_at": row[7], "finished_at": row[8],
            })
        for item in items:
            if item.get("role") == "assistant" and item.get("turn_id"):
                item["tool_evidence"] = grouped.get(str(item["turn_id"]), [])

    def chat_history(self, *, agent_id: str, conversation_id: str,
                     limit: int = 20, offset: int = 0,
                     before_seq: Optional[int] = None) -> Dict[str, Any]:
        """聊天历史分页（倒序返回）。`before_seq` 固定边界，避免与最新窗口重复。"""

        limit = max(1, min(int(limit), 100))
        offset = max(0, int(offset))
        clauses = ["agent_id=?", "conversation_id=?"]
        params: List[Any] = [agent_id, conversation_id]
        if before_seq is not None and int(before_seq) > 0:
            clauses.append("seq<?")
            params.append(int(before_seq))
        where = " AND ".join(clauses)
        with self._txn() as cur:
            rows = cur.execute(
                "SELECT * FROM chat_event WHERE " + where +
                " ORDER BY seq DESC LIMIT ? OFFSET ?",
                [*params, limit, offset],
            ).fetchall()
            total = int(cur.execute(
                "SELECT COUNT(*) FROM chat_event WHERE " + where, params
            ).fetchone()[0])
        return {"items": [self._chat_row(row) for row in rows], "total": total,
                "limit": limit, "offset": offset,
                "before_seq": int(before_seq) if before_seq else None}

    def chat_counts(self, *, agent_id: str, conversation_id: str) -> Dict[str, int]:
        with self._txn() as cur:
            row = cur.execute(
                "SELECT COUNT(*), COALESCE(MAX(seq),0), "
                "SUM(CASE WHEN role='assistant' THEN 1 ELSE 0 END) "
                "FROM chat_event WHERE agent_id=? AND conversation_id=?",
                (agent_id, conversation_id),
            ).fetchone()
        return {"messages": int(row[0] or 0), "last_seq": int(row[1] or 0),
                "assistant_messages": int(row[2] or 0)}

    @staticmethod
    def _chat_row(row) -> Dict[str, Any]:
        keys = set(row.keys()) if hasattr(row, "keys") else set()
        item = {key: row[key] for key in keys}
        try:
            item["media"] = json.loads(item.pop("media_json", "[]") or "[]")
        except ValueError:
            item["media"] = []
        return item

    # ------------------------------------------------------------------ #
    # 游标（归档 / L3 候选 / 事件提取）
    # ------------------------------------------------------------------ #

    def chat_cursor_get(self, *, agent_id: str, conversation_id: str, kind: str) -> int:
        with self._txn() as cur:
            row = cur.execute(
                "SELECT last_seq FROM chat_cursor WHERE agent_id=? AND conversation_id=? AND kind=?",
                (agent_id, conversation_id, kind),
            ).fetchone()
        return int(row[0]) if row is not None else 0

    def chat_cursor_set(self, *, agent_id: str, conversation_id: str, kind: str,
                        last_seq: int) -> None:
        with self._txn() as cur:
            cur.execute(
                "INSERT INTO chat_cursor(agent_id,conversation_id,kind,last_seq,updated_at) "
                "VALUES(?,?,?,?,?) ON CONFLICT(agent_id,conversation_id,kind) "
                "DO UPDATE SET last_seq=excluded.last_seq,updated_at=excluded.updated_at",
                (agent_id, conversation_id, kind, int(last_seq), _now_iso()),
            )

    # ------------------------------------------------------------------ #
    # 归档批次（100 条触发，整理较早 60 条，保留最新 40 条）
    # ------------------------------------------------------------------ #

    def chat_archive_plan(self, *, agent_id: str, conversation_id: str,
                          trigger_messages: int, batch_messages: int,
                          window_messages: int) -> Optional[Dict[str, Any]]:
        """返回可整理的区间（from_seq/to_seq/messages），不足触发条件返回 None。"""

        trigger = max(1, int(trigger_messages))
        batch = max(1, int(batch_messages))
        window = max(1, int(window_messages))
        with self._txn() as cur:
            last_seq = int(cur.execute(
                "SELECT COALESCE(MAX(seq),0) FROM chat_event "
                "WHERE agent_id=? AND conversation_id=?",
                (agent_id, conversation_id),
            ).fetchone()[0])
            cursor_row = cur.execute(
                "SELECT last_seq FROM chat_cursor WHERE agent_id=? AND conversation_id=? AND kind='archive'",
                (agent_id, conversation_id),
            ).fetchone()
            covered = int(cursor_row[0]) if cursor_row is not None else 0
            pending = last_seq - covered
            if pending < trigger:
                return None
            # 只整理“较早”的 batch 条：截止到 last_seq - window。
            upper = last_seq - window
            if upper <= covered:
                return None
            rows = cur.execute(
                "SELECT event_id,seq,role,display_name,sender_ref,kind,text,occurred_at "
                "FROM chat_event WHERE agent_id=? AND conversation_id=? AND seq>? AND seq<=? "
                "ORDER BY seq ASC LIMIT ?",
                (agent_id, conversation_id, covered, upper, batch),
            ).fetchall()
            if not rows:
                return None
            items = [dict(row) if hasattr(row, "keys") else {
                "event_id": row[0], "seq": row[1], "role": row[2],
                "display_name": row[3], "sender_ref": row[4], "kind": row[5],
                "text": row[6], "occurred_at": row[7],
            } for row in rows]
            existing = cur.execute(
                "SELECT archive_id,state,attempts,updated_at FROM chat_archive "
                "WHERE agent_id=? AND conversation_id=? AND from_seq=? AND to_seq=?",
                (agent_id, conversation_id, items[0]["seq"], items[-1]["seq"]),
            ).fetchone()
            now = _now_iso()
            if existing is not None:
                archive_id = str(existing[0])
                state = str(existing[1])
                attempts = int(existing[2] or 0)
                updated = str(existing[3] or "")
                # 启动恢复：失败/中断可重试一次；遗留 running 超时视为中断。
                if (
                    (state in {ARCHIVE_FAILED, ARCHIVE_INTERRUPTED} and attempts < 2)
                    or (state == ARCHIVE_RUNNING and _stale(updated, 120))
                ):
                    cur.execute(
                        "UPDATE chat_archive SET state='queued',error_code='',"
                        "attempts=attempts+1,updated_at=? WHERE archive_id=?",
                        (now, archive_id),
                    )
                    return {"archive_id": archive_id, "state": ARCHIVE_QUEUED,
                            "from_seq": items[0]["seq"], "to_seq": items[-1]["seq"],
                            "items": items, "existing": False, "resumed": True}
                return {"archive_id": archive_id, "state": state,
                        "from_seq": items[0]["seq"], "to_seq": items[-1]["seq"],
                        "items": items, "existing": True}
            archive_id = "ca-" + uuid.uuid4().hex
            cur.execute(
                "INSERT INTO chat_archive(archive_id,agent_id,conversation_id,from_seq,to_seq,"
                "message_count,state,error_code,memory_id,summary_chars,created_at,updated_at,attempts) "
                "VALUES(?,?,?,?,?,?,'queued','',NULL,0,?,?,0)",
                (archive_id, agent_id, conversation_id, items[0]["seq"], items[-1]["seq"],
                 len(items), now, now),
            )
        return {"archive_id": archive_id, "state": ARCHIVE_QUEUED,
                "from_seq": items[0]["seq"], "to_seq": items[-1]["seq"],
                "items": items, "existing": False}

    def chat_archive_claim(self, *, archive_id: str, agent_id: str) -> bool:
        with self._txn() as cur:
            cursor = cur.execute(
                "UPDATE chat_archive SET state='running',updated_at=? "
                "WHERE archive_id=? AND agent_id=? AND state='queued'",
                (_now_iso(), archive_id, agent_id),
            )
            return cursor.rowcount == 1

    def chat_archive_settle(self, *, archive_id: str, agent_id: str, state: str,
                            error_code: str = "", memory_id: str = "",
                            summary_chars: int = 0) -> bool:
        with self._txn() as cur:
            row = cur.execute(
                "SELECT state FROM chat_archive WHERE archive_id=? AND agent_id=?",
                (archive_id, agent_id),
            ).fetchone()
            if row is None or row[0] not in {ARCHIVE_RUNNING, ARCHIVE_QUEUED}:
                return False
            cur.execute(
                "UPDATE chat_archive SET state=?,error_code=?,memory_id=?,summary_chars=?,updated_at=? "
                "WHERE archive_id=? AND agent_id=?",
                (state, str(error_code or "")[:80], memory_id or None,
                 int(summary_chars or 0), _now_iso(), archive_id, agent_id),
            )
            return True

    def chat_archive_advance(self, *, agent_id: str, conversation_id: str,
                             to_seq: int) -> None:
        """只有摘要事务成功后才推进游标；失败/中断不推进。"""

        with self._txn() as cur:
            cur.execute(
                "INSERT INTO chat_cursor(agent_id,conversation_id,kind,last_seq,updated_at) "
                "VALUES(?,?,'archive',?,?) ON CONFLICT(agent_id,conversation_id,kind) "
                "DO UPDATE SET last_seq=MAX(last_seq,excluded.last_seq),updated_at=excluded.updated_at",
                (agent_id, conversation_id, int(to_seq), _now_iso()),
            )

    def chat_after(self, *, agent_id: str, conversation_id: str,
                   after_seq: int, limit: int) -> List[Dict[str, Any]]:
        """按 seq **正序**取游标之后的连续区间（后台增量处理用，不取最新倒序）。"""

        with self._txn() as cur:
            rows = cur.execute(
                "SELECT * FROM chat_event WHERE agent_id=? AND conversation_id=? AND seq>? "
                "ORDER BY seq ASC LIMIT ?",
                (agent_id, conversation_id, int(after_seq), max(1, min(int(limit), 200))),
            ).fetchall()
        return [self._chat_row(row) for row in rows]

    def chat_link_record(self, *, agent_id: str, conversation_id: str,
                         event_id: str, record_id: str) -> bool:
        """begin_turn 成功后回填 record_id（流水先写、不因轮次失败漏采）。"""

        if not event_id or not record_id:
            return False
        with self._txn() as cur:
            cursor = cur.execute(
                "UPDATE chat_event SET source_record_id=? WHERE agent_id=? AND dedupe_key=?",
                (record_id, agent_id, f"member:{event_id}"),
            )
            return cursor.rowcount == 1

    def chat_archive_commit(
        self, *, archive_id: str, agent_id: str, conversation_id: str,
        to_seq: int, memory_id: str, summary_chars: int,
        channel_id: str, account_id: str, content: str,
        source_seqs: Sequence[int], occurred_at: str = "",
        message_count: int = 0,
    ) -> bool:
        """**单一事务**完成：摘要写入 + 批次 completed + 游标推进。

        任一步失败整体回滚：不会出现"有摘要但游标未推进"或"游标推进但无摘要"。
        """

        text = (content or "").strip()
        if not text or not memory_id:
            return False
        dedupe = "chat:" + hashlib.sha256(
            f"chat_archive\x1f{conversation_id}\x1f{text}".encode()
        ).hexdigest()[:32]
        now = _now_iso()
        with self._txn() as cur:
            row = cur.execute(
                "SELECT state,to_seq FROM chat_archive WHERE archive_id=? AND agent_id=?",
                (archive_id, agent_id),
            ).fetchone()
            if row is None or str(row[0]) != ARCHIVE_RUNNING:
                return False
            # 允许按字符预算收缩区间：只校验不越过原计划上限。
            if int(row[1]) < int(to_seq):
                return False
            existing = cur.execute(
                "SELECT memory_id FROM l4_memory WHERE agent_id=? AND dedupe_key=?",
                (agent_id, dedupe),
            ).fetchone()
            if existing is not None:
                saved_id = str(existing[0])
            else:
                saved_id = memory_id
                cur.execute(
                    "INSERT INTO l4_memory(memory_id,agent_id,conversation_id,channel_id,account_id,"
                    "kind,status,content,content_chars,subject_identity_id,subject_label,request_text,"
                    "request_record_id,turn_id,dedupe_key,occurred_at,saved_at,updated_at) "
                    "VALUES(?,?,?,?,?,'chat_archive','saved',?,?,NULL,NULL,?,NULL,NULL,?,?,?,?)",
                    (saved_id, agent_id, conversation_id, channel_id[:80], account_id[:80],
                     text, len(text), text[:400], dedupe,
                     occurred_at or now, now, now),
                )
                for seq in list(source_seqs)[:200]:
                    cur.execute(
                        "INSERT OR IGNORE INTO l4_memory_source(memory_id,source_type,source_ref,position) "
                        "VALUES(?,?,?,?)",
                        (saved_id, "chat_seq", str(int(seq)), int(seq)),
                    )
            cur.execute(
                "UPDATE chat_archive SET state='completed',error_code='',memory_id=?,"
                "summary_chars=?,to_seq=?,message_count=?,updated_at=? "
                "WHERE archive_id=? AND agent_id=?",
                (saved_id, len(text), int(to_seq),
                 int(message_count) or len(list(source_seqs)), now,
                 archive_id, agent_id),
            )
            cur.execute(
                "INSERT INTO chat_cursor(agent_id,conversation_id,kind,last_seq,updated_at) "
                "VALUES(?,?,'archive',?,?) ON CONFLICT(agent_id,conversation_id,kind) "
                "DO UPDATE SET last_seq=MAX(last_seq,excluded.last_seq),updated_at=excluded.updated_at",
                (agent_id, conversation_id, int(to_seq), now),
            )
        return True

    # ------------------------------------------------------------------ #
    # 后台任务状态（失败次数/退避/手动重试；重启后据此恢复）
    # ------------------------------------------------------------------ #

    _TASK_DEFAULT_MAX = 5

    def chat_task_begin(self, *, agent_id: str, conversation_id: str,
                        kind: str, cursor_seq: int) -> Dict[str, Any]:
        with self._txn() as cur:
            row = cur.execute(
                "SELECT attempts,max_attempts,manual_required,next_allowed_at,error_code "
                "FROM chat_task WHERE agent_id=? AND conversation_id=? AND kind=?",
                (agent_id, conversation_id, kind),
            ).fetchone()
            now = _now_iso()
            if row is not None and (bool(row[2]) or (str(row[3] or '') and str(row[3])>now)):
                return {'attempts':int(row[0]),'max_attempts':int(row[1]),'manual_required':bool(row[2]),'next_allowed_at':str(row[3] or ''),'error_code':str(row[4] or '')}
            if row is None:
                cur.execute(
                    "INSERT INTO chat_task(task_id,agent_id,conversation_id,kind,cursor_seq,"
                    "attempts,max_attempts,state,error_code,next_allowed_at,manual_required,updated_at) "
                    "VALUES(?,?,?,?,?,0,?,'running','',NULL,0,?)",
                    ("ct-" + uuid.uuid4().hex, agent_id, conversation_id, kind,
                     int(cursor_seq), self._TASK_DEFAULT_MAX, now),
                )
                return {"attempts": 0, "max_attempts": self._TASK_DEFAULT_MAX,
                        "manual_required": False, "next_allowed_at": "",
                        "error_code": ""}
            cur.execute(
                "UPDATE chat_task SET state='running',cursor_seq=?,updated_at=? "
                "WHERE agent_id=? AND conversation_id=? AND kind=?",
                (int(cursor_seq), now, agent_id, conversation_id, kind),
            )
            return {
                "attempts": int(row[0] or 0), "max_attempts": int(row[1] or self._TASK_DEFAULT_MAX),
                "manual_required": bool(row[2]), "next_allowed_at": str(row[3] or ""),
                "error_code": str(row[4] or ""),
            }

    def chat_task_fail(self, *, agent_id: str, conversation_id: str, kind: str,
                       cursor_seq: int, error_code: str, retry_seconds: float) -> Dict[str, Any]:
        from datetime import datetime, timedelta

        now = _now_iso()
        next_at = (datetime.now(_user_tz()) + timedelta(
            seconds=max(1.0, float(retry_seconds)))).replace(tzinfo=None).isoformat(timespec="seconds")
        with self._txn() as cur:
            row = cur.execute(
                "SELECT attempts,max_attempts FROM chat_task WHERE agent_id=? AND conversation_id=? AND kind=?",
                (agent_id, conversation_id, kind),
            ).fetchone()
            attempts = int(row[0] or 0) + 1 if row else 1
            max_attempts = int(row[1] or self._TASK_DEFAULT_MAX) if row else self._TASK_DEFAULT_MAX
            manual = 1 if attempts >= max_attempts else 0
            state = "blocked" if manual else "failed"
            if row is None:
                cur.execute(
                    "INSERT INTO chat_task(task_id,agent_id,conversation_id,kind,cursor_seq,"
                    "attempts,max_attempts,state,error_code,next_allowed_at,manual_required,updated_at) "
                    "VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                    ("ct-" + uuid.uuid4().hex, agent_id, conversation_id, kind,
                     int(cursor_seq), attempts, max_attempts, state,
                     str(error_code)[:80], next_at, manual, now),
                )
            else:
                cur.execute(
                    "UPDATE chat_task SET cursor_seq=?,attempts=?,state=?,error_code=?,"
                    "next_allowed_at=?,manual_required=?,updated_at=? "
                    "WHERE agent_id=? AND conversation_id=? AND kind=?",
                    (int(cursor_seq), attempts, state, str(error_code)[:80],
                     next_at, manual, now, agent_id, conversation_id, kind),
                )
            return {"attempts": attempts, "manual_required": bool(manual),
                    "next_allowed_at": next_at, "state": state}

    def chat_task_success(self, *, agent_id: str, conversation_id: str,
                          kind: str, cursor_seq: int, pending: bool = False, cooldown_seconds: int=0) -> None:
        now = _now_iso()
        with self._txn() as cur:
            cur.execute(
                "INSERT INTO chat_task(task_id,agent_id,conversation_id,kind,cursor_seq,"
                "attempts,max_attempts,state,error_code,next_allowed_at,manual_required,updated_at) "
                "VALUES(?,?,?,?,?,0,?,'done','',NULL,0,?) "
                "ON CONFLICT(agent_id,conversation_id,kind) DO UPDATE SET "
                "cursor_seq=excluded.cursor_seq,attempts=0,state='done',error_code='',"
                "next_allowed_at=NULL,manual_required=0,updated_at=excluded.updated_at",
                ("ct-" + uuid.uuid4().hex, agent_id, conversation_id, kind,
                 int(cursor_seq), self._TASK_DEFAULT_MAX, now),
            )
            if pending:cur.execute("UPDATE chat_task SET state='pending' WHERE agent_id=? AND conversation_id=? AND kind=?",(agent_id,conversation_id,kind))
            if cooldown_seconds:
                from datetime import datetime,timedelta
                until=(datetime.now(_user_tz())+timedelta(seconds=cooldown_seconds)).replace(tzinfo=None).isoformat(timespec='seconds')
                cur.execute('UPDATE chat_task SET next_allowed_at=? WHERE agent_id=? AND conversation_id=? AND kind=?',(until,agent_id,conversation_id,kind))

    def chat_l3_eligible(self,*,agent_id,conversation_id,after_seq):
        with self._txn() as cur:
            rows=cur.execute("SELECT DISTINCT c.seq FROM chat_event c JOIN l2_turn_source s ON s.record_id=c.source_record_id JOIN l2_turn t ON t.turn_id=s.turn_id JOIN delivery_record d ON d.turn_id=t.turn_id WHERE c.agent_id=? AND c.conversation_id=? AND c.seq>? AND c.role='member' AND d.evidence<>'silent_collection' AND t.agent_id=c.agent_id AND t.conversation_id=c.conversation_id ORDER BY c.seq",(agent_id,conversation_id,after_seq)).fetchall()
        return [int(r[0]) for r in rows]

    def chat_task_pending(self, *, limit: int = 20) -> List[Dict[str, Any]]:
        with self._txn() as cur:
            rows = cur.execute(
                "SELECT agent_id,conversation_id,kind,cursor_seq,attempts,max_attempts,"
                "state,error_code,next_allowed_at,manual_required FROM chat_task "
                "WHERE manual_required=0 AND state IN ('pending','failed','running') "
                "ORDER BY updated_at ASC LIMIT ?",
                (max(1, min(int(limit), 50)),),
            ).fetchall()
        return [dict(row) if hasattr(row, "keys") else {
            "agent_id": row[0], "conversation_id": row[1], "kind": row[2],
            "cursor_seq": row[3], "attempts": row[4], "max_attempts": row[5],
            "state": row[6], "error_code": row[7], "next_allowed_at": row[8],
            "manual_required": bool(row[9]),
        } for row in rows]

    def chat_task_manual_retry(self, *, agent_id: str, conversation_id: str,
                               kind: str) -> bool:
        with self._txn() as cur:
            cursor = cur.execute(
                "UPDATE chat_task SET attempts=0,state='pending',error_code='',"
                "next_allowed_at=NULL,manual_required=0,updated_at=? "
                "WHERE agent_id=? AND conversation_id=? AND kind=?",
                (_now_iso(), agent_id, conversation_id, kind),
            )
            changed=cursor.rowcount==1
            if changed and kind=='archive':
                cur.execute("UPDATE chat_archive SET attempts=0,state='queued' WHERE agent_id=? AND conversation_id=? AND state IN ('failed','interrupted')",(agent_id,conversation_id))
            return changed

    def chat_task_list(self, *, agent_id, conversation_id):
        with self._txn() as cur:
            return [dict(r) for r in cur.execute('SELECT kind,cursor_seq,attempts,max_attempts,state,error_code,next_allowed_at,manual_required FROM chat_task WHERE agent_id=? AND conversation_id=? ORDER BY kind',(agent_id,conversation_id)).fetchall()]

    def chat_recovery_scopes(self, *, agent_id: str, limit: int=20, offset: int=0):
        with self._txn() as cur:
            rows=cur.execute("SELECT agent_id,conversation_id,MAX(seq) AS last_seq FROM chat_event c WHERE c.agent_id=? GROUP BY agent_id,conversation_id HAVING MAX(seq)>MIN(COALESCE((SELECT last_seq FROM chat_cursor WHERE agent_id=c.agent_id AND conversation_id=c.conversation_id AND kind='l3'),0),COALESCE((SELECT last_seq FROM chat_cursor WHERE agent_id=c.agent_id AND conversation_id=c.conversation_id AND kind='event'),0)) ORDER BY conversation_id LIMIT ? OFFSET ?",(agent_id,max(1,min(limit,50)),max(0,offset))).fetchall()
        return [dict(r) for r in rows]

    def chat_l3_list(self, *, agent_id: str, conversation_id: str, status: str = "active",
                identity_id: str = "", limit: int = 50) -> List[Dict[str, Any]]:
        """按状态列出事实（默认只 active）；候选/替代/撤回分开查询。"""

        clauses = ["agent_id=?", "conversation_id=?"]
        params: List[Any] = [agent_id, conversation_id]
        if status:
            clauses.append("status=?")
            params.append(status)
        if status=='active':
            clauses.append('(valid_until IS NULL OR valid_until>?)')
            params.append(_now_iso())
        if identity_id:
            clauses.append("identity_id=?")
            params.append(identity_id)
        params.append(max(1, min(int(limit), 200)))
        with self._txn() as cur:
            rows = cur.execute(
                "SELECT fact_id,identity_id,display_name,content,status,evidence_type,"
                "evidence_quote,source_record_ids,source_seqs_json,record_mode,valid_until,"
                "replaces_fact_id,occurred_at,updated_at FROM l3_fact WHERE " +
                " AND ".join(clauses) + " ORDER BY updated_at DESC LIMIT ?",
                params,
            ).fetchall()
        result = []
        for row in rows:
            item = dict(row) if hasattr(row, "keys") else {
                "fact_id": row[0], "identity_id": row[1], "display_name": row[2],
                "content": row[3], "status": row[4], "evidence_type": row[5],
                "evidence_quote": row[6], "source_record_ids": row[7],
                "source_seqs_json": row[8], "record_mode": row[9],
                "valid_until": row[10], "replaces_fact_id": row[11],
                "occurred_at": row[12], "updated_at": row[13],
            }
            for key in ("source_record_ids", "source_seqs_json"):
                raw = item.get(key)
                try:
                    parsed = json.loads(raw) if isinstance(raw, str) else raw
                except ValueError:
                    parsed = []
                item[key.replace("_json", "")] = parsed or []
                item.pop(key, None)
            result.append(item)
        return result

    def l3_set_status(self, *, agent_id: str, conversation_id: str, fact_id: str,
                      status: str, reason: str = "", replaced_by: str = "") -> bool:
        """用户纠正/撤回/替代：状态流转 + 历史记录（不删事实）。"""

        if status not in {"active", "replaced", "revoked", "candidate"}:
            return False
        now = _now_iso()
        with self._txn() as cur:
            row = cur.execute(
                "SELECT status,identity_id FROM l3_fact WHERE fact_id=? AND agent_id=? AND conversation_id=?",
                (fact_id, agent_id, conversation_id),
            ).fetchone()
            if row is None:
                return False
            if status=='replaced':
                replacement=cur.execute('SELECT identity_id,status FROM l3_fact WHERE fact_id=? AND agent_id=? AND conversation_id=?',(replaced_by,agent_id,conversation_id)).fetchone()
                if replaced_by==fact_id or replacement is None or replacement[0]!=row[1] or replacement[1]!='active':return False
                cur.execute('UPDATE l3_fact SET replaces_fact_id=? WHERE fact_id=?',(fact_id,replaced_by))
            previous = str(row[0] or "")
            cur.execute(
                "UPDATE l3_fact SET status=?,valid_until=?,"
                "updated_at=? WHERE fact_id=?",
                (status,
                 now if status in {"revoked", "replaced"} else None,
                 now, fact_id),
            )
            cur.execute(
                "INSERT INTO l3_fact_history(history_id,fact_id,agent_id,conversation_id,"
                "from_status,to_status,reason,replaced_by,changed_at) VALUES(?,?,?,?,?,?,?,?,?)",
                ("lh-" + uuid.uuid4().hex, fact_id, agent_id, conversation_id,
                 previous, status, str(reason)[:200], replaced_by or None, now),
            )
        return True

    def chat_link_assistant_echo(self, *, agent_id: str, conversation_id: str,
                                 source_event_id: str, action_id: str = "",
                                 asset_id: str = "",
                                 within_seconds: float = 180.0) -> bool:
        """把桥回显的机器人消息关联到已有 assistant 事件（不新增 member 记录）。

        优先按 `delivery_action_id` 精确匹配；否则只在该时间窗内、
        尚未关联回显且 action_id 非空的一条 assistant 事件上关联。
        """

        if not source_event_id or not action_id:
            # 无可靠业务标识时**不猜测**：宁可不关联，也不把回显绑到别的回复。
            return False
        with self._txn() as cur:
            duplicate = cur.execute(
                "SELECT 1 FROM chat_event WHERE agent_id=? AND conversation_id=? "
                "AND source_event_id=? LIMIT 1",
                (agent_id, conversation_id, str(source_event_id)[:250]),
            ).fetchone()
            if duplicate is not None:
                return True  # 重复回显幂等
            rows = cur.execute(
                "SELECT event_id,media_json FROM chat_event WHERE agent_id=? AND conversation_id=? "
                "AND role='assistant' AND delivery_action_id=? ORDER BY seq ASC LIMIT 2",
                (agent_id, conversation_id, action_id),
            ).fetchall()
            if len(rows) != 1:
                # 同一 action 对应多条或不存在：不确定，不修改任何记录。
                return False
            try:
                media = json.loads(rows[0][1] or "[]")
            except ValueError:
                media = []
            media = media if isinstance(media, list) else []
            if asset_id:
                # 多图同 action：必须按媒体项（asset_id/handle）区分。
                if not any(str(item.get("handle") or "") == asset_id for item in media):
                    return False
            elif len(media) > 1:
                return False
            cur.execute(
                "UPDATE chat_event SET source_event_id=? WHERE event_id=?",
                (str(source_event_id)[:250], rows[0][0]),
            )
        return True

    def chat_identity_for_record(self, *, agent_id: str, conversation_id: str,
                                 record_id: str) -> str:
        """从入站原文取稳定身份（L3 归属；取不到返回空，调用方跳过）。"""

        if not record_id:
            return ""
        with self._txn() as cur:
            row = cur.execute(
                "SELECT sender_identity_id FROM raw_message "
                "WHERE agent_id=? AND conversation_id=? AND record_id=?",
                (agent_id, conversation_id, record_id),
            ).fetchone()
        return str(row[0] or "") if row is not None else ""

    def chat_search(self, *, agent_id: str, conversation_id: str,
                    keywords: Sequence[str], since: str = "", until: str = "",
                    limit: int = 8) -> List[Dict[str, Any]]:
        """聊天原文关键词检索（有界；可按时间窗过滤；仅本会话）。"""

        terms = [str(item).strip()[:40] for item in (keywords or ()) if str(item).strip()][:6]
        if not terms:
            return []
        clauses = ["agent_id=?", "conversation_id=?"]
        params: List[Any] = [agent_id, conversation_id]
        like = " OR ".join(["text LIKE ?"] * len(terms))
        clauses.append(f"({like})")
        params.extend([f"%{term}%" for term in terms])
        if since:
            clauses.append("occurred_at>=?")
            params.append(since)
        if until:
            clauses.append("occurred_at<=?")
            params.append(until)
        params.append(max(1, min(int(limit), 20)))
        with self._txn() as cur:
            rows = cur.execute(
                "SELECT * FROM chat_event WHERE " + " AND ".join(clauses) +
                " ORDER BY seq DESC LIMIT ?",
                params,
            ).fetchall()
        return [self._chat_row(row) for row in reversed(rows)]

    def chat_archive_list(self, *, agent_id: str, conversation_id: str,
                          limit: int = 20) -> List[Dict[str, Any]]:
        with self._txn() as cur:
            rows = cur.execute(
                "SELECT archive_id,from_seq,to_seq,message_count,state,error_code,memory_id,"
                "summary_chars,created_at,updated_at FROM chat_archive "
                "WHERE agent_id=? AND conversation_id=? ORDER BY from_seq DESC LIMIT ?",
                (agent_id, conversation_id, max(1, min(int(limit), 100))),
            ).fetchall()
        return [dict(row) if hasattr(row, "keys") else {
            "archive_id": row[0], "from_seq": row[1], "to_seq": row[2],
            "message_count": row[3], "state": row[4], "error_code": row[5],
            "memory_id": row[6], "summary_chars": row[7],
            "created_at": row[8], "updated_at": row[9],
        } for row in rows]

    def chat_archive_status(self, *, agent_id: str, conversation_id: str) -> Dict[str, Any]:
        counts = self.chat_counts(agent_id=agent_id, conversation_id=conversation_id)
        with self._txn() as cur:
            row = cur.execute(
                "SELECT state,COUNT(*) FROM chat_archive WHERE agent_id=? AND conversation_id=? "
                "GROUP BY state",
                (agent_id, conversation_id),
            ).fetchall()
            cursor_row = cur.execute(
                "SELECT last_seq FROM chat_cursor WHERE agent_id=? AND conversation_id=? AND kind='archive'",
                (agent_id, conversation_id),
            ).fetchone()
        by_state = {(item[0] if not hasattr(item, "keys") else item[0]): int(item[1]) for item in row}
        return {
            "messages": counts["messages"],
            "last_seq": counts["last_seq"],
            "archive_cursor": int(cursor_row[0]) if cursor_row is not None else 0,
            "pending": max(0, counts["last_seq"] - (int(cursor_row[0]) if cursor_row is not None else 0)),
            "tasks": by_state,
        }

    # ------------------------------------------------------------------ #
    # 事件条目（新 kind，区别于旧的每轮截断日记）
    # ------------------------------------------------------------------ #

    def memory_event_upsert(self, *, agent_id: str, conversation_id: str, kind: str,
                            status: str, title: str, detail: str,
                            participants: Sequence[str] = (), source_seqs: Sequence[int] = (),
                            occurred_at: str = "", planned_at: str = "",
                            dedupe_key: str = "") -> Dict[str, Any]:
        # 稳定关联：同会话 + 同 kind + 归一标题 → 同一条事件，detail/status 只更新，
        # 不因叙述变化新建。明确传入 dedupe_key 时优先。
        normalized = "".join(
            char for char in (title or "") if char.isalnum()
        ).lower()[:80]
        dedupe = dedupe_key or hashlib.sha256(
            f"{conversation_id}\x1f{kind}\x1f{normalized}".encode()
        ).hexdigest()[:32]
        now = _now_iso()
        with self._txn() as cur:
            # 只保留确实属于本会话的序号（模型给的序号不做真实来源）。
            seqs = [int(item) for item in source_seqs][:200]
            valid_seqs: List[int] = []
            if seqs:
                placeholders = ",".join("?" * len(seqs))
                rows = cur.execute(
                    "SELECT seq FROM chat_event WHERE agent_id=? AND conversation_id=? "
                    f"AND seq IN ({placeholders})",
                    (agent_id, conversation_id, *seqs),
                ).fetchall()
                valid_seqs = sorted({int(row[0]) for row in rows})
            source_seqs = valid_seqs
            existing = cur.execute(
                "SELECT * FROM memory_event WHERE agent_id=? AND conversation_id=? AND dedupe_key=?",
                (agent_id, conversation_id, dedupe),
            ).fetchone()
            if existing is not None:
                cur.execute('INSERT INTO memory_event_history VALUES(?,?,?,?,?,?)',('meh-'+uuid.uuid4().hex,existing['event_id'],agent_id,conversation_id,json.dumps(dict(existing),ensure_ascii=False),now))
                source_seqs=sorted(set(source_seqs)|set(json.loads(existing['source_seqs_json'] or '[]')))
                cur.execute(
                    "UPDATE memory_event SET status=?,title=?,detail=?,participants_json=?,"
                    "source_seqs_json=?,occurred_at=?,planned_at=?,updated_at=? WHERE event_id=?",
                    (status, title[:200], detail[:2000],
                     json.dumps([str(item)[:120] for item in participants][:20], ensure_ascii=False),
                     json.dumps([int(item) for item in source_seqs][:200]),
                     occurred_at or None, planned_at or None, now, existing[0]),
                )
                return {"event_id": existing[0], "updated": True}
            event_id = "me-" + uuid.uuid4().hex
            cur.execute(
                "INSERT INTO memory_event(event_id,agent_id,conversation_id,kind,status,title,detail,"
                "participants_json,source_seqs_json,occurred_at,planned_at,created_at,updated_at,dedupe_key) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (event_id, agent_id, conversation_id, kind[:20], status[:20], title[:200],
                 detail[:2000],
                 json.dumps([str(item)[:120] for item in participants][:20], ensure_ascii=False),
                 json.dumps([int(item) for item in source_seqs][:200]),
                 occurred_at or None, planned_at or None, now, now, dedupe),
            )
        return {"event_id": event_id, "updated": False}

    def l4_summary_upsert(
        self, *, agent_id: str, conversation_id: str, channel_id: str,
        account_id: str, kind: str, content: str,
        source_seqs: Sequence[int] = (), occurred_at: str = "",
    ) -> Dict[str, Any]:
        """系统整理摘要写入 L4（不冒称用户明确授权），并登记来源序号。"""

        text = (content or "").strip()
        if not text:
            return {}
        dedupe = "chat:" + hashlib.sha256(
            f"{kind}\x1f{conversation_id}\x1f{text}".encode()
        ).hexdigest()[:32]
        now = _now_iso()
        with self._txn() as cur:
            existing = cur.execute(
                "SELECT memory_id FROM l4_memory WHERE agent_id=? AND dedupe_key=?",
                (agent_id, dedupe),
            ).fetchone()
            if existing is not None:
                return {"memory_id": existing[0], "duplicate": True}
            memory_id = "lm-" + uuid.uuid4().hex
            cur.execute(
                "INSERT INTO l4_memory(memory_id,agent_id,conversation_id,channel_id,account_id,"
                "kind,status,content,content_chars,subject_identity_id,subject_label,request_text,"
                "request_record_id,turn_id,dedupe_key,occurred_at,saved_at,updated_at) "
                "VALUES(?,?,?,?,?,?,'saved',?,?,NULL,NULL,?,NULL,NULL,?,?,?,?)",
                (memory_id, agent_id, conversation_id, channel_id[:80], account_id[:80],
                 kind[:40], text, len(text), text[:400], dedupe,
                 occurred_at or now, now, now),
            )
            for seq in list(source_seqs)[:200]:
                cur.execute(
                    "INSERT OR IGNORE INTO l4_memory_source(memory_id,source_type,source_ref,position) "
                    "VALUES(?,?,?,?)",
                    (memory_id, "chat_seq", str(int(seq)), int(seq)),
                )
        return {"memory_id": memory_id, "duplicate": False}

    def l3_from_archive(self,*,agent_id,conversation_id,archive_id,facts):
        """Bind accepted facts to a successfully archived member's raw identity."""
        with self._txn() as cur:
            rows=cur.execute("SELECT ce.event_id,ce.text,ce.source_record_id,r.sender_identity_id,ce.display_name,ce.seq,ce.occurred_at "
                "FROM l2_interaction i JOIN chat_event ce ON ce.event_id=i.event_id "
                "JOIN l2_archive a ON a.archive_id=i.archive_id "
                "JOIN raw_message r ON r.record_id=ce.source_record_id AND r.agent_id=i.agent_id AND r.conversation_id=i.conversation_id "
                "WHERE i.agent_id=? AND i.conversation_id=? AND i.archive_id=? AND a.state='completed' "
                "AND ce.role='member' AND ce.kind='text'",(agent_id,conversation_id,archive_id)).fetchall()
        sources={str(row[0]):row for row in rows};written=0
        for fact in facts[:3]:
            source=sources.get(str(fact.get('source_event_id') or ''))
            if not source or not source[2] or not source[3] or not fact.get('quote') or fact['quote'] not in source[1]:continue
            with self._txn() as cur:
                previous=cur.execute('SELECT status FROM l3_fact WHERE agent_id=? AND conversation_id=? AND identity_id=? AND content_hash=?',
                    (agent_id,conversation_id,source[3],hashlib.sha256(fact['content'].encode('utf-8')).hexdigest()[:32])).fetchone()
            if previous is not None and previous[0] in ('revoked','replaced'):continue
            saved=self.l3_candidate_upsert(agent_id=agent_id,conversation_id=conversation_id,identity_id=source[3],
                content=fact['content'],evidence_quote=fact['quote'],source_record_ids=[source[2]],
                display_name=str(source[4] or ''),source_seqs=[source[5]],occurred_at=str(source[6] or ''),
                status='active',record_mode='l2_archive',evidence_type='explicit_preference' if fact.get('category')=='stable_preference' else 'explicit_statement')
            if saved and not saved.get('duplicate'):written+=1
        return written

    def l3_candidate_upsert(
        self, *, agent_id: str, conversation_id: str, identity_id: str,
        content: str, evidence_quote: str, source_record_ids: Sequence[str],
        display_name: str = "", source_seqs: Sequence[int] = (),
        occurred_at: str = "", status: str = "candidate",
        evidence_type: str = "explicit_statement", replaces_fact_id: str = "", record_mode: str = "",
    ) -> Optional[Dict[str, Any]]:
        """L3 候选/生效事实写入（来源与逐字证据必须可核对）。

        - `source_record_ids` 必须真实存在且属于 (agent, conversation)；
        - `evidence_quote` 必须逐字出现在来源原文里；
        - 同 (agent, conversation, identity, content_hash) 不新增重复事实，
          只合并来源；已 active 的事实不被 candidate 降级。
        返回 `{fact_id, duplicate, status}`；校验失败返回 None。
        """

        text = (content or "").strip()
        quote = (evidence_quote or "").strip()
        records = [str(item) for item in (source_record_ids or ()) if str(item)]
        if not text or not quote or not records or not identity_id:
            return None
        if status not in {"candidate", "active", "replaced", "revoked"}:
            return None
        with self._txn() as cur:
            placeholders = ",".join("?" for _ in records)
            rows = cur.execute(
                "SELECT record_id,text FROM raw_message WHERE agent_id=? AND conversation_id=? "
                f"AND record_id IN ({placeholders})",
                (agent_id, conversation_id, *records),
            ).fetchall()
            found = {row[0]: row[1] for row in rows}
            if set(found) != set(records):
                return None
            if not any(quote in (value or "") for value in found.values()):
                return None
            content_hash = hashlib.sha256(text.encode("utf-8")).hexdigest()[:32]
            existing = cur.execute(
                "SELECT fact_id,status FROM l3_fact WHERE agent_id=? AND conversation_id=? "
                "AND identity_id=? AND content_hash=?",
                (agent_id, conversation_id, identity_id, content_hash),
            ).fetchone()
            now = _now_iso()
            if existing is not None:
                fact_id, current = existing[0], str(existing[1] or "")
                merged_status = "active" if current == "active" else status
                cur.execute(
                    "UPDATE l3_fact SET status=?,updated_at=?,evidence_quote=?,"
                    "source_seqs_json=?,record_mode=?,valid_until=NULL,"
                    "replaces_fact_id=COALESCE(?,replaces_fact_id) WHERE fact_id=?",
                    (merged_status, now, quote,
                     json.dumps([int(item) for item in source_seqs][:200]),
                     record_mode or ("background" if status == "candidate" else "explicit"),
                     replaces_fact_id or None, fact_id),
                )
                return {"fact_id": fact_id, "duplicate": True, "status": merged_status}
            fact_id = "lf-" + uuid.uuid4().hex
            cur.execute(
                "INSERT INTO l3_fact(fact_id,agent_id,conversation_id,identity_id,display_name,"
                "content,content_hash,status,evidence_type,evidence_quote,source_record_ids,"
                "occurred_at,extracted_at,updated_at,record_mode,valid_until,replaces_fact_id,"
                "source_seqs_json) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,NULL,?,?)",
                (fact_id, agent_id, conversation_id, identity_id, display_name[:120],
                 text, content_hash, status, evidence_type, quote,
                 json.dumps(records, ensure_ascii=False),
                 occurred_at or None, now, now,
                 record_mode or ("background" if status == "candidate" else "explicit"),
                 replaces_fact_id or None,
                 json.dumps([int(item) for item in source_seqs][:200])),
            )
        return {"fact_id": fact_id, "duplicate": False, "status": status}

    def memory_event_list(self, *, agent_id: str, conversation_id: str,
                          limit: int = 20, status: str = "") -> List[Dict[str, Any]]:
        clauses = ["agent_id=?", "conversation_id=?"]
        params: List[Any] = [agent_id, conversation_id]
        if status:
            clauses.append("status=?")
            params.append(status)
        params.append(max(1, min(int(limit), 100)))
        with self._txn() as cur:
            rows = cur.execute(
                "SELECT event_id,kind,status,title,detail,participants_json,source_seqs_json,"
                "occurred_at,planned_at,created_at,updated_at FROM memory_event WHERE " +
                " AND ".join(clauses) + " ORDER BY COALESCE(occurred_at,created_at) DESC, event_id DESC LIMIT ?",
                params,
            ).fetchall()
        result = []
        for row in rows:
            item = dict(row) if hasattr(row, "keys") else {
                "event_id": row[0], "kind": row[1], "status": row[2], "title": row[3],
                "detail": row[4], "participants_json": row[5], "source_seqs_json": row[6],
                "occurred_at": row[7], "planned_at": row[8],
                "created_at": row[9], "updated_at": row[10],
            }
            for key in ("participants_json", "source_seqs_json"):
                try:
                    item[key.replace("_json", "")] = json.loads(item.pop(key) or "[]")
                except ValueError:
                    item[key.replace("_json", "")] = []
            with self._txn() as cur:
                history=cur.execute('SELECT previous_json,changed_at FROM memory_event_history WHERE agent_id=? AND conversation_id=? AND event_id=? ORDER BY changed_at DESC,history_id DESC LIMIT 10',(agent_id,conversation_id,item['event_id'])).fetchall()
            item['history']=[{'previous':json.loads(h[0]),'changed_at':h[1]} for h in history]
            result.append(item)
        return result

    # ------------------------------------------------------------------ #
    # L2 独立交互（M01/M03）
    #
    # 只索引「机器人实际参与」的交互：一条真实用户消息、或一条**已确认送达**
    # 的机器人发言，各计 1 条；引用 `chat_event.event_id`，不复制原文。
    # 闲聊、判断机 PASS、未发送的助手文本都不进这张表。
    # ------------------------------------------------------------------ #

    @staticmethod
    def _l2_interaction_id(event_id: str) -> str:
        return "li-" + str(event_id or "")[:80]

    def l2_link_member(self, *, agent_id: str, conversation_id: str,
                       turn_id: str, source_event_ids: Sequence[str]) -> int:
        """把本轮**实际进入主模型**的用户原文登记为交互（幂等）。"""

        ids = [str(item)[:250] for item in (source_event_ids or ()) if str(item)]
        if not ids or not agent_id or not conversation_id:
            return 0
        now = _now_iso()
        inserted = 0
        with self._txn() as cur:
            for source_id in ids:
                row = cur.execute(
                    "SELECT event_id,seq,source_event_id,occurred_at FROM chat_event "
                    "WHERE agent_id=? AND dedupe_key=?",
                    (agent_id, f"member:{source_id}"),
                ).fetchone()
                if row is None:
                    continue
                cur.execute(
                    "INSERT OR IGNORE INTO l2_interaction("
                    " interaction_id,agent_id,conversation_id,event_id,seq,role,"
                    " turn_id,source_event_id,state,archive_id,occurred_at,created_at"
                    ") VALUES(?,?,?,?,?,'user',?,?,'active','',?,?)",
                    (self._l2_interaction_id(row[0]), agent_id, conversation_id,
                     row[0], int(row[1]), str(turn_id or ""), str(row[2] or ""),
                     str(row[3] or now), now),
                )
                inserted += cur.rowcount
        return inserted

    def l2_link_assistant(self, *, agent_id: str, conversation_id: str,
                          turn_id: str, dedupe_key: str = "") -> int:
        """把一条**已确认送达**的机器人发言登记为交互（幂等）。

        只认 `chat_event` 里由 `chat_append_assistant` 写下的记录（未确认发送的
        文本本来就不在流水里），因此不会把 unknown/未发送的内容当成已发言。
        """

        if not turn_id or not agent_id or not conversation_id:
            return 0
        now = _now_iso()
        with self._txn() as cur:
            row = cur.execute(
                "SELECT event_id,seq,source_event_id,occurred_at FROM chat_event "
                "WHERE agent_id=? AND conversation_id=? AND dedupe_key=? AND role='assistant'",
                (agent_id, conversation_id, dedupe_key or f"assistant:{turn_id}"),
            ).fetchone()
            if row is None:
                return 0
            cur.execute(
                "INSERT OR IGNORE INTO l2_interaction("
                " interaction_id,agent_id,conversation_id,event_id,seq,role,"
                " turn_id,source_event_id,state,archive_id,occurred_at,created_at"
                ") VALUES(?,?,?,?,?,'assistant',?,?,'active','',?,?)",
                (self._l2_interaction_id(row[0]), agent_id, conversation_id,
                 row[0], int(row[1]), turn_id, str(row[2] or ""),
                 str(row[3] or now), now),
            )
            return cur.rowcount

    def l2_snapshot(self, *, agent_id: str, conversation_id: str,
                    include_tools: bool = True,
                    exclude_source_event_ids: Sequence[str] = ()) -> Dict[str, Any]:
        """**单事务**读取应答所需的一致 L2 快照（最近前情 + 未归档交互）。

        与"两次独立 await"相比：前情与交互来自同一时刻，不会同时包含
        "已归档原文"和"它们的新摘要"；`snapshot_id` 由（前情 archive_id、
        最大 seq、条数）派生，可在日志/状态里比对两次装载是否同源（不含正文）。
        当前批次排除与 L10 去重都以这份快照为准。
        """

        excluded = [str(x)[:250] for x in (exclude_source_event_ids or ()) if str(x)]
        clauses = ["i.agent_id=?", "i.conversation_id=?", "i.state='active'"]
        params: List[Any] = [agent_id, conversation_id]
        if excluded:
            clauses.append(
                "ce.source_event_id NOT IN (" + ",".join("?" * len(excluded)) + ")"
            )
            params.extend(excluded)
        with self._txn() as cur:
            summary_row = cur.execute(
                "SELECT a.archive_id,a.from_seq,a.to_seq,a.interaction_count,"
                " a.memory_id,a.updated_at,m.content "
                "FROM l2_archive a JOIN l4_memory m ON m.memory_id=a.memory_id "
                "WHERE a.agent_id=? AND a.conversation_id=? AND a.state='completed' "
                " ORDER BY a.to_seq DESC, a.rowid DESC LIMIT 1",
                (agent_id, conversation_id),
            ).fetchone()
            rows = cur.execute(
                "SELECT i.interaction_id,i.event_id,i.seq,i.role,i.turn_id,i.state,"
                " i.source_event_id,ce.text,ce.kind,ce.display_name,ce.sender_ref,"
                " ce.occurred_at,ce.media_json,ce.identity_id "
                "FROM l2_interaction i JOIN chat_event ce ON ce.event_id=i.event_id "
                "WHERE " + " AND ".join(clauses) + " ORDER BY i.seq ASC",
                params,
            ).fetchall()
            total_active = int(cur.execute(
                "SELECT COUNT(*) FROM l2_interaction WHERE agent_id=? "
                "AND conversation_id=? AND state='active'",
                (agent_id, conversation_id),
            ).fetchone()[0])
            has_chat_events = bool(cur.execute(
                "SELECT 1 FROM chat_event WHERE agent_id=? AND conversation_id=? LIMIT 1",
                (agent_id, conversation_id),
            ).fetchone())
            items = [self._l2_row(row) for row in rows]
            if include_tools:
                self._attach_tool_evidence(cur, agent_id, conversation_id, items)
        summary = ({
            "archive_id": summary_row[0], "from_seq": int(summary_row[1]),
            "to_seq": int(summary_row[2]),
            "interaction_count": int(summary_row[3]),
            "memory_id": summary_row[4], "updated_at": summary_row[5],
            "content": str(summary_row[6] or ""),
        } if summary_row is not None else None)
        last_seq = int(items[-1]["seq"]) if items else 0
        digest = hashlib.sha256(
            "\x1f".join((
                agent_id, conversation_id,
                str((summary or {}).get("archive_id") or ""),
                str(last_seq), str(len(items)),
            )).encode()
        ).hexdigest()[:32]
        return {
            "summary": summary,
            "items": items,
            "last_seq": last_seq,
            "total_active": total_active,
            "has_chat_events": has_chat_events,
            "snapshot_id": "l2s-" + digest,
        }

    def l2_window(self, *, agent_id: str, conversation_id: str,
                  include_tools: bool = False,
                  exclude_event_ids: Sequence[str] = (),
                  exclude_source_event_ids: Sequence[str] = ()) -> List[Dict[str, Any]]:
        """**全部未归档交互**（正序）；可按事件 ID 排除（当前批次/去重）。"""

        clauses = ["i.agent_id=?", "i.conversation_id=?", "i.state='active'"]
        params: List[Any] = [agent_id, conversation_id]
        excluded_events = [str(x)[:80] for x in (exclude_event_ids or ()) if str(x)]
        if excluded_events:
            clauses.append(
                "i.event_id NOT IN (" + ",".join("?" * len(excluded_events)) + ")"
            )
            params.extend(excluded_events)
        excluded_sources = [str(x)[:250] for x in (exclude_source_event_ids or ()) if str(x)]
        if excluded_sources:
            clauses.append(
                "ce.source_event_id NOT IN (" + ",".join("?" * len(excluded_sources)) + ")"
            )
            params.extend(excluded_sources)
        sql = (
            "SELECT i.interaction_id,i.event_id,i.seq,i.role,i.turn_id,i.state,"
            " i.source_event_id,ce.text,ce.kind,ce.display_name,ce.sender_ref,"
            " ce.occurred_at,ce.media_json "
            "FROM l2_interaction i JOIN chat_event ce ON ce.event_id=i.event_id "
            "WHERE " + " AND ".join(clauses) + " ORDER BY i.seq ASC"
        )
        with self._txn() as cur:
            rows = cur.execute(sql, params).fetchall()
            items = [self._l2_row(row) for row in rows]
            if include_tools:
                self._attach_tool_evidence(cur, agent_id, conversation_id, items)
        return items

    def l2_list(self, *, agent_id: str, conversation_id: str, limit: int = 20,
                offset: int = 0, state: str = "") -> Dict[str, Any]:
        """管理端分页（倒序），带归档批次状态。"""

        limit = max(1, min(int(limit), 100))
        offset = max(0, int(offset))
        clauses = ["i.agent_id=?", "i.conversation_id=?"]
        params: List[Any] = [agent_id, conversation_id]
        if state:
            clauses.append("i.state=?")
            params.append(str(state))
        where = " AND ".join(clauses)
        with self._txn() as cur:
            total = int(cur.execute(
                "SELECT COUNT(*) FROM l2_interaction i WHERE " + where, params
            ).fetchone()[0])
            rows = cur.execute(
                "SELECT i.interaction_id,i.event_id,i.seq,i.role,i.turn_id,i.state,"
                " i.source_event_id,ce.text,ce.kind,ce.display_name,ce.sender_ref,"
                " ce.occurred_at,ce.media_json,a.state AS archive_state,a.memory_id "
                "FROM l2_interaction i JOIN chat_event ce ON ce.event_id=i.event_id "
                "LEFT JOIN l2_archive a ON a.archive_id=i.archive_id "
                "WHERE " + where + " ORDER BY i.seq DESC LIMIT ? OFFSET ?",
                [*params, limit, offset],
            ).fetchall()
            items = []
            for row in rows:
                item = self._l2_row(row)
                item["archive_state"] = str(row[13] or "")
                item["memory_id"] = str(row[14] or "")
                items.append(item)
            # 前端 L20 折叠组件需要真实工具证据：与 chat_window 同一附加逻辑。
            self._attach_tool_evidence(cur, agent_id, conversation_id, items)
            counts = self._l2_counts_locked(cur, agent_id, conversation_id)
        return {"items": items, "total": total, "limit": limit, "offset": offset,
                "counts": counts}

    @staticmethod
    def _l2_row(row) -> Dict[str, Any]:
        item = {
            "interaction_id": row[0], "event_id": row[1], "seq": int(row[2]),
            "role": row[3], "turn_id": row[4], "state": row[5],
            "source_event_id": row[6], "text": row[7], "kind": row[8],
            "display_name": row[9], "sender_ref": row[10],
            "occurred_at": row[11], "media": [],
        }
        try:
            item["media"] = json.loads(row[12] or "[]")
        except ValueError:
            item["media"] = []
        return item

    def _l2_counts_locked(self, cur, agent_id: str, conversation_id: str) -> Dict[str, Any]:
        row = cur.execute(
            "SELECT COUNT(*), "
            "SUM(CASE WHEN state='active' THEN 1 ELSE 0 END), "
            "SUM(CASE WHEN state='archived' THEN 1 ELSE 0 END), "
            "COALESCE(MAX(seq),0) "
            "FROM l2_interaction WHERE agent_id=? AND conversation_id=?",
            (agent_id, conversation_id),
        ).fetchone()
        latest = cur.execute(
            "SELECT archive_id,state,from_seq,to_seq,interaction_count,memory_id,"
            " error_code,attempts,created_at,updated_at FROM l2_archive "
            "WHERE agent_id=? AND conversation_id=? ORDER BY created_at DESC, rowid DESC LIMIT 1",
            (agent_id, conversation_id),
        ).fetchone()
        return {
            "total": int(row[0] or 0),
            "active": int(row[1] or 0),
            "archived": int(row[2] or 0),
            "last_seq": int(row[3] or 0),
            "latest_archive": ({
                "archive_id": latest[0], "state": latest[1],
                "from_seq": int(latest[2]), "to_seq": int(latest[3]),
                "interaction_count": int(latest[4]),
                "memory_id": str(latest[5] or ""), "error_code": str(latest[6] or ""),
                "attempts": int(latest[7] or 0),
                "created_at": latest[8], "updated_at": latest[9],
            } if latest is not None else None),
        }

    def l2_recovery_scopes(self, *, agent_id: str, trigger: int, char_trigger: int,
                           limit: int = 50, offset: int = 0) -> List[Dict[str, Any]]:
        """Find L2 backlog independently of the raw-chat extraction cursors."""
        with self._txn() as cur:
            rows = cur.execute(
                "SELECT i.conversation_id FROM l2_interaction i "
                "JOIN chat_event ce ON ce.event_id=i.event_id "
                "LEFT JOIN l2_config c ON c.agent_id=i.agent_id "
                "AND c.conversation_id=i.conversation_id "
                "WHERE i.agent_id=? AND i.state='active' AND COALESCE(c.enabled,1)=1 "
                "GROUP BY i.conversation_id "
                "HAVING COUNT(*)>=COALESCE(MAX(c.trigger_count),?) "
                "OR (? > 0 AND SUM(LENGTH(ce.text))>=?) "
                "ORDER BY i.conversation_id LIMIT ? OFFSET ?",
                (agent_id, int(trigger), int(char_trigger), int(char_trigger),
                 max(1, min(int(limit), 50)), max(0, int(offset))),
            ).fetchall()
        return [{'agent_id': agent_id, 'conversation_id': row[0]} for row in rows]

    def l2_counts(self, *, agent_id: str, conversation_id: str) -> Dict[str, Any]:
        with self._txn() as cur:
            return self._l2_counts_locked(cur, agent_id, conversation_id)

    def l2_latest_summary(self, *, agent_id: str, conversation_id: str) -> Optional[Dict[str, Any]]:
        """最近一条**已完成**的 L2 归档前情（供正常回复衔接）。"""

        with self._txn() as cur:
            row = cur.execute(
                "SELECT a.archive_id,a.from_seq,a.to_seq,a.interaction_count,"
                " a.memory_id,a.updated_at,m.content "
                "FROM l2_archive a JOIN l4_memory m ON m.memory_id=a.memory_id "
                "WHERE a.agent_id=? AND a.conversation_id=? AND a.state='completed' "
                " ORDER BY a.to_seq DESC, a.rowid DESC LIMIT 1",
                (agent_id, conversation_id),
            ).fetchone()
        if row is None:
            return None
        return {
            "archive_id": row[0], "from_seq": int(row[1]), "to_seq": int(row[2]),
            "interaction_count": int(row[3]), "memory_id": row[4],
            "updated_at": row[5], "content": str(row[6] or ""),
        }

    def l2_archive_plan(self, *, agent_id: str, conversation_id: str,
                        trigger: int, batch: int, retain: int,
                        char_trigger: int = 0,
                        expected_previous_archive_id: str = "") -> Optional[Dict[str, Any]]:
        """按**实际交互条数或字符预算**规划一次归档（不用 seq 差值）。

        触发条件：`active >= trigger` **或** 未归档交互正文字符总量 ≥ `char_trigger`
        （提前归档，避免上下文不断裁掉未归档历史造成覆盖空档）。
        边界优先保留完整轮次：先取最旧 `batch` 条，再把同一轮的尾部补齐；
        若补齐后剩余不足 `retain`，按轮次从尾部回退（保留最新对话）。
        最旧一轮本身就超过 batch 时整轮归档（否则永远无法推进），实际数量如实返回。
        `expected_previous_archive_id` 是调用方刚读到的最新完成前情版本；计划在同一
        事务快照里复核它仍未变化，防止两次读取之间并发归档推进造成前情错位。
        """

        trigger = max(2, int(trigger))
        batch = max(1, int(batch))
        retain = max(1, int(retain))
        char_trigger = max(0, int(char_trigger))
        with self._txn() as cur:
            counts = self._l2_counts_locked(cur, agent_id, conversation_id)
            active = int(counts["active"])
            summary_row = cur.execute(
                "SELECT a.archive_id,a.from_seq,a.to_seq,a.interaction_count,"
                " a.memory_id,a.updated_at,m.content "
                "FROM l2_archive a JOIN l4_memory m ON m.memory_id=a.memory_id "
                "WHERE a.agent_id=? AND a.conversation_id=? AND a.state='completed' "
                " ORDER BY a.to_seq DESC, a.rowid DESC LIMIT 1",
                (agent_id, conversation_id),
            ).fetchone()
            previous_summary = ({
                "archive_id": summary_row[0], "from_seq": int(summary_row[1]),
                "to_seq": int(summary_row[2]),
                "interaction_count": int(summary_row[3]),
                "memory_id": summary_row[4], "updated_at": summary_row[5],
                "content": str(summary_row[6] or ""),
            } if summary_row is not None else None)
            if str((previous_summary or {}).get("archive_id") or "") != str(
                expected_previous_archive_id or ""
            ):
                # 调用方读到的"此前前情"与当前最新完成前情不一致：两次读取之间
                # 已有并发归档推进，继续归档会把错误的前情作为累积基础。
                raise ValueError("summary_changed")
            rows = cur.execute(
                "SELECT i.interaction_id,i.seq,i.turn_id,i.event_id,ce.role,ce.text,"
                " ce.display_name,ce.sender_ref,ce.occurred_at,ce.kind "
                "FROM l2_interaction i JOIN chat_event ce ON ce.event_id=i.event_id "
                "WHERE i.agent_id=? AND i.conversation_id=? AND i.state='active' "
                "ORDER BY i.seq ASC",
                (agent_id, conversation_id),
            ).fetchall()
            if not rows:
                return None
            active_chars = sum(len(str(row[5] or "")) for row in rows)
            # char_trigger<=0 表示**显式禁用**字符预算触发（仅按条数触发）。
            char_reached = char_trigger > 0 and active_chars >= char_trigger
            if active < trigger and not char_reached:
                return None
            rows = cur.execute(
                "SELECT i.interaction_id,i.seq,i.turn_id,i.event_id,ce.role,ce.text,"
                " ce.display_name,ce.sender_ref,ce.occurred_at,ce.kind "
                "FROM l2_interaction i JOIN chat_event ce ON ce.event_id=i.event_id "
                "WHERE i.agent_id=? AND i.conversation_id=? AND i.state='active' "
                "ORDER BY i.seq ASC",
                (agent_id, conversation_id),
            ).fetchall()
            if not rows:
                return None
            take = min(batch, max(1, active - retain))
            selected = list(rows[:take])
            index = len(selected)
            # 补齐同一轮的后续交互（不把一轮切成两半）
            while index < len(rows) and selected and rows[index][2] == selected[-1][2]:
                selected.append(rows[index])
                index += 1
            # 剩余不足 retain：按轮次从尾部回退
            while selected and len(rows) - len(selected) < retain:
                tail_turn = selected[-1][2]
                while selected and selected[-1][2] == tail_turn:
                    selected.pop()
            if not selected:
                # 最旧一轮就比 batch 大：整轮归档，保证归档能推进。
                first_turn = rows[0][2]
                for row in rows:
                    if row[2] != first_turn:
                        break
                    selected.append(row)
            if not selected:
                return None
            items = [{
                "interaction_id": row[0], "seq": int(row[1]),
                "turn_id": str(row[2] or ""), "event_id": row[3],
                "role": str(row[4] or ""), "text": str(row[5] or ""),
                "display_name": str(row[6] or ""), "sender_ref": str(row[7] or ""),
                "occurred_at": str(row[8] or ""), "kind": str(row[9] or ""),
            } for row in selected]
            from_seq = items[0]["seq"]
            to_seq = items[-1]["seq"]
            existing = cur.execute(
                "SELECT archive_id,state,attempts,updated_at FROM l2_archive "
                "WHERE agent_id=? AND conversation_id=? AND from_seq=? AND to_seq=?",
                (agent_id, conversation_id, from_seq, to_seq),
            ).fetchone()
            now = _now_iso()
            if existing is not None:
                archive_id = str(existing[0])
                state = str(existing[1])
                attempts = int(existing[2] or 0)
                if (
                    (state in {ARCHIVE_FAILED, ARCHIVE_INTERRUPTED} and attempts < 2)
                    or (state == ARCHIVE_RUNNING and _stale(str(existing[3] or ""), 120))
                ):
                    cur.execute(
                        "UPDATE l2_archive SET state='queued',error_code='',"
                        "attempts=attempts+1,updated_at=? WHERE archive_id=?",
                        (now, archive_id),
                    )
                    return {"archive_id": archive_id, "state": ARCHIVE_QUEUED,
                            "items": items, "existing": False, "resumed": True,
                            "active_before": active, "retained": active - len(items),
                            "chars": active_chars, "triggered_by": "count",
                            "previous_summary": previous_summary}
                return {"archive_id": archive_id, "state": state, "items": items,
                        "existing": True, "active_before": active,
                        "retained": active - len(items), "chars": active_chars,
                        "triggered_by": "count",
                        "previous_summary": previous_summary}
            archive_id = "la-" + uuid.uuid4().hex
            cur.execute(
                "INSERT INTO l2_archive(archive_id,agent_id,conversation_id,from_seq,to_seq,"
                "interaction_count,state,error_code,memory_id,summary_chars,attempts,"
                "created_at,updated_at) "
                "VALUES(?,?,?,?,?,?,'queued','','',0,0,?,?)",
                (archive_id, agent_id, conversation_id, from_seq, to_seq,
                 len(items), now, now),
            )
        return {"archive_id": archive_id, "state": ARCHIVE_QUEUED, "items": items,
                "existing": False, "active_before": active,
                "retained": active - len(items), "chars": active_chars,
                "triggered_by": ("count" if active >= trigger else "char_budget"),
                "previous_summary": previous_summary}

    def l2_archive_claim(self, *, archive_id: str, agent_id: str) -> bool:
        with self._txn() as cur:
            cursor = cur.execute(
                "UPDATE l2_archive SET state='running',updated_at=? "
                "WHERE archive_id=? AND agent_id=? AND state='queued'",
                (_now_iso(), archive_id, agent_id),
            )
            return cursor.rowcount == 1

    def l2_archive_settle(self, *, archive_id: str, agent_id: str, state: str,
                          error_code: str = "") -> bool:
        with self._txn() as cur:
            row = cur.execute(
                "SELECT state FROM l2_archive WHERE archive_id=? AND agent_id=?",
                (archive_id, agent_id),
            ).fetchone()
            if row is None or str(row[0]) not in {ARCHIVE_RUNNING, ARCHIVE_QUEUED}:
                return False
            cur.execute(
                "UPDATE l2_archive SET state=?,error_code=?,updated_at=? "
                "WHERE archive_id=? AND agent_id=?",
                (state, str(error_code or "")[:80], _now_iso(), archive_id, agent_id),
            )
            return True

    def l2_archive_commit(self, *, archive_id: str, agent_id: str,
                          conversation_id: str, memory_id: str, content: str,
                          interaction_count: int = 0,
                          event_ids: Sequence[str] = (),
                          previous_archive_id: str = "") -> bool:
        """**单一事务**：写 L4 前情（status='saved'）+ 覆盖标记 + 批次 completed。

        只标记本批次实际包含的交互（`state='archived'`），绝不按 seq 区间批量更新，
        因此归档期间新到的交互不会被误标；任一步失败整体回滚。
        `previous_archive_id` 是计划时依赖的前情版本：提交时核验它仍是最新完成
        前情，否则说明并发归档已推进，本次按失败处理（不覆盖新前情）。
        """

        text = (content or "").strip()
        if not text or not memory_id:
            return False
        now = _now_iso()
        # Distinct archive batches may legitimately produce identical wording.
        # Their source ranges and parent summaries must remain independent.
        dedupe = "l2:" + str(archive_id)
        parts = str(conversation_id).split(":")
        channel_id = parts[0] if parts else ""
        account_id = parts[1] if len(parts) > 1 else ""
        with self._txn() as cur:
            row = cur.execute(
                "SELECT state,to_seq,from_seq FROM l2_archive WHERE archive_id=? AND agent_id=?",
                (archive_id, agent_id),
            ).fetchone()
            if row is None or str(row[0]) != ARCHIVE_RUNNING:
                return False
            latest = cur.execute(
                "SELECT a.archive_id,a.memory_id FROM l2_archive a "
                "WHERE a.agent_id=? AND a.conversation_id=? AND a.state='completed' "
                " ORDER BY a.to_seq DESC, a.rowid DESC LIMIT 1",
                (agent_id, conversation_id),
            ).fetchone()
            current_previous = str(latest[0]) if latest is not None else ""
            if current_previous != str(previous_archive_id or ""):
                # 计划依赖的前情已变化：本次摘要的输入不再是当前覆盖的延续。
                return False
            from_seq, to_seq = int(row[2]), int(row[1])
            wanted = [str(x)[:80] for x in (event_ids or ()) if str(x)]
            if wanted:
                # 只覆盖**实际送进摘要**的交互（字符预算可能提前收缩区间）。
                placeholders = ",".join("?" * len(wanted))
                items = cur.execute(
                    "SELECT interaction_id,event_id,seq FROM l2_interaction "
                    "WHERE agent_id=? AND conversation_id=? AND state='active' "
                    f"AND event_id IN ({placeholders}) ORDER BY seq ASC",
                    (agent_id, conversation_id, *wanted),
                ).fetchall()
            else:
                items = cur.execute(
                    "SELECT interaction_id,event_id,seq FROM l2_interaction "
                    "WHERE agent_id=? AND conversation_id=? AND state='active' "
                    "AND seq<=? AND seq>=? ORDER BY seq ASC",
                    (agent_id, conversation_id, to_seq, from_seq),
                ).fetchall()
            if not items:
                return False
            # 区间只收窄不放宽：实际覆盖到哪就记到哪。
            covered_to = int(items[-1][2])
            to_seq = min(to_seq, covered_to)
            existing = cur.execute(
                "SELECT memory_id FROM l4_memory WHERE agent_id=? AND dedupe_key=?",
                (agent_id, dedupe),
            ).fetchone()
            if existing is not None:
                saved_id = str(existing[0])
            else:
                saved_id = memory_id
                cur.execute(
                    "INSERT INTO l4_memory(memory_id,agent_id,conversation_id,channel_id,"
                    "account_id,kind,status,content,content_chars,subject_identity_id,"
                    "subject_label,request_text,request_record_id,turn_id,dedupe_key,"
                    "occurred_at,saved_at,updated_at) "
                    "VALUES(?,?,?,?,?,'l2_archive','saved',?,?,NULL,NULL,'',NULL,NULL,?,?,?,?)",
                    (saved_id, agent_id, conversation_id, channel_id, account_id,
                     text, len(text), dedupe, now, now, now),
                )
                # 来源一条不落：覆盖到多少条就写多少条来源（最多受配置 500 上限约束）。
                # 父前情来源链：本批摘要的累积基础是哪一份前情，可从 L4 来源追溯
                # （l2_parent → 上一份 l2_archive 的 memory_id），批内原文即 l2_event。
                if previous_archive_id:
                    cur.execute(
                        "INSERT OR IGNORE INTO l4_memory_source("
                        " memory_id,source_type,source_ref,position) VALUES(?,?,?,?)",
                        (saved_id, "l2_parent", str(latest[1]), 0),
                    )
                for offset, item in enumerate(items):
                    cur.execute(
                        "INSERT OR IGNORE INTO l4_memory_source("
                        " memory_id,source_type,source_ref,position) VALUES(?,?,?,?)",
                        (saved_id, "l2_event", str(item[1]), offset + 1),
                    )
            for item in items:
                cur.execute(
                    "UPDATE l2_interaction SET state='archived',archive_id=? "
                    "WHERE interaction_id=?",
                    (archive_id, item[0]),
                )
            cur.execute(
                "UPDATE l2_archive SET state='completed',memory_id=?,summary_chars=?,"
                "interaction_count=?,to_seq=?,error_code='',updated_at=? WHERE archive_id=?",
                (saved_id, len(text), len(items), to_seq, now, archive_id),
            )
            return True

    def l2_archive_status(self, *, agent_id: str, conversation_id: str,
                          limit: int = 10) -> Dict[str, Any]:
        with self._txn() as cur:
            counts = self._l2_counts_locked(cur, agent_id, conversation_id)
            rows = cur.execute(
                "SELECT archive_id,from_seq,to_seq,interaction_count,state,error_code,"
                " memory_id,summary_chars,attempts,created_at,updated_at FROM l2_archive "
                "WHERE agent_id=? AND conversation_id=? ORDER BY created_at DESC, rowid DESC LIMIT ?",
                (agent_id, conversation_id, max(1, min(int(limit), 50))),
            ).fetchall()
        pending = counts["active"]
        return {
            "counts": counts,
            "pending": pending,
            "items": [{
                "archive_id": row[0], "from_seq": int(row[1]), "to_seq": int(row[2]),
                "interaction_count": int(row[3]), "state": row[4],
                "error_code": str(row[5] or ""), "memory_id": str(row[6] or ""),
                "summary_chars": int(row[7] or 0), "attempts": int(row[8] or 0),
                "created_at": row[9], "updated_at": row[10],
            } for row in rows],
        }

    def l2_config_get(self, *, agent_id: str, conversation_id: str) -> Dict[str, Any]:
        with self._txn() as cur:
            row = cur.execute(
                "SELECT revision,trigger_count,retain_count,enabled,updated_at "
                "FROM l2_config WHERE agent_id=? AND conversation_id=?",
                (agent_id, conversation_id),
            ).fetchone()
        if row is None:
            return {"revision": 0, "trigger_count": None, "retain_count": None,
                    "enabled": None, "updated_at": ""}
        return {"revision": int(row[0]), "trigger_count": row[1],
                "retain_count": row[2],
                "enabled": (None if row[3] is None else bool(row[3])),
                "updated_at": row[4]}

    def l2_config_set(self, *, agent_id: str, conversation_id: str,
                      expected_revision: int, trigger_count: Optional[int],
                      retain_count: Optional[int], enabled: Optional[bool]) -> Dict[str, Any]:
        """会话级覆盖（乐观锁）；返回新 revision。`None` = 继承默认。"""

        now = _now_iso()
        with self._txn() as cur:
            row = cur.execute(
                "SELECT revision FROM l2_config WHERE agent_id=? AND conversation_id=?",
                (agent_id, conversation_id),
            ).fetchone()
            current = int(row[0]) if row is not None else 0
            if int(expected_revision) != current:
                raise ValueError("revision_conflict")
            revision = current + 1
            cur.execute(
                "INSERT INTO l2_config(agent_id,conversation_id,revision,trigger_count,"
                "retain_count,enabled,updated_at) VALUES(?,?,?,?,?,?,?) "
                "ON CONFLICT(agent_id,conversation_id) DO UPDATE SET "
                "revision=excluded.revision,trigger_count=excluded.trigger_count,"
                "retain_count=excluded.retain_count,enabled=excluded.enabled,"
                "updated_at=excluded.updated_at",
                (agent_id, conversation_id, revision,
                 None if trigger_count is None else int(trigger_count),
                 None if retain_count is None else int(retain_count),
                 None if enabled is None else (1 if enabled else 0), now),
            )
        return {"revision": revision, "updated_at": now}
