"""P15 存储混入：持久通用会话目录、消息回放、L3 人物事实、向量索引。

线程/事务模型与 `store.py` 一致（单进程单连接 + `_txn` 短事务）；
异步调用必须经 `repository.py` 的 `_read`/`_write`（线程内执行）。

边界：
- 会话目录是**持久事实**（跨重启）；内存里的运行状态（busy、L1/L10）重启即空，
  两者分开表达，不互相伪装。
- 回放只给记录 ID、状态、已确认正文与受控摘要；不回放 Base64、Key、工具全文。
- L3 只存"来源可核对的明确陈述/偏好"（status/evidence_type/来源记录 ID），
  不是人格推断；`identity/alias` ≠ 人物事实。
- 向量块带 (provider, model, dimension, text_sha256)：不同模型向量不混用，
  文本版本变化即重写；不索引 Key/临时结果/Base64/文件全文。
"""

from __future__ import annotations

import hashlib
import json
import uuid
from typing import Any, Dict, List, Optional, Sequence, Tuple

from pero.memory.models import new_record_id

FACT_ACTIVE = "active"
FACT_SUPERSEDED = "superseded"
FACT_BLOCKED = "blocked"
#: 合法事实状态；查询端只回这些值。
FACT_STATUSES = (FACT_ACTIVE, FACT_SUPERSEDED, FACT_BLOCKED)

#: 证据类型：只收"来源可核对"的类别，没有 free-form。
EVIDENCE_EXPLICIT_STATEMENT = "explicit_statement"
EVIDENCE_EXPLICIT_PREFERENCE = "explicit_preference"
EVIDENCE_TYPES = (EVIDENCE_EXPLICIT_STATEMENT, EVIDENCE_EXPLICIT_PREFERENCE)


def _fact_id() -> str:
    return f"f3-{uuid.uuid4().hex[:16]}"


def _now() -> str:
    """业务时间统一北京时间（aware，明确 +08:00）。"""

    from pero.core.timebase import now_iso

    return now_iso()


def _chunk_id() -> str:
    return f"v-{uuid.uuid4().hex[:16]}"


class P15StoreMixin:
    """会话目录 / 回放 / L3 / 向量的同步 SQL；挂在 SqliteMemoryStore 上。"""

    # ------------------------------------------------------------------ #
    # A：持久会话目录
    # ------------------------------------------------------------------ #

    def session_upsert(
        self,
        *,
        session_id: str,
        agent_id: str,
        channel_id: str,
        account_id: str,
        conversation_id: str,
        conversation_type: str,
        target_ref: str,
        display_name: str = "",
    ) -> Dict[str, Any]:
        now = _now()
        with self._txn() as cur:
            cur.execute(
                "INSERT INTO agent_session (session_id, agent_id, channel_id,"
                " account_id, conversation_id, conversation_type, target_ref,"
                " display_name, created_at, updated_at)"
                " VALUES (?,?,?,?,?,?,?,?,?,?)"
                " ON CONFLICT(session_id, agent_id) DO UPDATE SET"
                " updated_at=excluded.updated_at,"
                " display_name=COALESCE(NULLIF(excluded.display_name,''), agent_session.display_name)",
                (session_id, agent_id, channel_id, account_id, conversation_id,
                 conversation_type, target_ref, display_name, now, now),
            )
        return self.session_get(agent_id, session_id) or {}

    def session_touch(
        self,
        *,
        agent_id: str,
        session_id: str,
        run_count: Optional[int] = None,
        last_run_id: Optional[str] = None,
        last_status: Optional[str] = None,
        last_error_code: Optional[str] = None,
    ) -> bool:
        now = _now()
        with self._txn() as cur:
            cur.execute(
                "UPDATE agent_session SET updated_at=?,"
                " run_count=COALESCE(?, run_count),"
                " last_run_id=COALESCE(?, last_run_id),"
                " last_status=COALESCE(?, last_status),"
                " last_error_code=?"
                " WHERE session_id=? AND agent_id=?",
                (now, run_count, last_run_id, last_status, last_error_code,
                 session_id, agent_id),
            )
            return cur.rowcount > 0

    def session_get(self, agent_id: str, session_id: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM agent_session WHERE session_id=? AND agent_id=?",
                (session_id, agent_id),
            ).fetchone()
        return self._session_row(row) if row is not None else None

    def session_find(self, session_id: str) -> Optional[Dict[str, Any]]:
        """跨 Agent 精确查找（generic 路径用）；不按 ID 相似猜归属。"""
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM agent_session WHERE session_id=?", (session_id,)
            ).fetchall()
        if not rows:
            return None
        if len(rows) > 1:
            # 同 ID 多 Agent：受控拒绝，调用方要求带 agent_id。
            return {"__ambiguous__": [self._session_row(r) for r in rows]}
        return self._session_row(rows[0])

    def session_list(
        self, agent_id: str, *, limit: int, cursor: str = ""
    ) -> Dict[str, Any]:
        """稳定排序 (updated_at DESC, session_id DESC) 的 cursor 分页 + total。"""
        limit = max(1, min(int(limit), 200))
        with self._lock:
            total = int(
                self._conn.execute(
                    "SELECT COUNT(*) FROM agent_session WHERE agent_id=?", (agent_id,)
                ).fetchone()[0]
            )
            params: List[Any] = [agent_id]
            clause = ""
            if cursor:
                clause = " AND (updated_at || '|' || session_id) < ?"
                params.append(cursor)
            rows = self._conn.execute(
                "SELECT * FROM agent_session WHERE agent_id=" + "?" + clause
                + " ORDER BY bj_epoch(updated_at) DESC, session_id DESC LIMIT ?",
                [*params, limit],
            ).fetchall()
        items = [self._session_row(row) for row in rows]
        next_cursor = ""
        if len(items) == limit:
            last = items[-1]
            next_cursor = f"{last['updated_at']}|{last['session_id']}"
        return {"items": items, "total": total, "next_cursor": next_cursor}

    @staticmethod
    def _session_row(row) -> Dict[str, Any]:
        return {
            "session_id": row["session_id"],
            "agent_id": row["agent_id"],
            "channel_id": row["channel_id"],
            "account_id": row["account_id"],
            "conversation_id": row["conversation_id"],
            "conversation_type": row["conversation_type"],
            "target_ref": row["target_ref"],
            "display_name": row["display_name"] or "",
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
            "run_count": int(row["run_count"] or 0),
            "last_run_id": row["last_run_id"],
            "last_status": row["last_status"],
            "last_error_code": row["last_error_code"],
        }

    # ------------------------------------------------------------------ #
    # A：消息回放（原文 / 已确认正文 / 投递状态 / 关联 L20 / 附件目录）
    # ------------------------------------------------------------------ #

    def session_replay(
        self, agent_id: str, conversation_id: str, *, limit: int, cursor: str = ""
    ) -> Dict[str, Any]:
        """按轮次回放：`cursor` 为 `<created_at>|<turn_id>`，升序稳定分页。

        每轮：原文（user_text）、assistant 视图（l2_turn.assistant_text）、
        投递状态（delivery_record.status/evidence/action_id）、关联 L20 工具
        证据（编号/工具/状态/摘要截断）、附件目录（编号/名称/handle）。
        不回放 Base64、Key、完整工具参数/结果。
        """
        limit = max(1, min(int(limit), 100))
        with self._lock:
            total = int(
                self._conn.execute(
                    "SELECT COUNT(*) FROM l2_turn WHERE agent_id=? AND conversation_id=?",
                    (agent_id, conversation_id),
                ).fetchone()[0]
            )
            params: List[Any] = [agent_id, conversation_id]
            clause = ""
            if cursor:
                at, _, turn = (cursor.partition("|"))
                from pero.core.timebase import require_epoch

                bound = require_epoch(at, "cursor")
                clause = (
                    " AND (bj_epoch(t.created_at) > ?"
                    " OR (bj_epoch(t.created_at) = ? AND t.turn_id > ?))"
                )
                params.extend([bound, bound, turn])
            rows = self._conn.execute(
                "SELECT t.* FROM l2_turn t WHERE t.agent_id=? AND t.conversation_id=?"
                + clause
                + " ORDER BY bj_epoch(t.created_at) ASC, t.turn_id ASC LIMIT ?",
                [*params, limit],
            ).fetchall()
            items: List[Dict[str, Any]] = []
            for row in rows:
                turn_id = row["turn_id"]
                delivery = self._conn.execute(
                    "SELECT status, action_id, evidence, text, created_at"
                    " FROM delivery_record WHERE turn_id=?", (turn_id,)
                ).fetchone()
                tools = self._conn.execute(
                    "SELECT call_id, tool_name, status, error_code, duration_ms,"
                    " summary, summary_truncated, resource_refs_json"
                    " FROM l20_tool_evidence WHERE turn_id=? ORDER BY created_at",
                    (turn_id,),
                ).fetchall()
                attachments = self._conn.execute(
                    "SELECT attachment_id, kind, name, handle, recognized"
                    " FROM l2_attachment WHERE turn_id=?",
                    (turn_id,),
                ).fetchall()
                items.append({
                    "turn_id": turn_id,
                    "created_at": row["created_at"],
                    "status": row["status"],
                    "interrupt_reason": row["interrupt_reason"] if "interrupt_reason" in row.keys() else None,
                    "user_text": row["user_text"],
                    "assistant_text": row["assistant_text"],
                    "delivery": {
                        "status": delivery["status"] if delivery else "not_sent",
                        "action_id": delivery["action_id"] if delivery else None,
                        "evidence": delivery["evidence"] if delivery else "",
                    },
                    "tools": [
                        {
                            "evidence_id": None,
                            "call_id": t["call_id"],
                            "tool_name": t["tool_name"],
                            "status": t["status"],
                            "error_code": t["error_code"],
                            "duration_ms": int(t["duration_ms"] or 0),
                            "summary": (t["summary"] or "")[:200],
                            "summary_truncated": bool(t["summary_truncated"]),
                            "resource_refs": json.loads(t["resource_refs_json"] or "[]"),
                        }
                        for t in tools
                    ],
                    "attachments": [
                        {
                            "attachment_id": a["attachment_id"],
                            "kind": a["kind"],
                            "name": a["name"],
                            "handle": a["handle"],
                            "recognized": bool(a["recognized"]),
                        }
                        for a in attachments
                    ],
                })
        next_cursor = ""
        if len(items) == limit:
            last = items[-1]
            next_cursor = f"{last['created_at']}|{last['turn_id']}"
        return {"items": items, "total": total, "next_cursor": next_cursor}

    def session_history_pairs(
        self, agent_id: str, conversation_id: str, *, limit: int = 200
    ) -> List[Dict[str, Any]]:
        """重启恢复用：已收尾轮次的（用户原文 / assistant 视图）按时间正序。

        只取已收尾状态（不含 pending/中断半个事实）；assistant 只在
        delivery=sent/http_delivered（已确认对外/对调用方成立）时返回。
        """
        with self._lock:
            rows = self._conn.execute(
                "SELECT t.turn_id, t.created_at, t.status, t.user_text,"
                " t.assistant_text, d.status AS delivery_status"
                " FROM l2_turn t LEFT JOIN delivery_record d ON d.turn_id=t.turn_id"
                " WHERE t.agent_id=? AND t.conversation_id=?"
                " ORDER BY bj_epoch(t.created_at) ASC, t.turn_id ASC LIMIT ?",
                (agent_id, conversation_id, max(1, min(int(limit), 500))),
            ).fetchall()
        return [
            {
                "turn_id": r["turn_id"],
                "created_at": r["created_at"],
                "status": r["status"],
                "user_text": r["user_text"],
                "assistant_text": r["assistant_text"],
                "delivery_status": r["delivery_status"],
            }
            for r in rows
        ]

    def identities_list(
        self, agent_id: str, *, limit: int, cursor: str = ""
    ) -> Dict[str, Any]:
        """per-Agent 身份目录：**只列本 Agent 真实原文来源里出现过的身份**。

        可见性 = raw_message.sender_identity_id（该 Agent 实际收到过的发送者），
        不是"同账号就全列"——一个账号服务多个群/Agent 时，未在当前 Agent
        来源出现的人不返回；最近别名/别名计数也限定在这些来源会话内，
        源外 wxid/其他群别名不随目录泄露。
        """
        limit = max(1, min(int(limit), 200))
        with self._lock:
            clause = ""
            cursor_param: List[Any] = []
            if cursor:
                clause = " AND (i.updated_at || '|' || i.identity_id) < ?"
                cursor_param.append(cursor)
            rows = self._conn.execute(
                "SELECT i.identity_id, i.channel_id, i.account_id, i.bridge_user_id,"
                " i.wxid, i.wxid_source, i.created_at, i.updated_at,"
                " (SELECT a.alias FROM identity_alias a"
                "   JOIN raw_message rm ON rm.agent_id = ?"
                "    AND rm.sender_identity_id = i.identity_id"
                "    AND ((a.scope_type='group' AND a.scope_ref = rm.target_ref)"
                "      OR (a.scope_type='account' AND a.scope_ref = rm.account_id))"
                "   WHERE a.identity_id = i.identity_id"
                "   ORDER BY a.observed_at DESC LIMIT 1) AS latest_alias,"
                " (SELECT COUNT(DISTINCT a2.alias || '|' || a2.alias_type || '|'"
                "   || a2.scope_type || '|' || a2.scope_ref)"
                "   FROM identity_alias a2"
                "   JOIN raw_message rm2 ON rm2.agent_id = ?"
                "    AND rm2.sender_identity_id = i.identity_id"
                "    AND ((a2.scope_type='group' AND a2.scope_ref = rm2.target_ref)"
                "      OR (a2.scope_type='account' AND a2.scope_ref = rm2.account_id))"
                "   WHERE a2.identity_id = i.identity_id) AS alias_count"
                " FROM identity i WHERE i.identity_id IN"
                " (SELECT DISTINCT sender_identity_id FROM raw_message"
                "  WHERE agent_id=? AND sender_identity_id IS NOT NULL)"
                + clause + " ORDER BY bj_epoch(i.updated_at) DESC, i.identity_id DESC LIMIT ?",
                [agent_id, agent_id, agent_id, *cursor_param, int(limit)],
            ).fetchall()
        items = [
            {
                "identity_id": r["identity_id"],
                "channel_id": r["channel_id"],
                "account_id": r["account_id"],
                "bridge_user_id": r["bridge_user_id"],
                "wxid": r["wxid"],
                "wxid_source": r["wxid_source"],
                "latest_alias": r["latest_alias"],
                "alias_count": int(r["alias_count"] or 0),
                "updated_at": r["updated_at"],
            }
            for r in rows
        ]
        next_cursor = ""
        if len(items) == limit:
            next_cursor = f"{items[-1]['updated_at']}|{items[-1]['identity_id']}"
        return {"items": items, "limit": limit, "next_cursor": next_cursor or None}

    # ------------------------------------------------------------------ #
    # C：L3 人物事实（来源可核对的明确陈述/偏好；不是人格推断）
    # ------------------------------------------------------------------ #

    def l3_upsert(
        self,
        *,
        agent_id: str,
        conversation_id: str,
        identity_id: str,
        display_name: str = "",
        content: str,
        status: str = FACT_ACTIVE,
        evidence_type: str = EVIDENCE_EXPLICIT_STATEMENT,
        evidence_quote: str = "",
        source_record_ids: Sequence[str] = (),
        occurred_at: str = "",
    ) -> Dict[str, Any]:
        """幂等写入（**会话作用域内**）：同 (agent, conversation, identity,
        content_hash) 只更新状态/时间。

        程序校验（不是提示词）：
        - 来源记录必须**真实存在且属于 (agent, conversation)**，否则
          `invalid_source`——不把不可核对的来源写进证据链；
        - `evidence_quote` 必须非空且**逐字出现在来源原文里**（截断不通过），
          否则 `invalid_evidence`——未知证据类型/编造时间同样拒绝；
        - 冲突事实（同人物同会话不同内容）不覆盖旧行：新旧都是可查询的
          独立记录（status + 时间戳），不静默覆盖。
        """
        if status not in FACT_STATUSES:
            raise MemoryQueryErrorP15("l3_fact 状态不合法")
        if evidence_type not in EVIDENCE_TYPES:
            raise MemoryQueryErrorP15("l3_fact 证据类型不合法")
        text = (content or "").strip()
        if not text:
            raise MemoryQueryErrorP15("l3_fact 内容不能为空")
        records = [str(r) for r in source_record_ids if str(r)]
        quote = (evidence_quote or "").strip()
        with self._lock:
            if not records:
                raise MemoryQueryErrorP15("l3_fact 来源记录为空")
            rows = self._conn.execute(
                "SELECT record_id, text FROM raw_message WHERE agent_id=?"
                " AND conversation_id=? AND record_id IN ("
                + ",".join("?" for _ in records) + ")",
                (agent_id, conversation_id, *records),
            ).fetchall()
        found = {r["record_id"]: r["text"] for r in rows}
        if set(found) != set(records):
            raise MemoryQueryErrorP15("invalid_source")
        if not quote:
            raise MemoryQueryErrorP15("invalid_evidence")
        if not any(quote in (t or "") for t in found.values()):
            # 片段必须逐字来自来源原文（截断/改写不通过）。
            raise MemoryQueryErrorP15("invalid_evidence")
        content_hash = hashlib.sha256(text.encode("utf-8")).hexdigest()[:32]
        fact_id = _fact_id()
        now = _now()
        with self._txn() as cur:
            existing = cur.execute(
                "SELECT fact_id, status FROM l3_fact"
                " WHERE agent_id=? AND conversation_id=? AND identity_id=?"
                " AND content_hash=?",
                (agent_id, conversation_id, identity_id, content_hash),
            ).fetchone()
            if existing is not None:
                existing_sources = json.loads(
                    self._l3_sources(cur, existing["fact_id"]) or "[]"
                )
                merged = sorted(set(records) | {str(s) for s in existing_sources})
                cur.execute(
                    "UPDATE l3_fact SET status=?, updated_at=?,"
                    " source_record_ids=?, evidence_quote=?"
                    " WHERE fact_id=?",
                    (status, now, json.dumps(merged, ensure_ascii=False),
                     quote, existing["fact_id"]),
                )
                row = cur.execute(
                    "SELECT * FROM l3_fact WHERE fact_id=?", (existing["fact_id"],)
                ).fetchone()
                return self._l3_row(row, duplicate=True)
            cur.execute(
                "INSERT INTO l3_fact (fact_id, agent_id, conversation_id,"
                " identity_id, display_name, content, content_hash, status,"
                " evidence_type, evidence_quote, source_record_ids, occurred_at,"
                " extracted_at, updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (fact_id, agent_id, conversation_id, identity_id,
                 display_name, text, content_hash, status, evidence_type,
                 quote, json.dumps(records, ensure_ascii=False),
                 occurred_at, now, now),
            )
            row = cur.execute(
                "SELECT * FROM l3_fact WHERE fact_id=?", (fact_id,)
            ).fetchone()
        return self._l3_row(row, duplicate=False)

    @staticmethod
    def _l3_sources(cur, fact_id: str) -> str:
        row = cur.execute(
            "SELECT source_record_ids FROM l3_fact WHERE fact_id=?", (fact_id,)
        ).fetchone()
        return row["source_record_ids"] if row is not None else "[]"

    def l3_list(
        self,
        agent_id: str,
        *,
        conversation_id: str = "",
        identity_id: str = "",
        status: str = "",
        limit: int,
        cursor: str = "",
    ) -> Dict[str, Any]:
        """按 Agent + 会话 + 人物（+状态）可见范围查询；跨群不共享。"""
        limit = max(1, min(int(limit), 200))
        if status and status not in FACT_STATUSES:
            raise MemoryQueryErrorP15("l3_fact 状态不合法")
        with self._lock:
            params: List[Any] = [agent_id]
            scope = ""
            if conversation_id:
                scope += " AND conversation_id=?"
                params.append(conversation_id)
            if identity_id:
                scope += " AND identity_id=?"
                params.append(identity_id)
            if status:
                scope += " AND status=?"
                params.append(status)
            total = int(
                self._conn.execute(
                    "SELECT COUNT(*) FROM l3_fact WHERE agent_id=?" + scope, params
                ).fetchone()[0]
            )
            clause = ""
            if cursor:
                clause = " AND (updated_at || '|' || fact_id) < ?"
                params.append(cursor)
            rows = self._conn.execute(
                "SELECT * FROM l3_fact WHERE agent_id=?" + scope + clause
                + " ORDER BY bj_epoch(updated_at) DESC, fact_id DESC LIMIT ?",
                [*params, limit],
            ).fetchall()
        items = [self._l3_row(r, duplicate=False) for r in rows]
        next_cursor = ""
        if len(items) == limit:
            next_cursor = f"{items[-1]['updated_at']}|{items[-1]['fact_id']}"
        return {"items": items, "total": total, "next_cursor": next_cursor}

    @staticmethod
    def _l3_row(row, *, duplicate: bool) -> Dict[str, Any]:
        return {
            "fact_id": row["fact_id"],
            "agent_id": row["agent_id"],
            "conversation_id": row["conversation_id"],
            "identity_id": row["identity_id"],
            "display_name": row["display_name"] or "",
            "content": row["content"],
            "status": row["status"],
            "evidence_type": row["evidence_type"],
            "evidence_quote": row["evidence_quote"] if "evidence_quote" in row.keys() else None,
            "source_record_ids": json.loads(row["source_record_ids"] or "[]"),
            "occurred_at": row["occurred_at"] or None,
            "extracted_at": row["extracted_at"],
            "updated_at": row["updated_at"],
            "duplicate": duplicate,
        }

    # ------------------------------------------------------------------ #
    # D：向量索引（同 provider/model 维度隔离；来源记录可追溯）
    # ------------------------------------------------------------------ #

    def vector_upsert(
        self,
        *,
        agent_id: str,
        conversation_id: str,
        record_id: str,
        role: str,
        text: str,
        provider: str,
        model: str,
        dimension: int,
        vector: Sequence[float],
    ) -> int:
        """按 (record_id, role, provider, model) 幂等重写；文本版本变化即更新。"""
        import struct

        text_sha = hashlib.sha256((text or "").encode("utf-8")).hexdigest()[:32]
        blob = struct.pack(f"<{len(vector)}f", *[float(x) for x in vector])
        with self._txn() as cur:
            cur.execute(
                "INSERT INTO vector_chunk (chunk_id, agent_id, conversation_id,"
                " record_id, role, text_version, text_sha256, provider, model,"
                " dimension, vector, created_at)"
                " VALUES (?,?,?,?,?,?,?,?,?,?,?,?)"
                " ON CONFLICT(record_id, role, provider, model) DO UPDATE SET"
                " text_version=excluded.text_version, text_sha256=excluded.text_sha256,"
                " dimension=excluded.dimension, vector=excluded.vector,"
                " agent_id=excluded.agent_id, conversation_id=excluded.conversation_id",
                (_chunk_id(), agent_id, conversation_id, record_id, role,
                 text_sha, text_sha, provider, model, int(dimension), blob,
                 _now()),
            )
            return cur.rowcount

    def vector_fetch_scope(
        self,
        agent_id: str,
        conversation_ids: Sequence[str],
        *,
        provider: str,
        model: str,
        dimension: int,
        limit: int = 4000,
        identity_ids: Sequence[str] = (),
    ) -> List[Dict[str, Any]]:
        """**先按作用域（会话 + 记录类型的真实人物关联）过滤，再取行**做近邻。

        - `max_candidates` 作为 SQL LIMIT 实际约束运算量，不只在上层截断；
        - 指定人物时的过滤**按真实记录类型**落实（P15-R02，无旁路）：
          * `user`/`assistant`（轮次块，record_id=turn_id）→ 按该轮**全部
            原文参与者**（l2_turn_source → raw_message.sender_identity_id）
            匹配；assistant 视图继承所属轮次的合法人物/来源范围；
          * `L3`（record_id=fact_id）→ 按 l3_fact.identity_id（subject）；
          * `L4`（record_id=memory_id）→ 按 l4_memory.subject_identity_id
            （明确 subject），或其来源原文的参与者
            （l4_memory_source[source_type='raw_message'] → raw_message）；
          * 归属未知/未来新增角色：**不混入**指定人物的命中（被排除）；
        - 未指定人物（普通会话搜索）不加任何人物过滤，不受额外硬拒绝。
        """
        import struct

        ids = [c for c in conversation_ids if c]
        if not ids:
            return []
        placeholders = ",".join("?" for _ in ids)
        sql = (
            "SELECT v.chunk_id, v.conversation_id, v.record_id, v.role,"
            " v.text_sha256, v.dimension, v.vector FROM vector_chunk v"
            " WHERE v.agent_id=? AND v.conversation_id IN (" + placeholders + ")"
            " AND v.provider=? AND v.model=? AND v.dimension=?"
        )
        params: List[Any] = [agent_id, *ids, provider, model, int(dimension)]
        if identity_ids:
            id_ph = ",".join("?" for _ in identity_ids)
            # 每个分支独立展开身份占位；归属未知（无任何分支命中）不混入。
            sql += (
                " AND ("
                # 轮次块：全部原文参与者（assistant 继承同轮人物）
                "  (v.role IN ('user','assistant') AND EXISTS ("
                "     SELECT 1 FROM l2_turn t2"
                "      JOIN l2_turn_source ts ON ts.turn_id = t2.turn_id"
                "      JOIN raw_message r ON r.record_id = ts.record_id"
                "     WHERE t2.turn_id = v.record_id"
                "       AND t2.agent_id = v.agent_id"
                "       AND t2.conversation_id = v.conversation_id"
                f"       AND r.sender_identity_id IN ({id_ph})))"
                # L3：按 subject identity
                f"  OR (v.role = 'L3' AND EXISTS ("
                "     SELECT 1 FROM l3_fact f"
                "     WHERE f.fact_id = v.record_id"
                "       AND f.agent_id = v.agent_id"
                f"       AND f.identity_id IN ({id_ph})))"
                # L4：明确 subject 或其来源原文参与者（archive 摘要经
                # raw_message 来源证据，与 store 层关键词检索同一语义）
                "  OR (v.role = 'L4' AND EXISTS ("
                "     SELECT 1 FROM l4_memory m"
                "     WHERE m.memory_id = v.record_id"
                "       AND m.agent_id = v.agent_id"
                f"       AND (m.subject_identity_id IN ({id_ph})"
                "        OR EXISTS (SELECT 1 FROM l4_memory_source s"
                "          JOIN raw_message r2 ON r2.record_id = s.source_ref"
                "          WHERE s.memory_id = m.memory_id"
                "           AND s.source_type = 'raw_message'"
                f"           AND r2.sender_identity_id IN ({id_ph})))))"
                ")"
            )
            for _ in range(4):
                params.extend(identity_ids)
        sql += " LIMIT ?"
        params.append(max(1, min(int(limit), 8000)))
        with self._lock:
            rows = self._conn.execute(sql, params).fetchall()
        out: List[Dict[str, Any]] = []
        for row in rows:
            vector = list(struct.unpack(f"<{row['dimension']}f", row["vector"]))
            out.append({
                "chunk_id": row["chunk_id"],
                "conversation_id": row["conversation_id"],
                "record_id": row["record_id"],
                "role": row["role"],
                "text_sha256": row["text_sha256"],
                "vector": vector,
            })
        return out

    def vector_stats(self, agent_id: str = "") -> Dict[str, Any]:
        with self._lock:
            if agent_id:
                row = self._conn.execute(
                    "SELECT COUNT(*) AS n,"
                    " COUNT(DISTINCT provider || ':' || model) AS models"
                    " FROM vector_chunk WHERE agent_id=?",
                    (agent_id,),
                ).fetchone()
            else:
                row = self._conn.execute(
                    "SELECT COUNT(*) AS n,"
                    " COUNT(DISTINCT provider || ':' || model) AS models"
                    " FROM vector_chunk"
                ).fetchone()
        return {"chunks": int(row["n"]), "provider_model_pairs": int(row["models"])}

    # ------------------------------------------------------------------ #
    # B06：per-Agent 层计数 + L3 记录查询（记录层闭环）
    # ------------------------------------------------------------------ #

    def layer_counts(self, agent_id: str) -> Dict[str, int]:
        """按 Agent 真实范围计算各层计数（不是全库数）。"""
        out: Dict[str, int] = {}
        with self._lock:
            for layer, table, column in (
                ("raw", "raw_message", "stored_at"),
                ("L2", "l2_turn", "created_at"),
                ("L4", "l4_memory", "created_at"),
                ("journal", "event_journal", "occurred_at"),
            ):
                try:
                    row = self._conn.execute(
                        f"SELECT COUNT(*) FROM {table} WHERE agent_id=?",
                        (agent_id,),
                    ).fetchone()
                    out[layer] = int(row[0])
                except Exception:
                    out[layer] = 0
            try:
                row = self._conn.execute(
                    "SELECT COUNT(*) FROM l20_tool_evidence e"
                    " JOIN l2_turn t ON t.turn_id = e.turn_id WHERE t.agent_id=?",
                    (agent_id,),
                ).fetchone()
                out["L20"] = int(row[0])
            except Exception:
                out["L20"] = 0
            try:
                row = self._conn.execute(
                    "SELECT COUNT(*) FROM l3_fact WHERE agent_id=?",
                    (agent_id,),
                ).fetchone()
                out["L3"] = int(row[0])
            except Exception:
                out["L3"] = 0
            try:
                row = self._conn.execute(
                    "SELECT COUNT(*) FROM vector_chunk WHERE agent_id=?",
                    (agent_id,),
                ).fetchone()
                out["vector"] = int(row[0])
            except Exception:
                out["vector"] = 0
        return out

    def l3_query_records(
        self,
        *,
        agent_id: str,
        conversation_id: Optional[str],
        identity_id: Optional[str],
        since: Optional[str],
        until: Optional[str],
        keyword: Optional[str],
        limit: int,
        offset: int,
        preview_chars: int,
    ) -> Tuple[List[Dict[str, Any]], int]:
        """L3 作为可查询记录层：层/人物/会话/日期/关键词筛选 + 稳定分页。"""
        limit = max(1, min(int(limit), 200))
        with self._lock:
            where = ["agent_id=?"]
            params: List[Any] = [agent_id]
            if conversation_id:
                where.append("conversation_id=?")
                params.append(conversation_id)
            if identity_id:
                where.append("identity_id=?")
                params.append(identity_id)
            if since or until:
                from pero.core.timebase import require_epoch
            if since:
                where.append("bj_epoch(updated_at)>=?")
                params.append(require_epoch(since, "since"))
            if until:
                where.append("bj_epoch(updated_at)<?")
                params.append(require_epoch(until, "until"))
            if keyword:
                where.append("(content LIKE ? OR evidence_quote LIKE ?)")
                like = f"%{keyword}%"
                params.extend([like, like])
            clause = " AND ".join(where)
            rows = self._conn.execute(
                "SELECT fact_id, conversation_id, identity_id, display_name,"
                " content, status, evidence_type, evidence_quote,"
                " source_record_ids, occurred_at, extracted_at, updated_at"
                " FROM l3_fact WHERE " + clause
                + " ORDER BY bj_epoch(updated_at) DESC, fact_id DESC LIMIT ? OFFSET ?",
                [*params, limit, offset],
            ).fetchall()
            total = int(self._conn.execute(
                "SELECT COUNT(*) FROM l3_fact WHERE " + clause, params
            ).fetchone()[0])
        items = [
            {
                "record_id": r["fact_id"],
                "layer": "L3",
                "conversation_id": r["conversation_id"],
                "identity_id": r["identity_id"],
                "occurred_at": r["updated_at"],
                "created_at": r["updated_at"],
                "preview": (r["content"] or "")[: max(1, int(preview_chars))],
                "text_chars": len(r["content"] or ""),
                "status": r["status"],
                "evidence_type": r["evidence_type"],
                "evidence_quote": (r["evidence_quote"] or "")[: max(1, int(preview_chars))],
                "source_record_ids": json.loads(r["source_record_ids"] or "[]"),
                "subject": r["display_name"] or None,
            }
            for r in rows
        ]
        return items, total

    def l3_detail(self, *, agent_id: str, fact_id: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM l3_fact WHERE fact_id=? AND agent_id=?",
                (fact_id, agent_id),
            ).fetchone()
        if row is None:
            return None
        detail = self._l3_row(row, duplicate=False)
        detail.pop("duplicate", None)
        detail["layer"] = "L3"
        return detail


class MemoryQueryErrorP15(RuntimeError):
    """P15 存储入参问题（调用方回 422）。"""
