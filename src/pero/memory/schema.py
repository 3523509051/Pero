"""SQLite schema（当前版本 6）与迁移入口。

规则：
- 版本写在 `PRAGMA user_version`；已知版本之外的库拒绝打开，不做静默降级。
- 初始化/升级在**一个事务**里完成：要么整套表 + 版本号一起成立，要么什么都不留下。
- 只保存受控字段：原文、身份/别名、轮次与工具摘要、明确保存记录与事件日记；
  不存工具正文、网页/文件原文、base64、Key、Authorization、思维链。
"""

from __future__ import annotations

import sqlite3

SCHEMA_VERSION = 12

_DDL_V10 = """
ALTER TABLE activation_token ADD COLUMN agent_id TEXT;
ALTER TABLE activation_token ADD COLUMN request_id TEXT;
CREATE UNIQUE INDEX IF NOT EXISTS activation_request_unique ON activation_token(request_id) WHERE request_id IS NOT NULL;
CREATE TABLE IF NOT EXISTS private_access_state (
 singleton INTEGER PRIMARY KEY CHECK(singleton=1),
 revision INTEGER NOT NULL DEFAULT 1,
 policy_json TEXT NOT NULL DEFAULT '{}'
);
INSERT OR IGNORE INTO private_access_state(singleton,revision,policy_json) VALUES(1,1,'{}');
"""

_DDL_V8 = """
CREATE TABLE IF NOT EXISTS memory_event_history (
 history_id TEXT PRIMARY KEY,event_id TEXT NOT NULL,agent_id TEXT NOT NULL,
 conversation_id TEXT NOT NULL,previous_json TEXT NOT NULL,changed_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS memory_event_history_scope ON memory_event_history(agent_id,conversation_id,event_id,changed_at);
"""

#: v8 → v9：私聊授权系统（一次性开通码、持久授权、主人身份、跨群访问策略）。
_DDL_V9 = """
CREATE TABLE IF NOT EXISTS activation_token (
    token_id TEXT PRIMARY KEY,
    token_hash TEXT NOT NULL UNIQUE,
    channel_id TEXT NOT NULL,
    account_id TEXT NOT NULL,
    label TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    used_by_identity TEXT,
    used_at TEXT,
    revoked INTEGER NOT NULL DEFAULT 0,
    revoked_at TEXT
);
CREATE INDEX IF NOT EXISTS activation_token_lookup ON activation_token(token_hash);
CREATE INDEX IF NOT EXISTS activation_token_channel ON activation_token(channel_id, account_id, created_at);

CREATE TABLE IF NOT EXISTS private_chat_grant (
    grant_id TEXT PRIMARY KEY,
    channel_id TEXT NOT NULL,
    account_id TEXT NOT NULL,
    user_ref TEXT NOT NULL,
    user_wxid TEXT,
    identity_id TEXT REFERENCES identity(identity_id),
    agent_id TEXT NOT NULL,
    conversation_id TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'active',
    granted_via TEXT NOT NULL,
    token_id TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    status_revision INTEGER NOT NULL DEFAULT 1,
    UNIQUE (channel_id, account_id, user_ref)
);
CREATE INDEX IF NOT EXISTS private_grant_conversation ON private_chat_grant(conversation_id, status);
CREATE INDEX IF NOT EXISTS private_grant_identity ON private_chat_grant(identity_id, status);

CREATE TABLE IF NOT EXISTS instance_owner (
    owner_id TEXT PRIMARY KEY DEFAULT 'singleton',
    channel_id TEXT NOT NULL,
    account_id TEXT NOT NULL,
    user_ref TEXT NOT NULL,
    user_wxid TEXT,
    identity_id TEXT REFERENCES identity(identity_id),
    grant_id TEXT NOT NULL REFERENCES private_chat_grant(grant_id),
    display_name TEXT,
    bound_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    CHECK (owner_id = 'singleton')
);

CREATE TABLE IF NOT EXISTS owner_group_access (
    access_id INTEGER PRIMARY KEY AUTOINCREMENT,
    conversation_id TEXT NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 1,
    granted_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    access_revision INTEGER NOT NULL DEFAULT 1,
    UNIQUE (conversation_id)
);
CREATE INDEX IF NOT EXISTS owner_access_enabled ON owner_group_access(enabled, conversation_id);

CREATE TABLE IF NOT EXISTS access_audit_log (
    log_id TEXT PRIMARY KEY,
    operation TEXT NOT NULL,
    actor_type TEXT NOT NULL,
    actor_ref TEXT NOT NULL DEFAULT '',
    target_type TEXT NOT NULL,
    target_id TEXT NOT NULL,
    result TEXT NOT NULL,
    error_code TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS access_audit_time ON access_audit_log(created_at);
CREATE INDEX IF NOT EXISTS access_audit_target ON access_audit_log(target_type, target_id);
"""

_DDL_V1 = """
CREATE TABLE IF NOT EXISTS conversation (
    conversation_id   TEXT PRIMARY KEY,
    agent_id          TEXT NOT NULL,
    channel_id        TEXT NOT NULL,
    account_id        TEXT NOT NULL,
    conversation_type TEXT NOT NULL,
    target_ref        TEXT NOT NULL,
    created_at        TEXT NOT NULL,
    updated_at        TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS identity (
    identity_id    TEXT PRIMARY KEY,
    channel_id     TEXT NOT NULL,
    account_id     TEXT NOT NULL,
    bridge_user_id TEXT NOT NULL,
    wxid           TEXT,
    wxid_source    TEXT NOT NULL DEFAULT 'unknown',
    created_at     TEXT NOT NULL,
    updated_at     TEXT NOT NULL,
    UNIQUE (channel_id, account_id, bridge_user_id)
);

CREATE TABLE IF NOT EXISTS identity_alias (
    alias_id    INTEGER PRIMARY KEY AUTOINCREMENT,
    identity_id TEXT NOT NULL REFERENCES identity(identity_id) ON DELETE CASCADE,
    alias       TEXT NOT NULL,
    alias_type  TEXT NOT NULL,
    scope_type  TEXT NOT NULL,
    scope_ref   TEXT NOT NULL,
    source      TEXT NOT NULL,
    observed_at TEXT NOT NULL,
    UNIQUE (identity_id, alias, alias_type, scope_type, scope_ref)
);

CREATE INDEX IF NOT EXISTS identity_alias_lookup
    ON identity_alias(alias COLLATE NOCASE, scope_type, scope_ref);

CREATE TABLE IF NOT EXISTS identity_wxid_evidence (
    evidence_id  INTEGER PRIMARY KEY AUTOINCREMENT,
    identity_id  TEXT NOT NULL REFERENCES identity(identity_id) ON DELETE CASCADE,
    wxid         TEXT NOT NULL,
    source       TEXT NOT NULL,
    evidence_ref TEXT,
    observed_at  TEXT NOT NULL,
    UNIQUE (identity_id, wxid, source, evidence_ref)
);

CREATE TABLE IF NOT EXISTS raw_message (
    record_id          TEXT PRIMARY KEY,
    agent_id           TEXT NOT NULL,
    channel_id         TEXT NOT NULL,
    account_id         TEXT NOT NULL,
    conversation_id    TEXT NOT NULL,
    conversation_type  TEXT NOT NULL,
    target_ref         TEXT NOT NULL,
    sender_ref         TEXT NOT NULL,
    sender_identity_id TEXT REFERENCES identity(identity_id),
    display_name       TEXT,
    text               TEXT NOT NULL,
    text_chars         INTEGER NOT NULL,
    mentions_json      TEXT NOT NULL DEFAULT '[]',
    quoted_event_id    TEXT,
    received_at        TEXT,
    stored_at          TEXT NOT NULL,
    dedupe_scope       TEXT NOT NULL,
    source_event_id    TEXT NOT NULL,
    UNIQUE (channel_id, account_id, source_event_id)
);

CREATE INDEX IF NOT EXISTS raw_message_scope
    ON raw_message(agent_id, conversation_id, stored_at);
CREATE INDEX IF NOT EXISTS raw_message_sender
    ON raw_message(agent_id, sender_identity_id, stored_at);

CREATE TABLE IF NOT EXISTS l2_turn (
    turn_id            TEXT PRIMARY KEY,
    agent_id           TEXT NOT NULL,
    conversation_id    TEXT NOT NULL,
    run_id             TEXT,
    status             TEXT NOT NULL,
    user_text          TEXT NOT NULL,
    user_chars         INTEGER NOT NULL,
    assistant_text     TEXT,
    sender_identity_id TEXT,
    sender_ref         TEXT NOT NULL,
    dedupe_key         TEXT NOT NULL,
    created_at         TEXT NOT NULL,
    updated_at         TEXT NOT NULL,
    UNIQUE (agent_id, dedupe_key)
);

CREATE INDEX IF NOT EXISTS l2_turn_scope
    ON l2_turn(agent_id, conversation_id, created_at);
CREATE INDEX IF NOT EXISTS l2_turn_status
    ON l2_turn(agent_id, status, created_at);

CREATE TABLE IF NOT EXISTS l2_turn_source (
    turn_id   TEXT NOT NULL REFERENCES l2_turn(turn_id) ON DELETE CASCADE,
    record_id TEXT NOT NULL REFERENCES raw_message(record_id) ON DELETE CASCADE,
    position  INTEGER NOT NULL,
    PRIMARY KEY (turn_id, record_id)
);

CREATE TABLE IF NOT EXISTS l2_attachment (
    attachment_id      TEXT PRIMARY KEY,
    turn_id            TEXT NOT NULL REFERENCES l2_turn(turn_id) ON DELETE CASCADE,
    record_id          TEXT,
    kind               TEXT NOT NULL,
    name               TEXT,
    handle             TEXT,
    recognized         INTEGER NOT NULL DEFAULT 0,
    recognition_result TEXT,
    created_at         TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS l2_attachment_turn ON l2_attachment(turn_id);

CREATE TABLE IF NOT EXISTS l20_tool_evidence (
    evidence_id      TEXT PRIMARY KEY,
    turn_id          TEXT NOT NULL REFERENCES l2_turn(turn_id) ON DELETE CASCADE,
    agent_id         TEXT NOT NULL,
    run_id           TEXT,
    call_id          TEXT NOT NULL,
    tool_name        TEXT NOT NULL,
    status           TEXT NOT NULL,
    error_code       TEXT,
    duration_ms      INTEGER NOT NULL DEFAULT 0,
    started_at       TEXT NOT NULL,
    finished_at      TEXT NOT NULL,
    summary          TEXT NOT NULL DEFAULT '',
    summary_truncated INTEGER NOT NULL DEFAULT 0,
    resource_refs_json TEXT NOT NULL DEFAULT '[]',
    content_persisted  INTEGER NOT NULL DEFAULT 0,
    created_at       TEXT NOT NULL,
    UNIQUE (turn_id, call_id)
);

CREATE INDEX IF NOT EXISTS l20_tool_turn ON l20_tool_evidence(turn_id);
CREATE INDEX IF NOT EXISTS l20_tool_agent
    ON l20_tool_evidence(agent_id, created_at);

CREATE TABLE IF NOT EXISTS delivery_record (
    turn_id     TEXT PRIMARY KEY REFERENCES l2_turn(turn_id) ON DELETE CASCADE,
    agent_id    TEXT NOT NULL,
    run_id      TEXT,
    status      TEXT NOT NULL,
    action_id   TEXT,
    evidence    TEXT NOT NULL DEFAULT '',
    text        TEXT,
    text_chars  INTEGER,
    created_at  TEXT NOT NULL
);
"""


#: v10 → v11：插件工具 L20 证据（只记录状态/命中/资源引用，不落正文）。
_DDL_V11 = (
    "ALTER TABLE l20_tool_evidence ADD COLUMN plugin_id TEXT",
    "ALTER TABLE l20_tool_evidence ADD COLUMN cache_hit INTEGER",
    "ALTER TABLE l20_tool_evidence ADD COLUMN artifact_refs_json TEXT NOT NULL DEFAULT '[]'",
)

_L20_PLUGIN_COLUMNS = ("plugin_id", "cache_hit", "artifact_refs_json")


def _ensure_l20_plugin_columns(conn: sqlite3.Connection) -> None:
    existing = {row[1] for row in conn.execute("PRAGMA table_info(l20_tool_evidence)")}
    for column, statement in zip(_L20_PLUGIN_COLUMNS, _DDL_V11):
        if column not in existing:
            conn.execute(statement)


#: v1 → v2：按原文反查归属（同源防重）的索引 + 中断原因留痕。
_DDL_V2 = """
CREATE INDEX IF NOT EXISTS l2_turn_source_record ON l2_turn_source(record_id);
ALTER TABLE l2_turn ADD COLUMN interrupt_reason TEXT;
"""


#: v2 → v3：L4 明确保存记录（含来源）与事件日记（一轮一条、幂等）。
_DDL_V3 = """
CREATE TABLE IF NOT EXISTS l4_memory (
    memory_id             TEXT PRIMARY KEY,
    agent_id              TEXT NOT NULL,
    conversation_id       TEXT NOT NULL,
    channel_id            TEXT NOT NULL,
    account_id            TEXT NOT NULL,
    kind                  TEXT NOT NULL,
    status                TEXT NOT NULL,
    content               TEXT NOT NULL,
    content_chars         INTEGER NOT NULL,
    subject_identity_id   TEXT,
    subject_label         TEXT,
    request_text          TEXT NOT NULL,
    request_record_id     TEXT,
    turn_id               TEXT,
    dedupe_key            TEXT NOT NULL,
    occurred_at           TEXT NOT NULL,
    saved_at              TEXT NOT NULL,
    updated_at            TEXT NOT NULL,
    UNIQUE (agent_id, dedupe_key)
);

CREATE INDEX IF NOT EXISTS l4_memory_scope
    ON l4_memory(agent_id, conversation_id, saved_at);
CREATE INDEX IF NOT EXISTS l4_memory_subject
    ON l4_memory(agent_id, subject_identity_id, saved_at);

CREATE TABLE IF NOT EXISTS l4_memory_source (
    memory_id   TEXT NOT NULL REFERENCES l4_memory(memory_id) ON DELETE CASCADE,
    source_type TEXT NOT NULL,
    source_ref  TEXT NOT NULL,
    position    INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (memory_id, source_type, source_ref)
);

CREATE TABLE IF NOT EXISTS event_journal (
    entry_id              TEXT PRIMARY KEY,
    agent_id              TEXT NOT NULL,
    conversation_id       TEXT NOT NULL,
    turn_id               TEXT NOT NULL,
    kind                  TEXT NOT NULL,
    status                TEXT NOT NULL,
    summary               TEXT NOT NULL,
    summary_chars         INTEGER NOT NULL,
    participants_json     TEXT NOT NULL DEFAULT '[]',
    source_record_ids_json TEXT NOT NULL DEFAULT '[]',
    has_assistant         INTEGER NOT NULL DEFAULT 0,
    occurred_at           TEXT NOT NULL,
    created_at            TEXT NOT NULL,
    UNIQUE (turn_id)
);

CREATE INDEX IF NOT EXISTS event_journal_scope
    ON event_journal(agent_id, conversation_id, occurred_at);
CREATE INDEX IF NOT EXISTS event_journal_status
    ON event_journal(agent_id, status, occurred_at);
"""


_DDL_V4 = """
CREATE TABLE IF NOT EXISTS archive_task (
 task_id TEXT PRIMARY KEY, agent_id TEXT NOT NULL, conversation_id TEXT NOT NULL,
 state TEXT NOT NULL, error_code TEXT NOT NULL DEFAULT '', fingerprint TEXT NOT NULL,
 input_text TEXT NOT NULL, source_turn_ids TEXT NOT NULL, memory_id TEXT,
 model TEXT NOT NULL DEFAULT '', purpose TEXT NOT NULL DEFAULT 'conversation_summary',
 version INTEGER NOT NULL DEFAULT 1, from_time TEXT NOT NULL, to_time TEXT NOT NULL,
 created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
 UNIQUE(agent_id,conversation_id,fingerprint)
);
CREATE INDEX IF NOT EXISTS archive_task_scope ON archive_task(agent_id,conversation_id,state,created_at);
CREATE TABLE IF NOT EXISTS archive_task_source (
 agent_id TEXT NOT NULL, turn_id TEXT NOT NULL REFERENCES l2_turn(turn_id),
 task_id TEXT NOT NULL REFERENCES archive_task(task_id), position INTEGER NOT NULL,
 PRIMARY KEY(agent_id,turn_id), UNIQUE(task_id,position)
);
"""


# P15：持久通用会话目录、L3 人物事实、向量索引（SQLite 而非独立进程服务）。
_DDL_V5 = """
CREATE TABLE IF NOT EXISTS agent_session (
 session_id TEXT NOT NULL,
 agent_id TEXT NOT NULL,
 channel_id TEXT NOT NULL,
 account_id TEXT NOT NULL,
 conversation_id TEXT NOT NULL,
 conversation_type TEXT NOT NULL,
 target_ref TEXT NOT NULL,
 display_name TEXT,
 created_at TEXT NOT NULL,
 updated_at TEXT NOT NULL,
 run_count INTEGER NOT NULL DEFAULT 0,
 last_run_id TEXT,
 last_status TEXT,
 last_error_code TEXT,
 PRIMARY KEY (session_id, agent_id)
);
CREATE INDEX IF NOT EXISTS agent_session_dir
 ON agent_session(agent_id, updated_at DESC);

CREATE TABLE IF NOT EXISTS l3_fact (
 fact_id TEXT PRIMARY KEY,
 agent_id TEXT NOT NULL,
 conversation_id TEXT NOT NULL,
 identity_id TEXT NOT NULL,
 display_name TEXT,
 content TEXT NOT NULL,
 content_hash TEXT NOT NULL,
 status TEXT NOT NULL,
 evidence_type TEXT NOT NULL,
 evidence_quote TEXT,
 source_record_ids TEXT NOT NULL,
 occurred_at TEXT,
 extracted_at TEXT NOT NULL,
 updated_at TEXT NOT NULL,
 -- P15-R01：会话在唯一键内——同事实跨群是各自会话的独立记录，不共享一行。
 UNIQUE (agent_id, conversation_id, identity_id, content_hash)
);
CREATE INDEX IF NOT EXISTS l3_fact_scope
 ON l3_fact(agent_id, conversation_id, identity_id, updated_at DESC);

CREATE TABLE IF NOT EXISTS vector_chunk (
 chunk_id TEXT PRIMARY KEY,
 agent_id TEXT NOT NULL,
 conversation_id TEXT NOT NULL,
 record_id TEXT NOT NULL,
 role TEXT NOT NULL,
 text_version TEXT NOT NULL,
 text_sha256 TEXT NOT NULL,
 provider TEXT NOT NULL,
 model TEXT NOT NULL,
 dimension INTEGER NOT NULL,
 vector BLOB NOT NULL,
 created_at TEXT NOT NULL,
 UNIQUE (record_id, role, provider, model)
);
CREATE INDEX IF NOT EXISTS vector_chunk_scope
 ON vector_chunk(agent_id, conversation_id);
"""


#: v6：规范聊天流水（群友 + 已确认机器人发言）、会话序号、归档批次、
#: 事件条目与 L3 候选列。旧表保留：raw_message/l2_turn 仍是轮次与工具证据来源，
#: chat_event 是“一条真实群消息 = 一条”的规范视图。
_DDL_V6 = """
CREATE TABLE IF NOT EXISTS chat_event (
    event_id           TEXT PRIMARY KEY,
    agent_id           TEXT NOT NULL,
    conversation_id    TEXT NOT NULL,
    seq                INTEGER NOT NULL,
    role               TEXT NOT NULL,
    identity_id        TEXT,
    sender_ref         TEXT NOT NULL,
    display_name       TEXT,
    kind               TEXT NOT NULL,
    text               TEXT NOT NULL,
    text_chars         INTEGER NOT NULL,
    occurred_at        TEXT NOT NULL,
    stored_at          TEXT NOT NULL,
    source_event_id    TEXT NOT NULL DEFAULT '',
    source_record_id   TEXT,
    turn_id            TEXT,
    delivery_action_id TEXT,
    quote_event_id     TEXT,
    media_json         TEXT NOT NULL DEFAULT '[]',
    dedupe_key         TEXT NOT NULL,
    UNIQUE (agent_id, conversation_id, seq),
    UNIQUE (agent_id, dedupe_key)
);
CREATE INDEX IF NOT EXISTS chat_event_window
    ON chat_event(agent_id, conversation_id, seq DESC);
CREATE INDEX IF NOT EXISTS chat_event_role
    ON chat_event(agent_id, conversation_id, role, seq DESC);

CREATE TABLE IF NOT EXISTS chat_cursor (
    agent_id        TEXT NOT NULL,
    conversation_id TEXT NOT NULL,
    kind            TEXT NOT NULL,
    last_seq        INTEGER NOT NULL DEFAULT 0,
    updated_at      TEXT NOT NULL,
    PRIMARY KEY (agent_id, conversation_id, kind)
);

CREATE TABLE IF NOT EXISTS chat_archive (
    archive_id      TEXT PRIMARY KEY,
    agent_id        TEXT NOT NULL,
    conversation_id TEXT NOT NULL,
    from_seq        INTEGER NOT NULL,
    to_seq          INTEGER NOT NULL,
    message_count   INTEGER NOT NULL,
    state           TEXT NOT NULL,
    error_code      TEXT NOT NULL DEFAULT '',
    memory_id       TEXT,
    summary_chars   INTEGER NOT NULL DEFAULT 0,
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL,
    UNIQUE (agent_id, conversation_id, from_seq, to_seq)
);
CREATE INDEX IF NOT EXISTS chat_archive_scope
    ON chat_archive(agent_id, conversation_id, state, from_seq);

CREATE TABLE IF NOT EXISTS l3_fact_history (
    history_id   TEXT PRIMARY KEY,
    fact_id      TEXT NOT NULL,
    agent_id     TEXT NOT NULL,
    conversation_id TEXT NOT NULL,
    from_status  TEXT NOT NULL,
    to_status    TEXT NOT NULL,
    reason       TEXT NOT NULL DEFAULT '',
    replaced_by  TEXT,
    changed_at   TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS l3_fact_history_scope
    ON l3_fact_history(agent_id, conversation_id, fact_id, changed_at);

CREATE TABLE IF NOT EXISTS memory_event (
    event_id          TEXT PRIMARY KEY,
    agent_id          TEXT NOT NULL,
    conversation_id   TEXT NOT NULL,
    kind              TEXT NOT NULL,
    status            TEXT NOT NULL,
    title             TEXT NOT NULL,
    detail            TEXT NOT NULL,
    participants_json TEXT NOT NULL DEFAULT '[]',
    source_seqs_json  TEXT NOT NULL DEFAULT '[]',
    occurred_at       TEXT,
    planned_at        TEXT,
    created_at        TEXT NOT NULL,
    updated_at        TEXT NOT NULL,
    dedupe_key        TEXT NOT NULL,
    UNIQUE (agent_id, conversation_id, dedupe_key)
);
CREATE INDEX IF NOT EXISTS memory_event_scope
    ON memory_event(agent_id, conversation_id, occurred_at DESC);
"""

#: v7：后台任务状态（失败次数、退避、手动重试）；范围由游标界定。
_DDL_V7 = """
CREATE TABLE IF NOT EXISTS chat_task (
    task_id          TEXT PRIMARY KEY,
    agent_id         TEXT NOT NULL,
    conversation_id  TEXT NOT NULL,
    kind             TEXT NOT NULL,
    cursor_seq       INTEGER NOT NULL DEFAULT 0,
    attempts         INTEGER NOT NULL DEFAULT 0,
    max_attempts     INTEGER NOT NULL DEFAULT 5,
    state            TEXT NOT NULL DEFAULT 'pending',
    error_code       TEXT NOT NULL DEFAULT '',
    next_allowed_at  TEXT,
    manual_required  INTEGER NOT NULL DEFAULT 0,
    updated_at       TEXT NOT NULL,
    UNIQUE (agent_id, conversation_id, kind)
);
CREATE INDEX IF NOT EXISTS chat_task_pending
    ON chat_task(state, manual_required, next_allowed_at);
"""

#: v6 新增的 L3 列（旧库用 ALTER；SQLite 不支持 ADD COLUMN IF NOT EXISTS）。
_V6_L3_COLUMNS = (
    ("record_mode", "ALTER TABLE l3_fact ADD COLUMN record_mode TEXT NOT NULL DEFAULT 'explicit'"),
    ("valid_until", "ALTER TABLE l3_fact ADD COLUMN valid_until TEXT"),
    ("replaces_fact_id", "ALTER TABLE l3_fact ADD COLUMN replaces_fact_id TEXT"),
    ("source_seqs_json", "ALTER TABLE l3_fact ADD COLUMN source_seqs_json TEXT NOT NULL DEFAULT '[]'"),
)

_V6_ARCHIVE_COLUMNS = (
    ("attempts", "ALTER TABLE chat_archive ADD COLUMN attempts INTEGER NOT NULL DEFAULT 0"),
)


def _ensure_v6_columns(conn: sqlite3.Connection) -> None:
    columns = {row[1] for row in conn.execute("PRAGMA table_info(l3_fact)")}
    for name, ddl in _V6_L3_COLUMNS:
        if name not in columns:
            conn.execute(ddl)
    archive_columns = {row[1] for row in conn.execute("PRAGMA table_info(chat_archive)")}
    for name, ddl in _V6_ARCHIVE_COLUMNS:
        if name not in archive_columns:
            conn.execute(ddl)


#: v12：**独立交互记忆（L2）**。
#:
#: 与 `chat_event`（原始聊天流水，含全部闲聊）分开：`l2_interaction` 只索引
#: "机器人实际参与"的交互——引用真实 `chat_event.event_id`，不复制原文。
#: 归档覆盖由 `state/archive_id` 与 `l2_archive` 区间表达，**不用 seq 差值**计数
#: （原始 seq 里混着闲聊，差值不等于交互条数）。
_DDL_V12 = """
CREATE TABLE IF NOT EXISTS l2_interaction (
    interaction_id  TEXT PRIMARY KEY,
    agent_id        TEXT NOT NULL,
    conversation_id TEXT NOT NULL,
    event_id        TEXT NOT NULL,
    seq             INTEGER NOT NULL,
    role            TEXT NOT NULL,
    turn_id         TEXT NOT NULL DEFAULT '',
    source_event_id TEXT NOT NULL DEFAULT '',
    state           TEXT NOT NULL DEFAULT 'active',
    archive_id      TEXT NOT NULL DEFAULT '',
    occurred_at     TEXT NOT NULL,
    created_at      TEXT NOT NULL,
    UNIQUE (agent_id, event_id)
);
CREATE INDEX IF NOT EXISTS l2_interaction_window
    ON l2_interaction(agent_id, conversation_id, state, seq);
CREATE INDEX IF NOT EXISTS l2_interaction_archive
    ON l2_interaction(agent_id, conversation_id, archive_id, seq);

CREATE TABLE IF NOT EXISTS l2_archive (
    archive_id        TEXT PRIMARY KEY,
    agent_id          TEXT NOT NULL,
    conversation_id   TEXT NOT NULL,
    from_seq          INTEGER NOT NULL,
    to_seq            INTEGER NOT NULL,
    interaction_count INTEGER NOT NULL,
    state             TEXT NOT NULL,
    error_code        TEXT NOT NULL DEFAULT '',
    memory_id         TEXT NOT NULL DEFAULT '',
    summary_chars     INTEGER NOT NULL DEFAULT 0,
    attempts          INTEGER NOT NULL DEFAULT 0,
    created_at        TEXT NOT NULL,
    updated_at        TEXT NOT NULL,
    UNIQUE (agent_id, conversation_id, from_seq, to_seq)
);
CREATE INDEX IF NOT EXISTS l2_archive_scope
    ON l2_archive(agent_id, conversation_id, state, from_seq);

CREATE TABLE IF NOT EXISTS l2_config (
    agent_id        TEXT NOT NULL,
    conversation_id TEXT NOT NULL,
    revision        INTEGER NOT NULL DEFAULT 0,
    trigger_count   INTEGER,
    retain_count    INTEGER,
    enabled         INTEGER,
    updated_at      TEXT NOT NULL,
    PRIMARY KEY (agent_id, conversation_id)
);
"""

#: v11 → v12 回填：只用**可信**关联建交互索引。
#:
#: 归属规则与新版 `l2_link_member()` 一致，都以**主模型是否真的执行完**为准：
#: - 机器人侧：`chat_event.role='assistant'` 只在"已确认送达"时写（见
#:   `chat_append_assistant`），因此可直接收录；
#: - 用户侧轮次可能写在 `chat_event.turn_id`，但生产链路 `_collect_chat_events`
#:   不填它、只由 `_link_chat_records` 回填 `source_record_id` —— 统一用
#:   `COALESCE(ce.turn_id, l2_turn_source.turn_id)` 恢复真实轮次；
#: - 轮次状态必须属于"主模型确实执行完"：sent/generated/unknown/failed
#:   （failed = 模型完成后投递失败，参与事实成立）；PASS 中断（interrupted）、
#:   运行失败（run_failed/run_limit）、未收尾（pending）不收录；
#: - `record_id` 全库唯一、`turn_id` 为 uuid，record→turn 映射无跨会话歧义，
#:   交互仍限定在原 chat_event 的 agent/conversation 范围内。
_L2_BACKFILL = """
INSERT OR IGNORE INTO l2_interaction(
    interaction_id, agent_id, conversation_id, event_id, seq, role,
    turn_id, source_event_id, state, archive_id, occurred_at, created_at
)
SELECT 'li-' || ce.event_id, ce.agent_id, ce.conversation_id, ce.event_id, ce.seq,
       CASE WHEN ce.role='assistant' THEN 'assistant' ELSE 'user' END,
       COALESCE(NULLIF(ce.turn_id,''), src.turn_id, ''),
       ce.source_event_id, 'active', '',
       ce.occurred_at, ce.stored_at
FROM chat_event ce
LEFT JOIN l2_turn_source src
       ON ce.role='member' AND src.record_id = ce.source_record_id
LEFT JOIN l2_turn t
       ON t.turn_id = COALESCE(NULLIF(ce.turn_id,''), src.turn_id)
WHERE ce.role = 'assistant'
   OR (
        ce.role = 'member'
        AND COALESCE(NULLIF(ce.turn_id,''), src.turn_id) IS NOT NULL
        AND t.status IN ('sent','generated','unknown','failed')
        AND t.run_id IS NOT NULL AND t.run_id <> ''
        AND NOT EXISTS (
            SELECT 1 FROM delivery_record d
            WHERE d.turn_id=t.turn_id AND d.agent_id=ce.agent_id
              AND d.evidence IN ('silent_collection','pass_marker')
        )
      );
"""


def _backfill_l2_interaction(conn: sqlite3.Connection) -> None:
    """把既有真实轮次/已送达回复迁移成 L2 交互索引（幂等，可重复执行）。"""

    conn.execute(_L2_BACKFILL)


class SchemaVersionError(RuntimeError):
    """数据库版本与代码不一致（通常来自更新的 Pero 或外来库文件）。"""


def current_version(conn: sqlite3.Connection) -> int:
    return int(conn.execute("PRAGMA user_version").fetchone()[0])


def _execute_ddl(conn: sqlite3.Connection, ddl: str) -> None:
    """逐条执行 DDL。

    不用 `executescript`：它会在执行前隐式提交，破坏"建表与版本号同一事务"的保证。
    """

    for statement in ddl.split(";"):
        sql = statement.strip()
        if sql:
            conn.execute(sql)


def migrate(conn: sqlite3.Connection) -> int:
    """把空库/旧库升级到当前版本；返回最终版本号。

    调用方负责提交（`store` 在初始化的同一事务里提交）。
    """

    version = current_version(conn)
    if version == SCHEMA_VERSION:
        return version
    if version > SCHEMA_VERSION:
        raise SchemaVersionError(
            f"数据库 schema 版本 {version} 高于本程序支持的 {SCHEMA_VERSION}；"
            "拒绝打开以免写坏数据"
        )
    if not conn.in_transaction:
        conn.execute("BEGIN IMMEDIATE")
    # 逐级升级：每一级只加自己的表/列，旧数据不删除、不改写。
    if version == 0:
        _execute_ddl(conn, _DDL_V1)
        version = 1
    if version == 1:
        _execute_ddl(conn, _DDL_V2)
        version = 2
    if version == 2:
        _execute_ddl(conn, _DDL_V3)
        version = 3
    if version == 3:
        _execute_ddl(conn, _DDL_V4)
        version = 4
    if version == 4:
        _execute_ddl(conn, _DDL_V5)
        version = 5
    if version == 5:
        _execute_ddl(conn, _DDL_V6)
        _ensure_v6_columns(conn)
        version = 6
    if version == 6:
        _execute_ddl(conn, _DDL_V7)
        version = 7
    if version == 7:
        _execute_ddl(conn,_DDL_V8)
        version=8
    if version == 8:
        _execute_ddl(conn, _DDL_V9)
        version = 9
    if version == 9:
        _execute_ddl(conn, _DDL_V10)
        version = 10
    if version == 10:
        _ensure_l20_plugin_columns(conn)
        version = 11
    if version == 11:
        # 只建新表 + 用既有可信关联回填；不改写旧表、不删旧数据。
        _execute_ddl(conn, _DDL_V12)
        _backfill_l2_interaction(conn)
        version = 12
    if version != SCHEMA_VERSION:
        raise SchemaVersionError(
            f"数据库 schema 版本 {version} 没有对应迁移路径"
            f"（支持 0/1/2/3/4/5/6/7/8/9/10/11 → {SCHEMA_VERSION}）"
        )
    conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
    return SCHEMA_VERSION


__all__ = ["SCHEMA_VERSION", "SchemaVersionError", "current_version", "migrate"]


