"""持久记忆的数据契约（P05：原文、身份别名、L2 轮次、L20 工具证据）。

这些是**写入/读取边界**上的普通 dataclass，不依赖具体表名，也不含任何凭据。
表结构在 `schema.py`；SQL 在 `store.py`；对外的只读视图在 `repository.py`。
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from typing import Optional, Sequence, Tuple, Mapping, Any

#: 本阶段实现的持久层级。内存态层：L1（进程背景缓冲，P11 起已实现）、
#: L10（深缓冲，P15 起，默认关闭）；持久层：L3 人物事实（P15 起，默认关闭）。
#: 状态以 /memory/layers 的动态目录为准，不以本注释为准。
LAYER_RAW = "raw"
LAYER_L2 = "L2"
LAYER_L20 = "L20"
#: L3：从已归档交互里提取的人物事实（带逐字证据与来源原文）。
LAYER_L3 = "L3"
#: L4：用户明确要求保存的长期记录（不是模型自由发挥的人物画像）。
LAYER_L4 = "L4"
#: 事件日记：按已收尾轮次确定性生成的短事件索引。
LAYER_JOURNAL = "journal"

#: 对外可查询的记录层（raw 是原文来源存储，不是长期记忆层，但前端需要能看）。
#: P15-R01：L3 是持久人物事实层，进入可查询范围（列表/详情/筛选）。
QUERYABLE_LAYERS: Tuple[str, ...] = (
    LAYER_RAW, LAYER_L2, LAYER_L20, LAYER_L4, LAYER_JOURNAL, "L3",
)

#: L4 记录状态：只有 `saved` 表示事务已提交；其余是本次请求的结论，不落库。
SAVE_SAVED = "saved"
SAVE_NEEDS_CONTENT = "needs_content"
SAVE_SUBJECT_UNRESOLVED = "subject_unresolved"
SAVE_SUBJECT_AMBIGUOUS = "subject_ambiguous"
SAVE_DUPLICATE = "duplicate"
SAVE_FAILED = "failed"
#: 内容超过 `memory.save_max_chars`：明确未保存，不静默截断后报成功。
SAVE_TOO_LONG = "too_long"
#: `remember` 没有可信来源授权：拒绝写入，不把模型参数当用户请求。
SAVE_UNAUTHORIZED = "unauthorized"

#: 检索状态：调用方必须区分"没命中""身份歧义""不可用/失败"。
RETRIEVAL_OK = "ok"
RETRIEVAL_NO_HITS = "no_hits"
RETRIEVAL_IDENTITY_AMBIGUOUS = "identity_ambiguous"
RETRIEVAL_UNAVAILABLE = "unavailable"
RETRIEVAL_FAILED = "failed"

#: 轮次状态：只有 `sent` 才进入"已发言"历史。
#: pending 是写入后、运行/投递完成前的中间态；generated 表示生成了候选但没有对外发送；
#: interrupted 表示这轮被中断/未确认收尾（**不会自动重试**，只作为证据保留）。
TURN_PENDING = "pending"
TURN_SENT = "sent"
TURN_GENERATED = "generated"
TURN_FAILED = "failed"
TURN_UNKNOWN = "unknown"
TURN_RUN_FAILED = "run_failed"
TURN_RUN_LIMIT = "run_limit"
TURN_INTERRUPTED = "interrupted"

TURN_STATUSES: Tuple[str, ...] = (
    TURN_PENDING,
    TURN_SENT,
    TURN_GENERATED,
    TURN_FAILED,
    TURN_UNKNOWN,
    TURN_RUN_FAILED,
    TURN_RUN_LIMIT,
    TURN_INTERRUPTED,
)

#: 已收尾状态：同一原文再次到达时按 duplicate 拒绝，不再进入模型/投递。
TURN_SETTLED_STATUSES: Tuple[str, ...] = tuple(
    status for status in TURN_STATUSES if status != TURN_PENDING
)

#: 历史载入时的状态过滤：pending 视为"未确认"，不进入模型历史（避免半个事实）。
TURN_HISTORY_STATUSES: Tuple[str, ...] = TURN_SETTLED_STATUSES

#: 可信 wxid 证据来源白名单；桥当前不提供 wxid，未知一律保持 unknown。
WXID_TRUSTED_SOURCES: Tuple[str, ...] = ("bridge_extension", "manual_verified")


def new_record_id() -> str:
    return f"m-{uuid.uuid4().hex[:16]}"


def new_turn_id() -> str:
    return f"t-{uuid.uuid4().hex[:16]}"


def new_evidence_id() -> str:
    return f"e-{uuid.uuid4().hex[:16]}"


def new_memory_id() -> str:
    return f"l4-{uuid.uuid4().hex[:16]}"


def new_journal_id() -> str:
    return f"ev-{uuid.uuid4().hex[:16]}"


def stable_identity_id(channel_id: str, account_id: str, bridge_user_id: str) -> str:
    """稳定身份主键：只由通道 + 账号 + 桥 ID 决定，**不含昵称**。

    使用 uuid5 保证进程重启后同一发送者得到同一个 identity_id，
    这样按别名查到的身份与历史记录能对上。
    """

    name = f"pero:identity:{channel_id}:{account_id}:{bridge_user_id}"
    return f"id-{uuid.uuid5(uuid.NAMESPACE_URL, name).hex[:20]}"


@dataclass(frozen=True)
class ConversationRef:
    """会话与来源的稳定标识。"""

    conversation_id: str
    agent_id: str
    channel_id: str
    account_id: str
    conversation_type: str
    target_ref: str


@dataclass(frozen=True)
class InboundRecord:
    """一条入站原文（不含附件二进制，也不含通道原始帧）。"""

    source_event_id: str
    sender_ref: str
    text: str
    display_name: Optional[str] = None
    mentions: Sequence[str] = field(default_factory=tuple)
    received_at: Optional[str] = None
    dedupe_scope: str = "unknown"
    quoted_event_id: Optional[str] = None
    #: 通道在可信扩展字段里给出的原始 wxid；拿不到就是 None（不猜测、不用昵称反推）
    sender_wxid: Optional[str] = None
    attachments: Sequence[Mapping[str,Any]] = field(default_factory=tuple)


@dataclass(frozen=True)
class ToolEvidenceInput:
    """一条受控的工具使用证据（不含结果正文）。"""

    call_id: str
    tool_name: str
    status: str
    duration_ms: int
    started_at: str
    finished_at: str
    summary: str = ""
    error_code: Optional[str] = None
    resource_refs: Sequence[str] = field(default_factory=tuple)
    content_persisted: bool = False
    #: 插件工具受控证据（不保存结果正文）：插件 ID、缓存命中、产物引用。
    plugin_id: Optional[str] = None
    cache_hit: Optional[bool] = None
    artifact_refs: Sequence[str] = field(default_factory=tuple)


@dataclass(frozen=True)
class TurnHandle:
    """一轮的写入结果。

    - `created=True`：本轮由这次调用新建，可以进入模型/投递；
    - `created=False`：同一原文（或其中一部分）已被别的轮次归属，**不得再执行**；
      `status` 是该轮次的既有状态（`pending` → 未确认/中断，其余 → 已收尾）。
    - `skipped_record_ids`：本次被判定为"已归属"的原文，不进本轮 user_text；
    - `user_text`：按**实际归属的原文**重新渲染的正文（已去掉重复来源）。
    - `records`：本轮实际归属的 `(record_id, source_event_id)`，顺序与批次一致；
      调用方据此把原文 ID 映射回真正的入站消息（不靠文本位置猜）。
    """

    turn_id: Optional[str]
    created: bool
    status: str
    record_ids: Tuple[str, ...]
    dedupe_key: str
    skipped_record_ids: Tuple[str, ...] = ()
    user_text: str = ""
    owner_turn_id: Optional[str] = None
    reason: str = ""
    records: Tuple[Tuple[str, str], ...] = ()


@dataclass(frozen=True)
class StoredEvidence:
    """重建历史时附带的受控工具证据（只有摘要，没有结果正文）。"""

    tool_name: str
    status: str
    call_id: str
    run_id: Optional[str]
    summary: str
    duration_ms: int = 0
    resource_refs: Tuple[str, ...] = ()


#: 未产生已发送回复的标注（模型可读文本，不是用户发言）。
MISSING_REPLY_NOTE = "[该轮未产生已发送回复｜状态={status}]"


@dataclass(frozen=True)
class StoredTurn:
    """重建模型历史用的一条轮次。

    `assistant_text` 只有 `sent` 才有值；其它状态仍保留用户原文（用户事实），
    由 `status` 说明该轮为何没有已发送回答。
    """

    turn_id: str
    status: str
    user_text: str
    assistant_text: Optional[str]
    created_at: str
    tool_evidence: Tuple[StoredEvidence, ...] = ()
    interrupted_reason: Optional[str] = None

    @property
    def spoken(self) -> bool:
        return self.status == TURN_SENT and bool(self.assistant_text)

    def render_user_content(self) -> str:
        """该轮进入模型的 user 正文（**唯一实现**：预算与历史重建共用）。

        组成顺序固定为：原文 → 可选证据注记（空行分隔）→ 可选"未产生已发送回复"说明。
        这样存储侧算的字符数与 `_hydrate_history` 真正交给模型的内容逐字符一致，
        不会出现"入库时算一种长度、载入时又多出一段文本"。
        """

        content = self.user_text
        notes = render_evidence_notes(self.tool_evidence)
        if notes:
            content = f"{content}\n\n{notes}"
        if not self.spoken:
            content = f"{content}\n{MISSING_REPLY_NOTE.format(status=self.status)}"
        return content

    @property
    def history_cost(self) -> int:
        """该轮占用的字符预算（与最终入模文本完全一致，含助手正文）。"""

        cost = len(self.render_user_content())
        if self.spoken and self.assistant_text:
            cost += len(self.assistant_text)
        return cost


#: 历史载入结果的受控原因；接口与日志只出现这些常量，不出现正文。
HISTORY_OK = "ok"
HISTORY_NO_TURNS = "no_turns"
HISTORY_NEWEST_OVER_BUDGET = "newest_over_budget"
HISTORY_CHAR_BUDGET = "char_budget"
HISTORY_REASONS: Tuple[str, ...] = (
    HISTORY_OK,
    HISTORY_NO_TURNS,
    HISTORY_NEWEST_OVER_BUDGET,
    HISTORY_CHAR_BUDGET,
)

@dataclass(frozen=True)
class HistoryLoad:
    """一次历史重建的结果：轮次 + 受控原因 + 实际占用（不强制加载超限内容）。"""

    turns: Tuple[StoredTurn, ...] = ()
    reason: str = HISTORY_NO_TURNS
    budget_chars: int = 0
    used_chars: int = 0
    dropped_turns: int = 0

    @property
    def empty(self) -> bool:
        return not self.turns

    @property
    def over_budget(self) -> bool:
        """最新一轮就超预算：此时宁可给空历史，也不塞进超限内容。"""

        return self.reason == HISTORY_NEWEST_OVER_BUDGET


@dataclass(frozen=True)
class ExplicitSaveRequest:
    """一次"明确保存"的输入：原文请求 + 可核对内容 + 来源。

    只有 `content` 非空才允许落库；`needs_content` 的请求**不编造事实**、也不留空记录。
    """

    request_text: str
    content: str
    record_ids: Tuple[str, ...] = ()
    turn_id: Optional[str] = None
    subject_identity_id: Optional[str] = None
    subject_label: Optional[str] = None
    occurred_at: Optional[str] = None
    #: 触发来源：`user_text`（确定性识别）或 `tool`（模型主动调用）。
    trigger: str = "user_text"
    #: 请求级幂等槽位（例如 `<record_id>:0`）。给出时幂等键只看槽位，**不看内容**：
    #: 同一请求改写内容不能新建第二份记录，也不能绕过对象歧义。
    request_slot: str = ""


@dataclass(frozen=True)
class SaveOutcome:
    """保存结果：`memory_id` 只在事务提交后才有值，其余状态明确说明为何没保存。"""

    status: str
    memory_id: Optional[str] = None
    content: str = ""
    subject_identity_id: Optional[str] = None
    subject_label: Optional[str] = None
    record_ids: Tuple[str, ...] = ()
    turn_id: Optional[str] = None
    created: bool = False
    detail: str = ""

    @property
    def saved(self) -> bool:
        return self.status in {SAVE_SAVED, SAVE_DUPLICATE} and bool(self.memory_id)


@dataclass(frozen=True)
class RetrievalHit:
    """检索命中的一条：带层、记录 ID、时间、来源引用与有界预览。"""

    layer: str
    record_id: str
    occurred_at: str
    preview: str
    score: int = 0
    source_refs: Tuple[str, ...] = ()
    subject_label: Optional[str] = None


@dataclass(frozen=True)
class RetrievalResult:
    """一次检索的结果：状态必须能区分没命中 / 身份歧义 / 不可用 / 失败。

    身份歧义时 `hits` 仍可返回（不按某个随机候选过滤），并用 `detail` 说明
    "未按单人过滤"，避免把歧义包装成"查无此事"。
    """

    state: str
    query: str
    hits: Tuple[RetrievalHit, ...] = ()
    detail: str = ""
    identity_id: Optional[str] = None
    ambiguous_candidates: Tuple[str, ...] = ()

    @property
    def has_hits(self) -> bool:
        return bool(self.hits)


def render_source_line(display_name: Optional[str], sender_ref: str, text: str) -> str:
    """合并正文的单行渲染：`「显示名」：正文`（单条也带来源，写入与加载共用）。"""

    label = (display_name or "").strip() or sender_ref
    return f"「{label}」：{(text or '').strip()}"


#: 历史里的工具证据注记前缀：明确标注"非用户发言"。
EVIDENCE_NOTE_HEADER = "[工具使用记录｜非用户发言]"


def render_evidence_notes(evidence: Sequence[StoredEvidence]) -> str:
    """把受控证据渲染成普通文本注记（不伪造 tool 协议消息，不含结果正文）。"""

    if not evidence:
        return ""
    lines = [EVIDENCE_NOTE_HEADER]
    for item in evidence:
        outcome = "成功" if item.status == "ok" else "失败"
        detail = f"- {item.tool_name}：{outcome}，{int(item.duration_ms)}ms"
        origin = f"{item.call_id or '-'}"
        if item.run_id:
            origin = f"{origin} / {item.run_id}"
        detail += f"，来源 {origin}"
        if item.summary:
            detail += f"；摘要：{item.summary}"
        if item.resource_refs:
            detail += "；资源：" + "、".join(item.resource_refs)
        lines.append(detail)
    return "\n".join(lines)


#: 保存凭据注记：模型可见、标明不是用户发言。
SAVE_RECEIPT_HEADER = "[保存凭据｜系统注记，非用户发言]"
#: 检索参考注记：模型可见、标明不是用户发言。
RETRIEVAL_HEADER = "[检索参考｜系统注记，非用户发言]"


def render_save_receipt(outcome: SaveOutcome) -> str:
    """把保存结果渲染成模型可见的凭据（成功带真实记录 ID，失败带明确原因）。"""

    lines = [SAVE_RECEIPT_HEADER]
    if outcome.saved:
        lines.append(f"- 记录 ID：{outcome.memory_id}")
        lines.append(f"- 状态：{outcome.status}")
        lines.append(f"- 内容：{outcome.content}")
        if outcome.subject_label or outcome.subject_identity_id:
            lines.append(
                "- 对象："
                + (outcome.subject_label or "")
                + (f"（{outcome.subject_identity_id}）" if outcome.subject_identity_id else "")
            )
        if outcome.record_ids:
            lines.append("- 来源原文：" + "、".join(outcome.record_ids))
        if outcome.turn_id:
            lines.append(f"- 轮次：{outcome.turn_id}")
        lines.append("- 说明：记录已提交，可用记录 ID 回溯来源原文。")
    else:
        lines.append(f"- 状态：{outcome.status}")
        if outcome.detail:
            lines.append(f"- 说明：{outcome.detail}")
        lines.append("- 说明：本次**没有**产生长期记录，不要声称已记住。")
    return "\n".join(lines)


def render_retrieval_context(
    result: RetrievalResult, *, item_chars: int, total_chars: int
) -> str:
    """把检索结果渲染成独立、标明来源的参考上下文（有界，不改写用户原话）。"""

    lines = [RETRIEVAL_HEADER, f"- 状态：{result.state}"]
    if result.query:
        lines.append(f"- 关键词：{result.query}")
    if result.detail:
        lines.append(f"- 说明：{result.detail}")
    if not result.hits:
        lines.append("- 结果：本次没有任何命中（不等于用户从未说过）。")
        return "\n".join(lines)

    used = len("\n".join(lines))
    for item in result.hits:
        preview = item.preview
        if len(preview) > item_chars:
            preview = preview[:item_chars] + "…"
        line = f"- [{item.layer}] {item.occurred_at}｜{item.record_id}｜{preview}"
        if item.source_refs:
            line += "｜来源：" + "、".join(item.source_refs)
        if used + len(line) + 1 > total_chars:
            lines.append("- 结果：已达检索上下文预算，剩余命中未展示。")
            break
        lines.append(line)
        used += len(line) + 1
    return "\n".join(lines)


__all__ = [
    "ConversationRef",
    "EVIDENCE_NOTE_HEADER",
    "ExplicitSaveRequest",
    "HISTORY_CHAR_BUDGET",
    "HISTORY_NEWEST_OVER_BUDGET",
    "HISTORY_NO_TURNS",
    "HISTORY_OK",
    "HISTORY_REASONS",
    "HistoryLoad",
    "InboundRecord",
    "LAYER_JOURNAL",
    "LAYER_L2",
    "LAYER_L20",
    "LAYER_L3",
    "LAYER_L4",
    "LAYER_RAW",
    "MISSING_REPLY_NOTE",
    "QUERYABLE_LAYERS",
    "RETRIEVAL_FAILED",
    "RETRIEVAL_HEADER",
    "RETRIEVAL_IDENTITY_AMBIGUOUS",
    "RETRIEVAL_NO_HITS",
    "RETRIEVAL_OK",
    "RETRIEVAL_UNAVAILABLE",
    "RetrievalHit",
    "RetrievalResult",
    "SAVE_DUPLICATE",
    "SAVE_FAILED",
    "SAVE_NEEDS_CONTENT",
    "SAVE_TOO_LONG",
    "SAVE_UNAUTHORIZED",
    "SAVE_RECEIPT_HEADER",
    "SAVE_SAVED",
    "SAVE_SUBJECT_AMBIGUOUS",
    "SAVE_SUBJECT_UNRESOLVED",
    "SaveOutcome",
    "StoredEvidence",
    "StoredTurn",
    "TURN_FAILED",
    "TURN_GENERATED",
    "TURN_HISTORY_STATUSES",
    "TURN_INTERRUPTED",
    "TURN_PENDING",
    "TURN_RUN_FAILED",
    "TURN_RUN_LIMIT",
    "TURN_SENT",
    "TURN_SETTLED_STATUSES",
    "TURN_STATUSES",
    "TURN_UNKNOWN",
    "ToolEvidenceInput",
    "TurnHandle",
    "WXID_TRUSTED_SOURCES",
    "new_evidence_id",
    "new_journal_id",
    "new_memory_id",
    "new_record_id",
    "new_turn_id",
    "render_evidence_notes",
    "render_retrieval_context",
    "render_save_receipt",
    "render_source_line",
    "stable_identity_id",
]
