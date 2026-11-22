"""pero 内部契约：通道、模型、工具、记忆、检索、会话策略。

本模块只定义数据类型与抽象协议，不含任何实现，也不依赖 webot、AstrBot
或 OneBot 的具体对象。核心层持有这里的类型，而不是微信或框架对象。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, AsyncIterator, Mapping, Protocol, Sequence, runtime_checkable

#: 首版默认模型请求构造组件的固定 ID。配置装配与内置默认配置共用这一个常量，
#: 避免 `config/default.json` 与内置回退各自维护出不同的默认绑定。
DEFAULT_MODEL_COMPONENT_ID = "deepseek.chat_completions"

#: 首版通道适配组件 ID（OneBot v11 接收，默认不启用）。
DEFAULT_CHANNEL_COMPONENT_ID = "channel.onebot11"

#: 首版会话策略组件 ID（纯文本唤醒/窗口，默认不处理任何会话）。
DEFAULT_POLICY_COMPONENT_ID = "policy.window_text"

#: 首版持久记忆组件 ID（独立 SQLite：原文、身份别名、L2/L20）。
DEFAULT_MEMORY_COMPONENT_ID = "memory.sqlite"

#: 首版默认 Agent 标识与显示名（内置默认配置与配置模板保持一致）。
DEFAULT_AGENT_ID = "default"
DEFAULT_AGENT_DISPLAY_NAME = "Pero"
DEFAULT_AGENT_DESCRIPTION = (
    "第五阶段（P05）默认 Agent：模型客户端、有界工具循环、OneBot 接收与纯文本唤醒/窗口策略，"
    "并接入独立 SQLite 的原文/L2/L20 存储；三个开关默认关闭，尚未接入 L3/L4 与检索。"
)


# --------------------------------------------------------------------------- #
# 模型调用目的
# --------------------------------------------------------------------------- #


class ModelPurpose(str, Enum):
    """模型请求的唯一用途枚举。

    新增用途必须在这里登记，从而自动纳入“统一关闭思考”的同一出口；
    没有自由填写的 ``other`` 分支，避免某处调用悄悄绕开约束。
    """

    MAIN_REPLY = "main_reply"
    TOOL_CONTINUATION = "tool_continuation"
    TOOL_RETRY = "tool_retry"
    TOOL_ANALYSIS = "tool_analysis"
    PLUGIN_RESULT = "plugin_result"
    ENGAGEMENT_PREFLIGHT = "engagement_preflight"
    IMAGE_SELECTION = "image_selection"
    WEATHER_EXTRACTION = "weather_extraction"
    MEMORY_FIELD_CLEANUP = "memory_field_cleanup"
    JOURNAL = "journal"
    CONVERSATION_SUMMARY = "conversation_summary"


PURPOSE_DESCRIPTIONS: Mapping[ModelPurpose, str] = {
    ModelPurpose.MAIN_REPLY: "主回复：普通文字回答、带图回答、带文件回答。",
    ModelPurpose.TOOL_CONTINUATION: "工具执行后的继续回答（每个子请求各自构造）。",
    ModelPurpose.TOOL_RETRY: "工具失败或参数错误后的重试请求。",
    ModelPurpose.TOOL_ANALYSIS: "工具结果的独立模型分析阶段。",
    ModelPurpose.PLUGIN_RESULT: "后台插件结果回复。",
    ModelPurpose.ENGAGEMENT_PREFLIGHT: "接话/唤醒判断等 preflight 辅助调用。",
    ModelPurpose.IMAGE_SELECTION: "图片选择器：从候选中挑图。",
    ModelPurpose.WEATHER_EXTRACTION: "天气等参数提取。",
    ModelPurpose.MEMORY_FIELD_CLEANUP: "记忆字段整理与规范化。",
    ModelPurpose.JOURNAL: "日记生成。",
    ModelPurpose.CONVERSATION_SUMMARY: "对话总结与其他后台整理。",
}

#: 该用途是否允许携带工具定义。工具开关与思考开关相互独立：
#: 关闭思考不等于禁用工具调用。
PURPOSE_TOOLS_ALLOWED: Mapping[ModelPurpose, bool] = {
    ModelPurpose.MAIN_REPLY: True,
    ModelPurpose.TOOL_CONTINUATION: True,
    ModelPurpose.TOOL_RETRY: True,
    ModelPurpose.TOOL_ANALYSIS: False,
    ModelPurpose.PLUGIN_RESULT: False,
    ModelPurpose.ENGAGEMENT_PREFLIGHT: False,
    ModelPurpose.IMAGE_SELECTION: False,
    ModelPurpose.WEATHER_EXTRACTION: False,
    ModelPurpose.MEMORY_FIELD_CLEANUP: False,
    ModelPurpose.JOURNAL: False,
    ModelPurpose.CONVERSATION_SUMMARY: False,
}


# --------------------------------------------------------------------------- #
# 消息与通道
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class AttachmentRef:
    """附件引用。跨进程只传句柄与元数据，不传宿主机绝对路径。"""

    handle: str
    kind: str
    name: str | None = None
    size: int | None = None
    sha256: str | None = None


@dataclass(frozen=True)
class InboundMessage:
    """标准入站消息。取不到的字段留 None 或空，不用昵称推断身份。

    身份分层：
    - `channel_id + account_id + sender_ref` 才是稳定身份；
    - `display_name` / `group_display_name` 只是带来源的展示别名，不能用来合并用户；
    - `raw_target_ref`（原始群 wxid）与 `sender_raw_ref`（原始 wxid）在通道取不到时为 None，
      不得用昵称或桥 ID 反推。
    """

    event_id: str
    channel_id: str
    account_id: str
    conversation_id: str
    sender_ref: str
    text: str
    display_name: str | None = None
    mentions: tuple[str, ...] = ()
    attachments: tuple[AttachmentRef, ...] = ()
    quoted_event_id: str | None = None
    received_at: str | None = None
    #: `group` / `private` / `unknown`
    conversation_type: str = "unknown"
    #: 通道内稳定目标 ID（群 ID 或私聊对端 ID），用于路由与出站
    target_ref: str | None = None
    #: 原始群 ID（微信 wxid）；通道未提供时为 None
    raw_target_ref: str | None = None
    #: 原始发送者 ID（微信 wxid）；通道未提供时为 None
    sender_raw_ref: str | None = None
    #: 群内名称；通道未提供时为 None
    group_display_name: str | None = None
    #: event_id 的可去重范围，例如 `bridge_process`；`unknown` 表示不可跨重启去重
    dedupe_scope: str = "unknown"
    #: 版本化引用事实（P13，桥 `pero_ext.quote`）：结构化元数据（来源/作者/句柄），
    #: **不是**被引用事件 ID——桥没有跨消息 ID 关联时不合成 quoted_event_id。
    quote_meta: Mapping[str, Any] | None = None
    #: 版本化文件事实（P13，桥 `pero_ext.file`）：句柄/名称/大小/哈希/群归属；
    #: 只是"来源声明"，字节须经限定 RPC 校验后入库，占位文本不算已收到文件。
    file_offer: Mapping[str, Any] | None = None


class SendStatus(str, Enum):
    SENT = "sent"
    FAILED = "failed"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class OutboundAction:
    """标准出站动作。由通道适配器翻译成具体平台的发送与 @。"""

    conversation_id: str
    text: str
    mention_refs: tuple[str, ...] = ()
    reply_to_event_id: str | None = None
    #: 适配器分配或调用方指定的稳定动作 ID，用于 echo 关联与回执追踪
    action_id: str | None = None


@dataclass(frozen=True)
class SendReceipt:
    """发送回执。

    只有 ``sent`` 才算"已经在会话里说过"；``unknown`` 一律不自动重发，
    也不能记成已发送。``evidence`` 记录判定依据（例如"桥返回 message_id"）。
    """

    status: SendStatus
    detail: str | None = None
    action_id: str | None = None
    evidence: str = ""


class QueryStatus(str, Enum):
    """只读查询状态（P13-R01）：与发送回执严格分离。

    ``ok`` 只代表**查到了数据**，绝不代表任何消息 sent；``failed`` 是桥或
    校验给出的明确失败；``unknown`` 是无连接/超时/断连等无法确认的情形，
    与发送三态互不复用、互不映射。
    """

    OK = "ok"
    FAILED = "failed"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class QueryResult:
    """只读查询回执（文件字节 / 引用原图），与 ``SendReceipt`` 分离。

    调用方不得把 ``QueryStatus`` 转成发送状态，也不得据此写投递证据；
    ``evidence`` 记录判定依据（例如 no_connection / query_timeout / 桥错误码）。
    """

    status: QueryStatus
    query_id: str = ""
    detail: str | None = None
    evidence: str = ""


class ChannelEventKind(str, Enum):
    MESSAGE = "message"
    META = "meta_event"
    ACTION_ECHO = "action_echo"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class ChannelEvent:
    """通道事件的通用信封：消息、元事件、动作回执或不可识别事件。"""

    kind: ChannelEventKind
    channel_id: str
    account_id: str
    event_id: str | None = None
    event_time: str | None = None
    message: InboundMessage | None = None
    meta_event_type: str | None = None
    #: 不可处理事件的受控错误码（不含原文）
    reject_code: str | None = None
    reject_reason: str | None = None


# --------------------------------------------------------------------------- #
# 模型请求与结果
# --------------------------------------------------------------------------- #


class MessageRole(str, Enum):
    SYSTEM = "system"
    USER = "user"
    ASSISTANT = "assistant"
    TOOL = "tool"


@dataclass(frozen=True)
class ModelToolCall:
    """模型请求的一次工具调用。

    ``arguments`` 保留模型返回的**原始 JSON 字符串**，不在解析层做纠正；
    执行器负责严格 JSON 解析与参数校验。
    """

    call_id: str
    name: str
    arguments: str


@dataclass(frozen=True)
class ModelMessage:
    """provider 无关的消息结构。

    ``content`` 允许字符串（纯文本）或内容块列表（文本/图片/文件等），
    assistant 仅调用工具时可为 ``None``；``tool_calls`` 保存 assistant 发起的调用，
    ``tool_call_id`` 保存 tool 消息对应的调用 ID。由具体 provider 的构造器转换为线上格式。
    """

    role: MessageRole
    content: Any
    name: str | None = None
    tool_call_id: str | None = None
    reasoning_content: str | None = field(default=None,repr=False,compare=False)
    tool_calls: tuple[ModelToolCall, ...] = ()


@dataclass(frozen=True)
class ToolSpec:
    """函数形式的工具定义（名称、说明、JSON Schema 参数）。"""

    name: str
    description: str
    parameters: Mapping[str, Any]


@dataclass(frozen=True)
class ModelRequest:
    """业务模块对模型的唯一声明方式：说清用途和内容，不自己拼 HTTP。"""

    purpose: ModelPurpose
    messages: Sequence[ModelMessage]
    tools: Sequence[ToolSpec] = ()
    parameters: Mapping[str, Any] = field(default_factory=dict)
    tool_name: str | None = None
    history_prefix: int = 0
    system_prefix: int = 0


@dataclass(frozen=True)
class ModelResult:
    """一次模型调用的结果。

    ``text`` 可能为空字符串（仅调用工具、无正文）；``tool_calls`` 保留原始
    arguments 字符串；``usage`` 只保存已知的数值统计。``reasoning_content``
    不属于本结构，响应中的思维链内容不进入历史或日志。
    """

    text: str
    tool_calls: tuple[ModelToolCall, ...] = ()
    finish_reason: str | None = None
    usage: Mapping[str, Any] = field(default_factory=dict)
    request_id: str | None = None
    reasoning_content: str | None = field(default=None,repr=False,compare=False)


# --------------------------------------------------------------------------- #
# 组件协议（P01 只定义，不代表已有可用实现）
# --------------------------------------------------------------------------- #


@runtime_checkable
class ChannelAdapter(Protocol):
    """通道适配器：把平台事件转成标准消息，并执行标准出站动作。"""

    channel_id: str

    def status(self) -> str: ...

    def receive(self) -> AsyncIterator[InboundMessage]: ...

    async def send(self, action: OutboundAction) -> SendReceipt: ...


@runtime_checkable
class ModelProvider(Protocol):
    """模型提供方：接收标准请求，返回文本、工具调用与用量。"""

    provider_id: str

    async def complete(self, request: ModelRequest) -> ModelResult: ...


@runtime_checkable
class Tool(Protocol):
    """工具：声明参数模式并执行，返回结构化结果。"""

    tool_id: str

    def spec(self) -> ToolSpec: ...

    async def run(self, arguments: Mapping[str, Any]) -> Mapping[str, Any]: ...


@runtime_checkable
class MemoryRepository(Protocol):
    """记忆存储：按层保存与查询，上层不依赖具体表名。"""

    repository_id: str

    def layer_overview(self) -> Sequence[Mapping[str, Any]]: ...

    async def query(
        self,
        *,
        layer: str,
        limit: int,
        cursor: str | None = None,
    ) -> Mapping[str, Any]: ...


@runtime_checkable
class Retriever(Protocol):
    """检索器：按范围检索并返回证据条目与查询状态。"""

    retriever_id: str

    async def retrieve(
        self,
        *,
        query: str,
        scope: Mapping[str, Any],
        limit: int = 8,
    ) -> Sequence[Mapping[str, Any]]: ...


@runtime_checkable
class ConversationPolicy(Protocol):
    """会话策略：唤醒、接话判断与上下文组装。"""

    policy_id: str

    async def should_reply(
        self, message: InboundMessage, context: Mapping[str, Any]
    ) -> bool: ...

    async def compose_context(
        self, message: InboundMessage, context: Mapping[str, Any]
    ) -> Sequence[ModelMessage]: ...


# --------------------------------------------------------------------------- #
# 记忆层级目录（P01 仅描述，未初始化任何存储）
# --------------------------------------------------------------------------- #

MEMORY_LAYER_CATALOG: Sequence[Mapping[str, Any]] = (
    {
        "key": "L1",
        "name": "即时语境",
        "description": "最近几轮对话的内存快照，不落盘，重启即丢失。",
        "persistent": False,
    },
    {
        "key": "L10",
        "name": "深层缓冲",
        "description": "较长范围的会话缓冲，同样保持内存态。",
        "persistent": False,
    },
    {
        "key": "L2",
        "name": "聊天视图",
        "description": "最近对话视图及可追溯原文，受上下文预算限制。",
        "persistent": True,
    },
    {
        "key": "L20",
        "name": "工具与附件回执",
        "description": "工具调用、图片/文件编号、读取与发送状态。",
        "persistent": True,
    },
    {
        "key": "L3",
        "name": "人物事实",
        "description": "人物事实、别名关系、来源与更新时间。",
        "persistent": True,
    },
    {
        "key": "L4",
        "name": "长期知识",
        "description": "长期知识及检索索引状态，区分可靠记录与派生索引。",
        "persistent": True,
    },
    {
        "key": "journal",
        "name": "日记与总结",
        "description": "按日期和话题保存的日记与总结，可回到来源。",
        "persistent": True,
    },
    {
        "key": "L2.5",
        "name": "处理过程归档",
        "description": "归档状态的处理过程，不作为独立记忆库展示。",
        "persistent": True,
    },
)
