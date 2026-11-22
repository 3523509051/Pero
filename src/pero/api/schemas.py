"""只读接口的响应数据模型。

字段即契约：后续独立前端只依赖这里的结构，不依赖内部表名或实现细节。
凭据类字段一律不存在，或只能缺省/掩码。
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from pero.components.schemas import (
    ComponentStatus,
    ComponentType,
    ConfigFieldDescription,
)


class HealthResponse(BaseModel):
    draining: bool = False
    ok: bool
    service: str = "pero"
    version: str
    phase: str
    agent_ready: bool = Field(
        description="整体是否可直接接待外部对话：仍为 false（无通道、无记忆）。"
    )
    loop_ready: bool = Field(
        description="Agent 循环已实现且默认 Agent 绑定了模型组件（不代表凭据与连通性就绪）。"
    )
    model_configured: bool = Field(
        description="模型凭据环境变量是否已配置；false 时模型调用会被明确拒绝。"
    )
    default_agent_id: str
    uptime_seconds: float
    started_at: str
    pid: int
    python_version: str
    components_registered: int
    config_loaded: bool = Field(description="是否加载了配置文件；否表示使用内置默认值。")


class ComponentTypeInfo(BaseModel):
    type: ComponentType
    description: str
    registered: int


class PurposeInfo(BaseModel):
    purpose: str
    description: str
    tools_allowed: bool = Field(description="首版预期是否携带工具定义，仅供说明，不改变关闭思考政策。")
    thinking: Dict[str, str]


class ThinkingPolicyInfo(BaseModel):
    model: str
    protocol: str
    always_disabled: bool = True
    overridable: bool = False
    wire_field: str = "thinking"
    wire_value: Dict[str, str]
    sdk_location: str = Field(description="使用 OpenAI 兼容 SDK 时该字段位于 extra_body，而非 HTTP 正文层级。")
    forbidden_overrides: List[str]
    purposes: List[PurposeInfo]


class MemoryLayerInfo(BaseModel):
    key: str
    name: str
    description: str
    persistent: bool
    status: ComponentStatus


class ModelClientInfo(BaseModel):
    """模型客户端的三件事分开说：实现存在 / 凭据已配置 / 连通性未核对。"""

    client_implemented: bool
    http_client: str
    api_key_env: str
    api_key_configured: bool
    remote_connectivity: ComponentStatus = ComponentStatus.NOT_VERIFIED
    streaming: bool = False
    auto_retry: bool = False
    timeout_seconds: float


class RuntimeLimitsView(BaseModel):
    max_model_calls: int
    max_tool_calls: int
    tool_timeout_seconds: float
    max_history_messages: int
    max_user_chars: int
    max_tool_result_chars: int


class ChannelStatusView(BaseModel):
    """通道适配的脱敏元数据：实现 / 配置启用 / 真实连接分开报告。"""

    channel_id: str
    implementation_delivered: bool
    enabled: bool
    listening: bool
    connected: bool
    host: str
    port: int
    path: str
    disabled_reason: str = ""
    connections: int = 0
    online_accounts: List[str] = Field(
        default_factory=list, description="当前在线的账号；空表示无账号在线。"
    )
    accounts: List[Dict[str, Any]] = Field(
        default_factory=list,
        description="按账号的连接元数据（账号、连接序号、角色、时间、待确认动作数），无正文无令牌。",
    )
    duplicate_connection_policy: str = "reject_new"
    pending_actions: int = 0
    token_required: bool = False
    last_connected_at: Optional[str] = None
    last_disconnected_at: Optional[str] = None
    last_event_at: Optional[str] = None
    last_heartbeat_at: Optional[str] = None
    last_error_code: Optional[str] = None
    counters: Dict[str, int] = Field(default_factory=dict)
    dedupe: Dict[str, Any] = Field(default_factory=dict)
    notes: List[str] = Field(default_factory=list)


class ChannelsResponse(BaseModel):
    api_version: str
    channels: List[ChannelStatusView]


class OrchestrationStatusView(BaseModel):
    """通道文本编排的脱敏状态：开关、队列、策略窗口、计数；不含正文。"""

    auto_process_enabled: bool
    auto_send_enabled: bool
    channel_enabled: bool
    policy: Dict[str, Any] = Field(default_factory=dict)
    routes: Dict[str, Any] = Field(default_factory=dict)
    send_whitelist: Dict[str, Any] = Field(default_factory=dict)
    queues: Dict[str, Any] = Field(default_factory=dict)
    merge: Dict[str, Any] = Field(default_factory=dict)
    candidates: Dict[str, Any] = Field(default_factory=dict)
    maintenance: Dict[str, Any] = Field(
        default_factory=dict,
        description="后台巡检（候选 TTL 清理）的间隔与是否在运行。",
    )
    counters: Dict[str, int] = Field(default_factory=dict)
    decisions: Dict[str, int] = Field(default_factory=dict)
    rejections: Dict[str, int] = Field(default_factory=dict)


class OrchestrationResponse(BaseModel):
    api_version: str
    orchestration: OrchestrationStatusView


class CapabilitiesResponse(BaseModel):
    api_version: str
    phase: str
    service_version: str
    component_types: List[ComponentTypeInfo]
    thinking_policy: ThinkingPolicyInfo
    capability_status: Dict[str, ComponentStatus]
    memory_layers: List[MemoryLayerInfo]
    model_client: ModelClientInfo
    runtime_limits: RuntimeLimitsView
    channels: List[ChannelStatusView] = Field(default_factory=list)
    orchestration: OrchestrationStatusView


class ComponentView(BaseModel):
    id: str
    type: ComponentType
    version: str
    description: str
    config_fields: List[ConfigFieldDescription]
    capabilities: Dict[str, Any]
    dependencies: List[str]
    status: ComponentStatus
    status_detail: Optional[str] = None
    has_instance: bool


class ComponentsResponse(BaseModel):
    api_version: str
    components: List[ComponentView]
    component_types: List[ComponentTypeInfo]
    note: str


class AgentSummary(BaseModel):
    agent_id: str
    display_name: str
    description: str
    agent_ready: bool
    loop_ready: bool
    model_configured: bool
    channel_status: ComponentStatus
    channel_connected: bool
    status: ComponentStatus
    status_detail: str
    bound_component_ids: List[str]
    unbound_slots: List[str] = Field(description="配置中完全没有绑定组件 ID 的槽位。")
    not_ready_slots: List[str] = Field(
        default_factory=list,
        description="已绑定但当前状态不是 available 的槽位（如未连接、缺凭据）。",
    )


class AgentsResponse(BaseModel):
    api_version: str
    default_agent_id: str
    agents: List[AgentSummary]


class SlotView(BaseModel):
    slot: str
    expected_type: str
    component_id: Optional[str] = None
    status: ComponentStatus
    status_detail: Optional[str] = None
    capabilities: Dict[str, Any] = Field(default_factory=dict)


class CompositionResponse(BaseModel):
    api_version: str
    agent_id: str
    display_name: str
    agent_ready: bool
    thinking_policy: ThinkingPolicyInfo
    slots: List[SlotView]


class ErrorResponse(BaseModel):
    error: str
    message: str
    detail: Dict[str, Any] = Field(default_factory=dict)


# --------------------------------------------------------------------------- #
# P02：内存会话与运行
# --------------------------------------------------------------------------- #


class SessionCreateResponse(BaseModel):
    api_version: str
    agent_id: str
    session_id: str
    created_at: str
    session_status: str = Field(description="内存会话状态；重启即丢失，不等于持久记忆。")
    runtime_limits: RuntimeLimitsView


class SessionMessageRequest(BaseModel):
    image_ids: List[str] = Field(default_factory=list,max_length=3,
        description="可选：本会话已接收的 Pero 图片编号；仅在本轮挂载。")
    text: str = Field(
        min_length=1,
        description="用户正文；图片原件通过资源接口接收，再用 image_ids 引用。",
    )


class RunToolView(BaseModel):
    analysis: Optional[Dict[str, Any]] = None
    call_id: str
    name: str
    status: str
    error_code: Optional[str] = None
    duration_ms: int


class RunResponse(BaseModel):
    image_inputs: List[Dict[str, Any]] = Field(default_factory=list)
    api_version: str
    agent_id: str
    session_id: str
    run_id: str
    status: str = Field(
        description="真实运行状态：completed / failed / limit_reached / cancelled。"
    )
    text: Optional[str] = None
    error_code: Optional[str] = None
    error_message: Optional[str] = None
    finish_reason: Optional[str] = None
    tool_calls: List[RunToolView] = Field(default_factory=list)
    model_calls: int = 0
    tool_call_count: int = 0
    started_at: str
    finished_at: str


class LastRunView(BaseModel):
    run_id: str
    status: str
    error_code: Optional[str] = None
    started_at: str
    finished_at: str
    model_calls: int
    tool_calls: int


class SessionInfoResponse(BaseModel):
    api_version: str
    agent_id: str
    session_id: str
    session_status: str
    busy: bool
    created_at: str
    updated_at: str
    message_count: int = Field(description="历史消息条数，不返回正文。")
    run_count: int
    last_run: Optional[LastRunView] = None
    runtime_limits: RuntimeLimitsView


# ---------------------------------------------------------------------- #
# P05 持久记忆
# ---------------------------------------------------------------------- #


class MemoryStorageView(BaseModel):
    """存储状态：不返回数据库路径，也不返回任何正文。"""

    component_id: str
    backend: str
    available: bool
    schema_version: Optional[int] = None
    expected_schema_version: int
    single_process: bool = True
    path_exposed: bool = False
    error: Optional[str] = Field(
        default=None, description="初始化失败原因（已脱敏，不含路径）。"
    )


class MemoryLayerView(BaseModel):
    key: str
    name: str
    kind: str = Field(description="layer=记忆层；source=来源存储（原文）。")
    persistent: bool
    status: str = Field(description="available / not_initialized。")
    detail: str
    record_count: int = 0


class MemoryLayersResponse(BaseModel):
    api_version: str
    agent_id: Optional[str] = None
    storage: MemoryStorageView
    layers: List[MemoryLayerView] = Field(default_factory=list)
    counts: Dict[str, int] = Field(default_factory=dict)
    queryable_layers: List[str] = Field(default_factory=list)


class MemoryRecordItem(BaseModel):
    """列表项：只给计数与截断预览，默认不返回整段正文。"""

    record_id: str
    layer: str
    kind: Optional[str] = None
    conversation_id: Optional[str] = None
    created_at: str
    status: str
    text_chars: int
    preview: str = ""
    preview_truncated: bool = False
    identity_id: Optional[str] = None
    source_event_id: Optional[str] = None
    tool_name: Optional[str] = None
    call_id: Optional[str] = None
    error_code: Optional[str] = None
    turn_id: Optional[str] = None
    has_assistant: Optional[bool] = None


class MemoryRecordsResponse(BaseModel):
    api_version: str
    agent_id: str
    layer: str
    items: List[MemoryRecordItem] = Field(default_factory=list)
    total: int
    limit: int
    offset: int
    next_offset: Optional[int] = None
    preview_chars: int = Field(description="预览长度上限；完整正文请用详情接口。")


class MemoryRecordDetailResponse(BaseModel):
    api_version: str
    agent_id: str
    record: Dict[str, Any] = Field(
        description="单条详情：正文按 detail_max_chars 截断并标记；含来源引用。"
    )


class MemoryAliasView(BaseModel):
    alias: str
    alias_type: str
    scope_type: str
    scope_ref: str
    source: str
    observed_at: str


class MemoryIdentityCandidate(BaseModel):
    identity_id: str
    channel_id: str
    account_id: str
    bridge_user_id: str
    wxid_known: bool = Field(
        default=False, description="是否已有可信 wxid；未证实时保持 false，不猜测。"
    )
    wxid_source: str = "unknown"
    aliases: List[MemoryAliasView] = Field(default_factory=list)


class MemoryIdentitiesResponse(BaseModel):
    api_version: str
    agent_id: str
    alias: str
    account_id: Optional[str] = None
    conversation_id: Optional[str] = None
    candidates: List[MemoryIdentityCandidate] = Field(default_factory=list)
    ambiguous: bool = Field(
        default=False,
        description="按**总命中数**判定：多于一个候选时为 true（limit=1 时同样成立）。",
    )
    total: int = Field(default=0, description="LIMIT 之前的候选总数（用于判断歧义）。")
    has_more: bool = Field(default=False, description="是否还有未返回的候选。")
    limit: int
    wxid_note: str = ""


class MemorySearchItem(BaseModel):
    """一条检索命中：层、记录 ID、时间、有界预览与来源引用。"""

    layer: str
    record_id: str
    occurred_at: str
    preview: str
    score: int = 0
    source_refs: List[str] = Field(default_factory=list)
    subject_label: Optional[str] = None


class MemorySearchResponse(BaseModel):
    """只读检索结果：状态必须区分没命中 / 身份歧义 / 不可用 / 失败。"""

    api_version: str
    agent_id: str
    query: str
    state: str = Field(
        description="ok / no_hits / identity_ambiguous / unavailable / failed"
    )
    detail: str = ""
    identity_id: Optional[str] = None
    ambiguous_candidates: List[str] = Field(default_factory=list)
    items: List[MemorySearchItem] = Field(default_factory=list)
