"""按配置装配组件注册表，并解析 Agent 的组件绑定关系。

登记原则：只登记真实存在的对象，能力只写实际具备的行为。
P05 起登记独立 SQLite 记忆仓库（原文 / 身份别名 / L2 / L20）；检索仍只有空槽位。
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from pero import PHASE, __version__
from pero.components.registry import ComponentRegistry
from pero.components.schemas import (
    ComponentDescriptor,
    ComponentStatus,
    ComponentType,
    ConfigFieldDescription,
)
from pero.agent.policy import WindowConversationPolicy
from pero.channels.onebot11.server import OneBotChannelServer
from pero.core.contracts import (
    DEFAULT_CHANNEL_COMPONENT_ID,
    DEFAULT_MODEL_COMPONENT_ID,
    ModelPurpose,
)
from pero.core.settings import AgentSettings, ProviderSettings, Settings
from pero.memory.repository import SqliteMemoryRepository
from pero.memory.retriever import SqliteKeywordRetriever
from pero.providers.deepseek.client import DeepSeekModelProvider
from pero.providers.deepseek.request_builder import FORBIDDEN_PARAMETER_KEYS
from pero.tools.registry import ToolRegistry

MODEL_PROVIDER_COMPONENT_ID = DEFAULT_MODEL_COMPONENT_ID
CHANNEL_COMPONENT_ID = DEFAULT_CHANNEL_COMPONENT_ID
#: 检索组件 ID（配置 `agents[].composition.retriever` 引用）。
RETRIEVER_COMPONENT_ID = "retriever.sqlite_keyword"

#: Agent 装配槽位与期望组件类型的映射。配置用组件 ID 绑定实例。
SLOT_TYPES: Dict[str, ComponentType] = {
    "channel": ComponentType.CHANNEL,
    "model_provider": ComponentType.MODEL_PROVIDER,
    "memory_repository": ComponentType.MEMORY_REPOSITORY,
    "retriever": ComponentType.RETRIEVER,
    "conversation_policy": ComponentType.CONVERSATION_POLICY,
}


def model_provider_descriptor(
    provider_settings: ProviderSettings,
    provider: DeepSeekModelProvider,
) -> ComponentDescriptor:
    """模型客户端描述：区分“实现存在 / 凭据已配置 / 真实连通性未核对”。"""

    configured = provider.api_key_configured
    detail = (
        "客户端已实现：统一构造器 + 导出复核 + 超时/取消/错误映射；凭据已配置；"
        "真实连通性未核对（本阶段没有发起真实调用）。"
        if configured
        else (
            f"客户端已实现，但未配置凭据：环境变量 {provider_settings.api_key_env} 为空，"
            "请求会被明确拒绝；真实连通性未核对。"
        )
    )
    return ComponentDescriptor(
        id=MODEL_PROVIDER_COMPONENT_ID,
        type=ComponentType.MODEL_PROVIDER,
        version=__version__,
        description=(
            "DeepSeek Chat Completions 客户端：统一请求构造、强制 thinking=disabled、"
            "发送前复核最终正文；首版非流式、不自动重试。"
        ),
        config_fields=[
            ConfigFieldDescription(
                name="model",
                type="string",
                required=True,
                default=provider.model_name,
                description="首版统一模型名，来自 config 的 thinking.model。",
            ),
            ConfigFieldDescription(
                name="base_url",
                type="string",
                required=False,
                default=provider_settings.base_url,
                description="OpenAI 兼容基地址；实际调用路径为 {base_url}/chat/completions。",
            ),
            ConfigFieldDescription(
                name="api_key_env",
                type="string",
                required=False,
                default=provider_settings.api_key_env,
                description="读取 Key 的环境变量名。仅名称，绝不返回其值。",
                secret=True,
            ),
            ConfigFieldDescription(
                name="request_timeout_seconds",
                type="number",
                required=False,
                default=provider_settings.request_timeout_seconds,
                description="单次模型调用的超时，单位秒。",
            ),
        ],
        capabilities={
            "request_building": True,
            "network_execution": True,
            "http_client": "httpx",
            "streaming": False,
            "auto_retry": False,
            "tool_calls": True,
            "thinking_mode": "disabled_only",
            "thinking_overridable": False,
            "credentials_configured": configured,
            "remote_connectivity": "not_verified",
            "purposes": [purpose.value for purpose in ModelPurpose],
        },
        dependencies=[],
        status=ComponentStatus.AVAILABLE if configured else ComponentStatus.NOT_INITIALIZED,
        status_detail=detail,
    )


def channel_descriptor(
    settings: Settings, channel: OneBotChannelServer
) -> ComponentDescriptor:
    """OneBot 接收适配器的组件描述。

    注册表是静态快照：这里只能说"实现已交付 / 配置是否启用"，
    实时监听与连接状态由 `/api/v1/channels` 提供，不能写成 connected。
    """

    cfg = settings.channels.onebot11
    if cfg.enabled:
        status = ComponentStatus.NOT_VERIFIED
        detail = (
            "已按配置启用接收；实时监听/连接状态见 /api/v1/channels。"
            "本阶段不触发模型、不自动回复、不发送任何消息。"
        )
    else:
        status = ComponentStatus.NOT_CONNECTED
        detail = (
            "实现已交付，但配置未启用（channels.onebot11.enabled=false）："
            "不监听端口、未连接任何桥。本阶段不触发模型、不自动回复。"
        )
    return ComponentDescriptor(
        id=CHANNEL_COMPONENT_ID,
        type=ComponentType.CHANNEL,
        version=__version__,
        description=(
            "OneBot v11 反向 WebSocket 接收适配：事件归一化、稳定身份与会话键、"
            "有界短期去重、动作编码与 echo 回执关联。"
        ),
        config_fields=[
            ConfigFieldDescription(
                name="enabled",
                type="boolean",
                required=False,
                default=False,
                description="是否监听接收端口；默认 false，不改生产端口。",
            ),
            ConfigFieldDescription(
                name="host",
                type="string",
                required=False,
                default=cfg.host,
                description="监听地址，默认仅本机回环。",
            ),
            ConfigFieldDescription(
                name="port",
                type="number",
                required=False,
                default=cfg.port,
                description="独立端口；不复用桥/旧服务端口。",
            ),
            ConfigFieldDescription(
                name="path",
                type="string",
                required=False,
                default=cfg.path,
                description="反向 WebSocket 路径。",
            ),
            ConfigFieldDescription(
                name="token_env",
                type="string",
                required=False,
                default=cfg.token_env,
                description="可选握手令牌的环境变量名；只读名称，不返回值。",
                secret=True,
            ),
        ],
        capabilities={
            "receives_events": True,
            "sends_actions": True,
            "echo_receipts": True,
            "triggers_model": False,
            "auto_reply": False,
            "default_enabled": False,
            "dedupe": "bounded_ttl",
            "protocol": "onebot11_reverse_ws",
            "raw_group_id_available": False,
            "quoted_message_available": False,
            "binding_required": True,
        },
        dependencies=[],
        status=status,
        status_detail=detail,
    )


def policy_descriptor(settings: Settings, policy: WindowConversationPolicy) -> ComponentDescriptor:
    """纯文本唤醒/窗口策略组件：实现存在，但默认不处理任何会话。"""

    orchestration = settings.orchestration
    active = orchestration.auto_process_enabled and bool(
        orchestration.allowed_conversations
    )
    detail = (
        "已启用自动处理，且配置了允许会话；实际是否回复仍受唤醒词/窗口/路由约束。"
        if active
        else (
            "实现已交付，但当前未启用：auto_process_enabled=false 或 allowed_conversations 为空，"
            "事件只会被统计，不会进入模型。"
        )
    )
    detail += (
        " 编排按 Agent 的 conversation_policy 槽位解析本组件："
        "槽位未绑定/未登记/类型不符时明确拒绝，不回退全局实例。"
    )
    return ComponentDescriptor(
        id=policy.policy_id,
        type=ComponentType.CONVERSATION_POLICY,
        version=__version__,
        description=(
            "纯文本唤醒与窗口策略：显式唤醒词、可选可信 at、滑动窗口、"
            "会话级隔离；不调用模型，不做接话判断。"
        ),
        config_fields=[
            ConfigFieldDescription(
                name="wake_words",
                type="array",
                required=False,
                default=list(orchestration.wake_words),
                description="文本唤醒词；桥的 at 段默认不被当作可信提及。",
            ),
            ConfigFieldDescription(
                name="window_after_wake_seconds",
                type="number",
                required=False,
                default=orchestration.window_after_wake_seconds,
                description="被可信唤醒后的初始窗口长度。",
            ),
            ConfigFieldDescription(
                name="window_idle_seconds",
                type="number",
                required=False,
                default=orchestration.window_idle_seconds,
                description="窗口内新消息的滑动续期。",
            ),
            ConfigFieldDescription(
                name="window_max_seconds",
                type="number",
                required=False,
                default=orchestration.window_max_seconds,
                description="单次激活的存活上限。",
            ),
        ],
        capabilities={
            "text_only": True,
            "wake_word": True,
            "trusted_at_optional": True,
            "window": True,
            "calls_model": False,
            "images_supported": settings.images.enabled,
            "binding_required": True,
            "state_scope": "agent_conversation",
        },
        dependencies=[],
        status=ComponentStatus.AVAILABLE if active else ComponentStatus.NOT_INITIALIZED,
        status_detail=detail,
    )


def memory_descriptor(
    settings: Settings, repository: SqliteMemoryRepository
) -> ComponentDescriptor:
    """独立 SQLite 记忆仓库：只声明真实具备的层与能力，不暴露路径。"""

    memory = settings.memory
    available = repository.available
    if available:
        status = ComponentStatus.AVAILABLE
        detail = (
            f"独立 SQLite 已就绪（schema v{repository.schema_version}，单进程）。"
            "持久化：入站原文、身份与别名、L2 轮次、L20 工具证据、L4 明确保存记录、"
            "事件日记；只有 sent 轮次进入模型可读历史，工具结果只存受控摘要；"
            "图片输入记录只存名称/编号/提供状态，不存Base64，不把提供当成识别成功。数据库位置不对外返回。"
        )
    else:
        status = ComponentStatus.NOT_INITIALIZED
        detail = (
            "存储初始化失败："
            f"{repository.error_detail or '未知原因'}。"
            "编排会明确拒绝（memory_unavailable），不会假装已保存。"
        )
    return ComponentDescriptor(
        id=repository.repository_id,
        type=ComponentType.MEMORY_REPOSITORY,
        version=__version__,
        description=(
            "独立 SQLite 持久记忆：按 source/event ID 幂等保存原文，"
            "稳定身份 + 带作用域的别名，L2 轮次、L20 工具证据、L4 明确保存与事件日记；"
            "检索由独立 retriever 组件承担，本组件不内置检索。"
        ),
        config_fields=[
            ConfigFieldDescription(
                name="load_turn_limit",
                type="number",
                required=False,
                default=memory.load_turn_limit,
                description="重建模型历史时最多载入的已发言轮次。",
            ),
            ConfigFieldDescription(
                name="load_char_budget",
                type="number",
                required=False,
                default=memory.load_char_budget,
                description="重建历史时正文总字符预算。",
            ),
            ConfigFieldDescription(
                name="detail_max_chars",
                type="number",
                required=False,
                default=memory.detail_max_chars,
                description="详情接口返回正文的长度上限。",
            ),
            ConfigFieldDescription(
                name="preview_chars",
                type="number",
                required=False,
                default=memory.preview_chars,
                description="列表接口预览长度上限（默认不返回整段正文）。",
            ),
        ],
        capabilities={
            "persistent": True,
            "backend": "sqlite",
            "single_process": True,
            "schema_version": repository.schema_version,
            "layers": ["raw", "L2", "L20", "L4", "journal"],
            "raw_messages": True,
            "identity_aliases": True,
            "explicit_save": True,
            "archive_summary": True,
            "archive_enabled": settings.archive.enabled,
            "archive_default_enabled": False,
            "archive_thinking": "disabled",
            "event_journal": True,
            "journal_generated_by_model": False,
            "wxid_known": False,
            "images_supported": True,
            "image_input_enabled": settings.images.enabled,
            "image_fields_reserved": False,
            "semantic_recognition_verified": False,
            "tool_bodies_persisted": False,
            "retrieval": False,
            "exposes_path": False,
        },
        dependencies=[],
        status=status,
        status_detail=detail,
    )


def retriever_descriptor(
    settings: Settings, retriever: SqliteKeywordRetriever
) -> ComponentDescriptor:
    """检索组件描述：只声明真实实现的能力，不把关键词检索说成语义检索。"""

    memory = settings.memory
    available = retriever.available
    return ComponentDescriptor(
        id=RETRIEVER_COMPONENT_ID,
        type=ComponentType.RETRIEVER,
        version=__version__,
        description=(
            "回答前检索：按 Agent/会话范围查 L4 明确保存、事件日记与原文来源；"
            "英文按词、中文按二元组做参数化关键词匹配，命中数与时间排序，只读不写。"
        ),
        config_fields=[
            ConfigFieldDescription(
                name="retrieval_limit",
                type="number",
                required=False,
                default=memory.retrieval_limit,
                description="回答前检索最多返回的命中条数。",
            ),
            ConfigFieldDescription(
                name="retrieval_item_chars",
                type="number",
                required=False,
                default=memory.retrieval_item_chars,
                description="单条命中预览的字符上限。",
            ),
            ConfigFieldDescription(
                name="retrieval_total_chars",
                type="number",
                required=False,
                default=memory.retrieval_total_chars,
                description="注入模型的检索参考上下文总字符上限。",
            ),
        ],
        capabilities={
            "persistent": False,
            "read_only": True,
            "backend": "sqlite_like_keywords",
            "vector_search": False,
            "embedding": False,
            "llm_rerank": False,
            "chinese_tokenization": "bigram",
            "searches_layers": ["L4", "journal", "raw"],
            "max_results": memory.retrieval_limit,
            "item_chars": memory.retrieval_item_chars,
            "total_chars": memory.retrieval_total_chars,
            "exposes_path": False,
        },
        dependencies=[],
        status=(
            ComponentStatus.AVAILABLE if available else ComponentStatus.NOT_INITIALIZED
        ),
        status_detail=(
            f"{PHASE}：只读关键词检索，不调用模型、不写库。"
            if available
            else "记忆存储未就绪：检索不可用（不会退化成‘没命中’）。"
        ),
    )


def tool_descriptor(tool: Any) -> ComponentDescriptor:
    """把真实工具登记为组件；能力只写实际具备的行为。

    `remember` 只确认/复述**已由编排保存**的授权结果（不写库）；
    L4 的写入发生在编排的保存事务里，不是工具副作用。
    """

    spec = tool.spec()
    writes = False
    web_backend = getattr(tool, "backend", None)
    enabled = web_backend is None or spec.name == "list_files" or web_backend.settings.enabled
    return ComponentDescriptor(
        id=f"tool.{spec.name}",
        type=ComponentType.TOOL,
        version=__version__,
        description=spec.description,
        config_fields=[],
        capabilities={
            "read_only": not writes,
            "side_effects": writes,
            "writes_layer": "L4" if writes else None,
            "parameters": dict(spec.parameters),
            "temporary_result": bool(getattr(tool, "ephemeral_result", False)),
            "network_execution": spec.name in {"web_search","read_website","read_file"},
            "enabled": enabled,
        },
        dependencies=[],
        status=ComponentStatus.AVAILABLE if enabled else ComponentStatus.NOT_INITIALIZED,
        status_detail=(f"{PHASE}：工具实现存在，按配置执行，有超时与参数校验。"
            if enabled else "外部资料工具实现存在，默认关闭；未验证远端连通性。"),
    )


def build_registry(
    settings: Settings,
    *,
    provider: Optional[DeepSeekModelProvider] = None,
    tools: Optional[ToolRegistry] = None,
    channel: Optional[OneBotChannelServer] = None,
    policy: Optional[WindowConversationPolicy] = None,
    memory: Optional[SqliteMemoryRepository] = None,
    retriever: Optional[SqliteKeywordRetriever] = None,
) -> ComponentRegistry:
    registry = ComponentRegistry()
    resolved_provider = provider or DeepSeekModelProvider(
        settings=settings.provider,
        model=settings.thinking.model,
    )
    registry.register(
        descriptor=model_provider_descriptor(settings.provider, resolved_provider),
        instance=resolved_provider,
    )

    if tools is not None:
        for spec in tools.specs():
            tool = tools.get(spec.name)
            registry.register(descriptor=tool_descriptor(tool), instance=tool)

    if channel is not None:
        registry.register(
            descriptor=channel_descriptor(settings, channel),
            instance=channel,
        )

    if policy is not None:
        registry.register(
            descriptor=policy_descriptor(settings, policy),
            instance=policy,
        )

    if memory is not None:
        registry.register(
            descriptor=memory_descriptor(settings, memory),
            instance=memory,
        )

    if retriever is not None:
        registry.register(
            descriptor=retriever_descriptor(settings, retriever),
            instance=retriever,
        )

    return registry


def resolve_composition(
    agent: AgentSettings, registry: ComponentRegistry
) -> List[Dict[str, Any]]:
    """把配置里的槽位绑定解析成“真实状态 + 未就绪原因”。"""

    slots: List[Dict[str, Any]] = []
    configured = dict(agent.composition)

    for slot, expected_type in SLOT_TYPES.items():
        component_id: Optional[str] = configured.pop(slot, None)
        if not component_id:
            slots.append(
                {
                    "slot": slot,
                    "expected_type": expected_type.value,
                    "component_id": None,
                    "status": ComponentStatus.NOT_INITIALIZED,
                    "status_detail": "配置中未绑定组件 ID。",
                    "capabilities": {},
                }
            )
            continue

        registered = registry.get_optional(component_id)
        if registered is None:
            slots.append(
                {
                    "slot": slot,
                    "expected_type": expected_type.value,
                    "component_id": component_id,
                    "status": ComponentStatus.NOT_INITIALIZED,
                    "status_detail": f"配置引用的组件 ID 未登记：{component_id}",
                    "capabilities": {},
                }
            )
            continue

        if registered.descriptor.type != expected_type:
            slots.append(
                {
                    "slot": slot,
                    "expected_type": expected_type.value,
                    "component_id": component_id,
                    "status": ComponentStatus.NOT_INITIALIZED,
                    "status_detail": (
                        f"组件类型不匹配：槽位需要 {expected_type.value}，"
                        f"实际为 {registered.descriptor.type.value}"
                    ),
                    "capabilities": {},
                }
            )
            continue

        slots.append(
            {
                "slot": slot,
                "expected_type": expected_type.value,
                "component_id": component_id,
                "status": registered.descriptor.status,
                "status_detail": registered.descriptor.status_detail,
                "capabilities": dict(registered.descriptor.capabilities),
            }
        )

    # 配置里出现但代码不认识的槽位：如实报出，不静默忽略。
    for slot, component_id in configured.items():
        slots.append(
            {
                "slot": slot,
                "expected_type": "unknown",
                "component_id": component_id,
                "status": ComponentStatus.NOT_INITIALIZED,
                "status_detail": "配置中的槽位不在首版装配映射内。",
                "capabilities": {},
            }
        )

    return slots


def forbidden_parameter_keys() -> List[str]:
    return sorted(FORBIDDEN_PARAMETER_KEYS)
