"""进程装配：把配置变成可运行的对象图。

`build_runtime()` 是唯一把 settings、组件注册表、工具注册表、模型客户端、
会话存储与 Agent 循环串起来的地方；API 层只读这些对象，不自行创建。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Tuple

from pero.agent.background import ConversationBackground, DeepBackground
from pero.agent.binding import AgentBinding, BindingResolution, unresolved
from pero.agent.loop import AgentLoop
from pero.agent.orchestrator import Orchestrator
from pero.agent.policy import WindowConversationPolicy
from pero.agent.sessions import SessionStore
from pero.channels.onebot11.server import OneBotChannelServer
from pero.channels.onebot11.file_fetch import BridgeFileIngress
from pero.components.bootstrap import build_registry
from pero.components.registry import ComponentRegistry
from pero.components.schemas import ComponentType
from pero.core.settings import AgentSettings, Settings, load_settings, repo_root
from pero.configuration.view import RuntimeConfiguration
from pero.plugins.installer import PluginInstaller
from pero.configuration.credentials import CredentialResolver
from pero.memory.repository import SqliteMemoryRepository
from pero.memory.retriever import SqliteKeywordRetriever
from pero.memory.semantic import SemanticIndex
from pero.memory.archive import ArchiveService
from pero.memory.chat_memory import ChatMemoryService
from pero.api.events import EventBus
from pero.providers.deepseek.client import DeepSeekModelProvider
from pero.tools.builtin import GetCurrentTimeTool
from pero.tools.executor import ToolExecutor
from pero.tools.amap import install_network_log_redaction
from pero.tools.amap_tools import AmapBackend, AmapTool
from pero.tools.memory_tools import RecallTool, RememberTool
from pero.tools.profile import LookProfileTool
from pero.tools.registry import ToolRegistry
from pero.tools.web import WebBackend, WebTool
from pero.tools.files import FileBackend, FileTool
from pero.resources.catalog import ResourceCatalog
from pero.resources.images import ImageBackend
from pero.resources.stickers import StickerAssetStore
from pero.tools.images import ImageTool
from pero.tools.stickers import StickerBackend, StickerTool
from pero.channels.image_ingress import ImageIngress


@dataclass
class Runtime:
    settings: Settings
    components: ComponentRegistry
    tools: ToolRegistry
    provider: DeepSeekModelProvider
    executor: ToolExecutor
    sessions: SessionStore
    loop: AgentLoop
    channel: OneBotChannelServer
    policy: WindowConversationPolicy
    memory: SqliteMemoryRepository
    retriever: SqliteKeywordRetriever
    orchestrator: Orchestrator
    archive: Optional[ArchiveService] = None
    web: Optional[WebBackend] = None
    files: Optional[FileBackend] = None
    images: Optional[ImageBackend] = None
    image_ingress: Optional[ImageIngress] = None
    background: Optional[ConversationBackground] = None
    l10: Optional[DeepBackground] = None
    events: Optional[EventBus] = None
    semantic: Optional[SemanticIndex] = None
    amap: Optional[AmapBackend] = None
    stickers: Optional[StickerBackend] = None
    file_ingress: Optional[BridgeFileIngress] = None
    configuration: Optional[RuntimeConfiguration] = None
    credentials: Optional[CredentialResolver] = None
    chat_memory: Optional[ChatMemoryService] = None
    plugins: Optional[PluginInstaller] = None

    async def aclose(self) -> None:
        await self.channel.stop()
        if self.image_ingress is not None:
            await self.image_ingress.stop()
        await self.orchestrator.stop()
        for background in (self.background,self.l10):
            if background is not None and hasattr(background,'flush_persistence'):
                await background.flush_persistence()
        if self.archive is not None:
            await self.archive.stop()
        if self.amap is not None:
            await self.amap.aclose()
        if self.file_ingress is not None:
            await self.file_ingress.aclose()
        if self.web is not None:
            await self.web.aclose()
        if self.images is not None:
            await self.images.aclose()
        if self.files is not None:
            await self.files.aclose()
        await self.channel.stop()
        await self.provider.aclose()
        if self.credentials is not None:await self.credentials.close()
        if self.plugins is not None:await self.plugins.aclose()
        await self.memory.aclose()


def build_tool_registry(memory: Optional[SqliteMemoryRepository] = None, web=None,
                        files=None, images=None, l3_repository=None) -> ToolRegistry:
    """注册真实工具：只读时间 + 记忆工具（绑定存储时才注册 remember/recall）。"""

    registry = ToolRegistry()
    registry.register(GetCurrentTimeTool())
    if images is not None:
        registry.register(ImageTool("look_image",images))
        registry.register(ImageTool("list_images",images))
    if files is not None:
        registry.register(FileTool("list_files", files))
        registry.register(FileTool("read_file", files))
    if web is not None:
        registry.register(WebTool("web_search", web))
        registry.register(WebTool("read_website", web))
    if memory is not None:
        registry.register(RememberTool())
        registry.register(RecallTool())
    # P15：look_profile 只在 L3 开启时注册（读真实人物事实，不是拼原文）。
    if l3_repository is not None and getattr(l3_repository.settings, "l3_enabled", False):
        registry.register(LookProfileTool(l3_repository))
    return registry


def build_runtime(settings: Optional[Settings] = None) -> Runtime:
    resolved = settings or load_settings()
    # 网络层日志脱敏（httpx/httpcore）：进程级常驻过滤器，把 key= 查询参数
    # 脱敏为 ***；幂等安装，不随请求切换日志级别（P11-R01）。
    install_network_log_redaction()
    provider = DeepSeekModelProvider(
        settings=resolved.provider,
        model=resolved.thinking.model,
    )
    policy = WindowConversationPolicy(
        resolved.orchestration,
        max_text_chars=resolved.runtime.max_user_chars,
        allow_images=resolved.images.enabled,
    )
    channel = OneBotChannelServer(resolved.channels.onebot11)
    # 独立存储根：初始化失败不阻止进程启动，但状态如实标记，
    # 编排会在入队资格检查里明确拒绝（memory_unavailable），不会假装已保存。
    memory = SqliteMemoryRepository(resolved.memory)
    # 回答前检索：本阶段唯一实现，直接读同一个存储（不建第二份事实）。
    retriever = SqliteKeywordRetriever(memory, resolved.memory)
    web = WebBackend(resolved.web, resolved.memory.data_root())
    catalog = ResourceCatalog(resolved.memory.data_root() / "resources",
        max_bytes=resolved.files.max_bytes, max_total_bytes=resolved.files.max_total_bytes,
        allow_images=True,max_image_bytes=resolved.images.max_bytes)
    files = FileBackend(resolved.files, catalog)
    images = ImageBackend(resolved.images,catalog)
    tools = build_tool_registry(memory, web, files, images, l3_repository=memory)
    from pero.knowledge.service import knowledge_for
    from pero.knowledge.tools import KnowledgeTool
    knowledge=knowledge_for(resolved)
    for name in ('search_knowledge','read_knowledge'):tools.register(KnowledgeTool(name,knowledge))
    components = build_registry(
        resolved,
        provider=provider,
        tools=tools,
        channel=channel,
        policy=policy,
        memory=memory,
        retriever=retriever,
    )
    executor = ToolExecutor(
        tools,
        timeout_seconds=resolved.runtime.tool_timeout_seconds,
        max_calls_per_run=resolved.runtime.max_tool_calls,
        max_result_chars=resolved.runtime.max_tool_result_chars,
    )
    sessions = SessionStore(max_history_messages=resolved.runtime.max_history_messages)
    # P15：SSE 事件总线（六类真实事件；有界队列，慢客户端不阻塞主链路）。
    events = EventBus(
        queue_max=resolved.events.queue_max,
        buffer_limit=resolved.events.buffer_limit,
        payload_max_chars=resolved.events.payload_max_chars,
    )
    installer = PluginInstaller(
        resolved.memory.data_root() / 'plugins',
        tools,
        events=events,
        timeout_seconds=resolved.runtime.tool_timeout_seconds,
        source_roots=(repo_root(), resolved.memory.data_root() / 'plugin-imports'),
        worker_python=resolved.plugins.worker_python,
        worker_timeout=resolved.plugins.worker_timeout_seconds,
        quota_mb=resolved.plugins.data_quota_mb,
        network_hosts=resolved.plugins.network_hosts,
        container_image=resolved.plugins.container_image,
    )
    loop = AgentLoop(
        background_jobs=installer.jobs,
        provider=provider,
        executor=executor,
        tool_registry=tools,
        limits=resolved.runtime,
        event_bus=events,
    )
    orchestrator = Orchestrator(
        settings=resolved,
        sessions=sessions,
        loop=loop,
    )
    runtime = Runtime(
        settings=resolved,
        components=components,
        tools=tools,
        provider=provider,
        executor=executor,
        sessions=sessions,
        loop=loop,
        channel=channel,
        policy=policy,
        memory=memory,
        retriever=retriever,
        orchestrator=orchestrator,
        plugins=installer,
    )
    runtime.configuration = RuntimeConfiguration(resolved, tools, resolved.memory.data_root() / 'configuration' / 'applied.json')
    from pero.access import service_for
    orchestrator.set_private_access(service_for(runtime) if runtime.memory.available else None)
    runtime.configuration.scope_resolver = lambda: tuple(
        scope['conversation_id'] for scope in service_for(runtime).private_scopes()
    ) if runtime.memory.available else ()
    orchestrator.set_plugin_jobs(installer.jobs)
    orchestrator.set_configuration(runtime.configuration)
    runtime.credentials=CredentialResolver(runtime.configuration,resolved.provider,resolved.thinking.model)
    provider.credential_resolver=runtime.credentials
    web.credential_resolver=runtime.credentials
    files.credential_resolver=runtime.credentials
    # 按 agent_id 解析模型；未配置/缺凭据如实返回，不偷偷回退默认 Agent。
    orchestrator.set_provider_resolver(
        lambda agent_id: _resolve_agent_provider(runtime, agent_id)
    )
    # 按 agent_id 解析通道与会话策略（注册表驱动，不做全局实例兜底）。
    orchestrator.set_binding_resolver(
        lambda agent_id: resolve_agent_binding(runtime, agent_id)
    )
    # 通道回调只做非阻塞入队；编排器未启用时 enqueue 立即返回。
    channel.set_event_handler(orchestrator.enqueue)
    runtime.archive = ArchiveService(resolved.archive, resolved.agents,
        lambda agent_id: _resolve_archive(runtime, agent_id))
    runtime.web = web
    runtime.files = files
    runtime.images = images
    runtime.image_ingress=ImageIngress(resolved.images,catalog,resolved.orchestration.routes,
        resolved.orchestration.allowed_conversations,orchestrator.enqueue)
    runtime.image_ingress._private_access = getattr(orchestrator, "_private_access", None)
    channel.set_media_handler(runtime.image_ingress.enqueue)
    orchestrator.set_file_backend(files)
    orchestrator.set_image_backend(images)
    orchestrator.set_archive_service(runtime.archive)
    # P11：有界 L1 短期背景（进程内存，重启即空）；允许的群消息未唤醒也更新。
    if resolved.conversation.background_enabled:
        background = ConversationBackground(
            max_turns=resolved.conversation.background_max_turns,
            max_chars=resolved.conversation.background_max_chars,
            idle_seconds=resolved.conversation.background_idle_seconds,
            max_conversations=resolved.conversation.background_max_conversations,
        )
        runtime.background = background
        orchestrator.set_background(background)
        from pero.agent.background_persistence import attach
        attach(background,resolved.memory.data_root()/'background'/'L1.json')
    # P15：L10 深缓冲（默认关闭）——独立于 L1 的第二份有界数据，承接 L1 溢出。
    if resolved.conversation.l10_enabled:
        l10 = DeepBackground(
            max_turns=resolved.conversation.l10_max_turns,
            max_chars=resolved.conversation.l10_max_chars,
            idle_seconds=resolved.conversation.l10_idle_seconds,
            max_conversations=resolved.conversation.l10_max_conversations,
        )
        runtime.l10 = l10
        orchestrator.set_l10(l10)
        from pero.agent.background_persistence import attach
        attach(l10,resolved.memory.data_root()/'background'/'L10.json')
        if runtime.background is not None:
            runtime.background.set_overflow_sink(l10.absorb)
    # v6：聊天流水后台（100 条归档 + L3 候选）。归档按 settings.archive.enabled，
    # L3 按 memory.l3_enabled；两者都关闭时服务不会发起任何后台模型调用。
    chat_memory = ChatMemoryService(resolved)
    runtime.chat_memory = chat_memory
    orchestrator.set_chat_memory(chat_memory)
    # P15：语义索引（默认关闭、默认不联网建索引；不可用明确降级关键词）。
    if resolved.embeddings.enabled:
        semantic = SemanticIndex(resolved.embeddings, memory)
        runtime.semantic = semantic
        retriever.set_semantic(semantic)
        orchestrator.set_semantic_index(semantic)
    orchestrator.set_event_bus(events)
    runtime.events = events
    # P15-R01：component.status 接真实通道连接生命周期（connected/disconnected）。
    if runtime.events is not None:
        def _channel_status(state: str, *, conn_id: str = "", account: str = "",
                            error: str = "") -> None:
            # 组件状态是**通道级全局事件**（不属于某个 Agent），广播给全部订阅。
            runtime.events.publish(
                "component.status",
                agent_id="",
                data={"component": "channel.onebot11", "state": state,
                      "account": account, "conn_id": conn_id, "error": error},
            )
        channel.set_status_emitter(_channel_status)
    # P11：高德后端（默认关闭）；启用时注册三个真实工具并注入编排器。
    if resolved.amap.enabled:
        amap = AmapBackend(resolved.amap, resolved.memory.data_root())
        runtime.amap = amap
        amap.credential_resolver=runtime.credentials
        orchestrator.set_amap(amap)
        tools.register(AmapTool("amap_weather", amap))
        tools.register(AmapTool("amap_places", amap))
        tools.register(AmapTool("amap_route", amap))
    # P12：表情资产后端（独立新库）。编排器始终注入——标记剥离不依赖开关；
    # 工具只在 enabled 时注册；发送还要求 auto_send_enabled。
    sticker_store = StickerAssetStore(resolved.memory.data_root() / "sticker-assets.sqlite3")
    sticker_store.initialize()
    stickers = StickerBackend(resolved.stickers, catalog, sticker_store)
    runtime.stickers = stickers
    orchestrator.set_stickers(stickers)
    if resolved.stickers.enabled:
        tools.register(StickerTool("list_stickers", stickers))
        tools.register(StickerTool("save_sticker", stickers))
        tools.register(StickerTool("rename_sticker", stickers))
    # P13-R01：桥文件字节补传（默认关闭）。入队在**接收阶段**（通道回调），
    # 模型批次不再有第二入口；worker 生命周期由 lifespan 管理。
    if resolved.bridge_context.enabled:
        ingress = BridgeFileIngress(
            resolved.bridge_context,
            catalog,
            channel,
            routes={
                (route.account, route.conversation_type, route.target): route.agent_id
                for route in resolved.orchestration.routes
            },
            allowed_conversations=resolved.orchestration.allowed_conversations,
        )
        runtime.file_ingress = ingress
        ingress._private_access = getattr(orchestrator, "_private_access", None)
        # 接收侧同步入队：可信账号 + 允许会话 + Agent 路由验证在回调内完成。
        channel.set_file_offer_handler(ingress.offer_from_event)
    # P13-R01：引用原图查询（默认关闭）；装配后引用命中才经桥只读 RPC 取原件。
    if resolved.bridge_context.enabled and resolved.bridge_context.quote_image_fetch_enabled:
        def _quote_fetcher(account_id, target_ref, image_md5, refer_time, query_id, conversation_type='group'):
            return channel.query_quote_image(
                action=resolved.bridge_context.quote_image_action,
                query_id=query_id,
                account_id=account_id,
                target_ref=target_ref,
                conversation_type=conversation_type,
                image_md5=image_md5,
                refer_time=refer_time,
                timeout=resolved.bridge_context.fetch_timeout_seconds,
            )
        images.set_quote_fetcher(_quote_fetcher)
    return runtime


def _resolve_archive(runtime: Runtime, agent_id: str):
    agent = runtime.settings.find_agent(agent_id)
    if agent is None:
        return None, None, "unknown_agent"
    memory, code, _detail = resolve_memory(runtime, agent)
    if memory is None:
        return None, None, code
    provider, code, _detail = resolve_model_provider(runtime, agent)
    return memory, provider, code


def resolve_agent_binding(runtime: Runtime, agent_id: str) -> BindingResolution:
    """按 Agent 解析通道与 `conversation_policy`，任一项不成立即明确拒绝。

    P04 只有一种策略实现，但绑定检查照样生效：未配置 Agent、槽位未绑定、
    组件未登记、类型不符、实例不可用都不会退回"全局默认实例"。
    """

    agent = runtime.settings.find_agent(agent_id)
    if agent is None:
        return unresolved("agent_unknown", f"未配置的 Agent：{agent_id}")

    channel, channel_code, channel_detail = resolve_channel(runtime, agent)
    if channel is None:
        return unresolved(channel_code or "channel_unbound", channel_detail)

    policy, policy_code, policy_detail = resolve_policy(runtime, agent)
    if policy is None:
        return unresolved(policy_code or "policy_unbound", policy_detail)

    memory, memory_code, memory_detail = resolve_memory(runtime, agent)
    if memory is None and memory_code != "memory_unbound":
        # 绑定存在但不可用：明确拒绝，不退化成"假装持久化"。
        return unresolved(memory_code or "memory_not_ready", memory_detail)

    retriever, retriever_code, retriever_detail = resolve_retriever(runtime, agent)
    if retriever is None and retriever_code != "retriever_unbound":
        # 绑定了检索但不可用：明确拒绝，不偷偷改用"无检索"继续答。
        return unresolved(retriever_code or "retriever_not_ready", retriever_detail)

    return BindingResolution(
        binding=AgentBinding(
            agent_id=agent.agent_id,
            channel_component_id=str(agent.composition.get("channel")),
            policy_component_id=str(agent.composition.get("conversation_policy")),
            channel=channel,
            policy=policy,
            memory=memory,
            memory_component_id=(
                str(agent.composition.get("memory_repository")) if memory else None
            ),
            memory_detail=memory_detail,
            retriever=retriever,
            retriever_component_id=(
                str(agent.composition.get("retriever")) if retriever else None
            ),
            retriever_detail=retriever_detail,
        )
    )


def resolve_retriever(
    runtime: Runtime, agent: AgentSettings
) -> Tuple[Optional[SqliteKeywordRetriever], Optional[str], str]:
    """按 Agent 解析 `retriever` 槽位；未绑定即明确不检索，不退回全局库。"""

    component_id = agent.composition.get("retriever")
    if not component_id:
        return None, "retriever_unbound", "该 Agent 未绑定 retriever 槽位"
    registered = runtime.components.get_optional(component_id)
    if registered is None:
        return (
            None,
            "retriever_unknown",
            f"该 Agent 绑定的检索组件未登记：{component_id}",
        )
    if registered.descriptor.type != ComponentType.RETRIEVER:
        return (
            None,
            "retriever_type_mismatch",
            f"槽位需要 retriever，实际为 {registered.descriptor.type.value}",
        )
    instance = registered.instance
    if not isinstance(instance, SqliteKeywordRetriever):
        return None, "retriever_not_ready", "该组件实例不是可用的检索实现"
    if not instance.available:
        return None, "retriever_unavailable", "检索组件不可用（记忆存储未就绪）"
    return instance, None, ""


def resolve_memory(
    runtime: Runtime, agent: AgentSettings
) -> Tuple[Optional[SqliteMemoryRepository], Optional[str], str]:
    """按 Agent 解析 `memory_repository` 槽位。

    未绑定 → `memory_unbound`（调用方据此进入"仅内存"模式，状态如实说明）；
    绑定但未登记 / 类型不符 / 实例不可用 → 明确错误码，绝不退回全局默认库。
    """

    component_id = agent.composition.get("memory_repository")
    if not component_id:
        return None, "memory_unbound", "该 Agent 未绑定 memory_repository 槽位"
    registered = runtime.components.get_optional(component_id)
    if registered is None:
        return None, "memory_unknown", f"该 Agent 绑定的记忆组件未登记：{component_id}"
    if registered.descriptor.type != ComponentType.MEMORY_REPOSITORY:
        return (
            None,
            "memory_type_mismatch",
            f"槽位需要 memory_repository，实际为 {registered.descriptor.type.value}",
        )
    instance = registered.instance
    if not isinstance(instance, SqliteMemoryRepository):
        return None, "memory_not_ready", "该组件实例不是可用的记忆仓库"
    if not instance.available:
        return (
            None,
            "memory_unavailable",
            f"存储未就绪：{instance.error_detail or '未知原因'}",
        )
    return instance, None, ""


def _resolve_agent_provider(
    runtime: Runtime, agent_id: str
) -> Tuple[Optional[DeepSeekModelProvider], Optional[str], str]:
    agent = runtime.settings.find_agent(agent_id)
    if agent is None:
        return None, "unknown_agent", f"未配置的 Agent：{agent_id}"
    return resolve_model_provider(runtime, agent)


def resolve_channel(runtime: Runtime, agent: AgentSettings) -> Tuple[Optional[OneBotChannelServer], Optional[str], str]:
    """按 Agent 装配解析通道组件；未绑定/未登记/类型不符/实例不可用如实返回。"""

    component_id = agent.composition.get("channel")
    if not component_id:
        return None, "channel_unbound", "该 Agent 未绑定 channel 槽位"
    registered = runtime.components.get_optional(component_id)
    if registered is None:
        return None, "channel_unknown", f"该 Agent 绑定的通道组件未登记：{component_id}"
    if registered.descriptor.type != ComponentType.CHANNEL:
        return (
            None,
            "channel_type_mismatch",
            f"槽位需要 channel，实际为 {registered.descriptor.type.value}",
        )
    instance = registered.instance
    if not isinstance(instance, OneBotChannelServer):
        return None, "channel_not_ready", "该组件实例不是可用的通道适配器"
    return instance, None, ""


def resolve_policy(
    runtime: Runtime, agent: AgentSettings
) -> Tuple[Optional[WindowConversationPolicy], Optional[str], str]:
    """按 Agent 装配解析 `conversation_policy` 槽位；不做全局实例兜底。"""

    component_id = agent.composition.get("conversation_policy")
    if not component_id:
        return None, "policy_unbound", "该 Agent 未绑定 conversation_policy 槽位"
    registered = runtime.components.get_optional(component_id)
    if registered is None:
        return None, "policy_unknown", f"该 Agent 绑定的策略组件未登记：{component_id}"
    if registered.descriptor.type != ComponentType.CONVERSATION_POLICY:
        return (
            None,
            "policy_type_mismatch",
            f"槽位需要 conversation_policy，实际为 {registered.descriptor.type.value}",
        )
    instance = registered.instance
    if not isinstance(instance, WindowConversationPolicy):
        return None, "policy_not_ready", "该组件实例不是可用的会话策略"
    return instance, None, ""


def resolve_model_provider(
    runtime: Runtime, agent: AgentSettings
) -> Tuple[Optional[DeepSeekModelProvider], Optional[str], str]:
    """按 Agent 自己的装配解析模型组件，不硬编码只支持 default。

    返回 ``(provider, error_code, message)``；成功时错误码为 None。
    """

    component_id = agent.composition.get("model_provider")
    if not component_id:
        return None, "model_provider_unbound", "该 Agent 未绑定 model_provider 槽位"
    registered = runtime.components.get_optional(component_id)
    if registered is None:
        return None, "model_provider_unknown", f"该 Agent 绑定的组件未登记：{component_id}"
    if registered.descriptor.type != ComponentType.MODEL_PROVIDER:
        return (
            None,
            "model_provider_type_mismatch",
            f"槽位需要 model_provider，实际为 {registered.descriptor.type.value}",
        )
    instance = registered.instance
    if not isinstance(instance, DeepSeekModelProvider):
        return None, "model_provider_not_ready", "该组件实例不是可用的模型客户端"
    if not instance.api_key_configured:
        return (
            None,
            "api_key_missing",
            f"未配置模型凭据：环境变量 {instance.api_key_env} 为空",
        )
    return instance, None, ""
