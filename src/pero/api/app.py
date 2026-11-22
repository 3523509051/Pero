"""FastAPI 应用：只读状态接口 + 本地内存会话入口。

所有响应都由实际进程状态、配置、组件注册表与真实运行结果生成：
没有演示聊天、没有随机记忆，也不把计划能力说成已完成。
缺凭据时返回明确的不可用结果，不伪造模型回复。
"""

from __future__ import annotations

import asyncio
import logging
import os
import platform
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import base64
from pathlib import Path

from fastapi import Depends, FastAPI, HTTPException, Query, Request
from pero.configuration.auth import require_internal
from pero.knowledge.service import KnowledgeError, KnowledgeService

#: 知识库实例（按数据根懒加载；与主记忆库分离）。
_KNOWLEDGE: dict = {}


def _knowledge_for(settings) -> KnowledgeService:
    from pero.knowledge.service import knowledge_for
    return knowledge_for(settings)
from fastapi.responses import JSONResponse, Response, StreamingResponse
from pero.tools.context import ToolContext, tool_scope
from pero.resources.catalog import ResourceCatalogError
from pero.api.events import format_sse

from pero import PHASE, __version__
from pero.agent.loop import (
    HistoryLimitError,
    RunResult,
    SessionBusyError,
    UserInputTooLongError,
)
from pero.agent.sessions import AgentSession
from pero.api.schemas import (
    AgentSummary,
    AgentsResponse,
    CapabilitiesResponse,
    ChannelStatusView,
    ChannelsResponse,
    ComponentTypeInfo,
    ComponentView,
    ComponentsResponse,
    CompositionResponse,
    HealthResponse,
    LastRunView,
    MemoryAliasView,
    MemoryIdentitiesResponse,
    MemoryIdentityCandidate,
    MemoryLayerInfo,
    MemoryLayersResponse,
    MemoryLayerView,
    MemoryRecordDetailResponse,
    MemoryRecordItem,
    MemoryRecordsResponse,
    MemorySearchItem,
    MemorySearchResponse,
    MemoryStorageView,
    ModelClientInfo,
    OrchestrationResponse,
    OrchestrationStatusView,
    PurposeInfo,
    RunResponse,
    RunToolView,
    RuntimeLimitsView,
    SessionCreateResponse,
    SessionInfoResponse,
    SessionMessageRequest,
    SlotView,
    ThinkingPolicyInfo,
)
from pero.components.bootstrap import (
    SLOT_TYPES,
    forbidden_parameter_keys,
    resolve_composition,
)
from pero.components.registry import ComponentRegistry
from pero.components.schemas import (
    COMPONENT_TYPE_DESCRIPTIONS,
    ComponentStatus,
    ComponentType,
)
from pero.core.contracts import MEMORY_LAYER_CATALOG
from pero.core.settings import AgentSettings, Settings, load_settings
from pero.memory.models import QUERYABLE_LAYERS
from pero.memory.p15_store import MemoryQueryErrorP15
from pero.memory.repository import MemoryQueryError, SqliteMemoryRepository
from pero.agent.orchestrator import TurnBlockedError
from pero.providers.deepseek.client import DeepSeekModelProvider
from pero.providers.deepseek.request_builder import THINKING_DISABLED, describe_purposes
from pero.runtime import (
    Runtime,
    build_runtime,
    resolve_channel,
    resolve_memory,
    resolve_model_provider,
    resolve_retriever,
)

logger = logging.getLogger(__name__)

API_VERSION = "v1"


# --------------------------------------------------------------------------- #
# 视图辅助
# --------------------------------------------------------------------------- #


def _thinking_policy(settings: Settings, configuration=None) -> ThinkingPolicyInfo:
    profiles,policy=configuration.snapshot('*').model_configuration() if configuration else ({},{})
    profile=profiles.get(policy.get('default_profile','default'),{})
    return ThinkingPolicyInfo(
        model=profile.get('model',settings.thinking.model),
        always_disabled=False,
        overridable=True,
        protocol=settings.thinking.protocol,
        wire_field="thinking",
        wire_value={'type':'enabled' if profile.get('reply_thinking',False) else 'disabled'},
        sdk_location="extra_body.thinking（仅 OpenAI 兼容 SDK 的包装层级）",
        forbidden_overrides=forbidden_parameter_keys(),
        purposes=[PurposeInfo(**{**item, 'thinking':{'type':(
            'enabled' if (profile.get('reply_thinking',False) if item['purpose'] in {'main_reply','tool_continuation','tool_retry'}
                          else policy.get('tool_thinking_default',False) if item['purpose']=='tool_analysis' else False) else 'disabled')}})
                  for item in describe_purposes()],
    )


def _runtime_limits(runtime: Runtime) -> RuntimeLimitsView:
    limits = runtime.settings.runtime
    return RuntimeLimitsView(
        max_model_calls=limits.max_model_calls,
        max_tool_calls=limits.max_tool_calls,
        tool_timeout_seconds=limits.tool_timeout_seconds,
        max_history_messages=limits.max_history_messages,
        max_user_chars=limits.max_user_chars,
        max_tool_result_chars=limits.max_tool_result_chars,
    )


def _model_client_info(runtime: Runtime) -> ModelClientInfo:
    provider = runtime.provider
    return ModelClientInfo(
        client_implemented=True,
        http_client="httpx",
        api_key_env=provider.api_key_env,
        api_key_configured=provider.api_key_configured,
        remote_connectivity=ComponentStatus.NOT_VERIFIED,
        streaming=False,
        auto_retry=False,
        timeout_seconds=runtime.settings.provider.request_timeout_seconds,
    )


def _component_types(registry: ComponentRegistry) -> List[ComponentTypeInfo]:
    counts = registry.count_by_type()
    return [
        ComponentTypeInfo(
            type=member,
            description=COMPONENT_TYPE_DESCRIPTIONS[member],
            registered=counts.get(member.value, 0),
        )
        for member in ComponentType
    ]


def _capability_status(runtime: Runtime) -> Dict[str, ComponentStatus]:
    """按注册表与真实配置给出各项能力状态，而不是按计划填写。"""

    registry = runtime.components
    provider_component = registry.get_optional(
        runtime.settings.default_agent().composition.get("model_provider") or ""
    )
    credentials_ok = bool(
        provider_component and provider_component.descriptor.capabilities.get("credentials_configured")
    )
    tools_ready = len(runtime.tools) > 0
    channel = runtime.channel
    if not channel.enabled:
        channel_status = ComponentStatus.NOT_CONNECTED
    elif channel.connected:
        channel_status = ComponentStatus.AVAILABLE
    else:
        channel_status = ComponentStatus.NOT_CONNECTED
    return {
        "model_request_construction": ComponentStatus.AVAILABLE,
        "model_client_implementation": ComponentStatus.AVAILABLE,
        "model_credentials": (
            ComponentStatus.AVAILABLE if credentials_ok else ComponentStatus.NOT_INITIALIZED
        ),
        "model_remote_connectivity": ComponentStatus.NOT_VERIFIED,
        "agent_loop": ComponentStatus.AVAILABLE,
        "tool_execution": ComponentStatus.AVAILABLE if tools_ready else ComponentStatus.NOT_IMPLEMENTED,
        "chat_reply": (
            ComponentStatus.AVAILABLE if credentials_ok else ComponentStatus.NOT_INITIALIZED
        ),
        "channel_implementation": ComponentStatus.AVAILABLE,
        "channel": channel_status,
        "memory": ComponentStatus.NOT_INITIALIZED,
        "retrieval": ComponentStatus.NOT_INITIALIZED,
        "frontend": ComponentStatus.NOT_IMPLEMENTED,
    }


#: P05 真正落库的层；其余层在目录里保持未初始化，不把计划能力写成已完成。
IMPLEMENTED_MEMORY_LAYERS = {"L2", "L20"}


def _memory_layers(runtime: Runtime) -> List[MemoryLayerInfo]:
    """能力目录：只有真实落库的层标 available，且与存储实际状态一致。"""

    ready = runtime.memory.available
    layers: List[MemoryLayerInfo] = []
    for layer in MEMORY_LAYER_CATALOG:
        implemented = layer["key"] in IMPLEMENTED_MEMORY_LAYERS and ready
        layers.append(
            MemoryLayerInfo(
                key=layer["key"],
                name=layer["name"],
                description=layer["description"],
                persistent=bool(layer["persistent"]),
                status=(
                    ComponentStatus.AVAILABLE
                    if implemented
                    else ComponentStatus.NOT_INITIALIZED
                ),
            )
        )
    return layers


def _agent_binding(runtime: Runtime, agent: AgentSettings) -> tuple[bool, bool]:
    """返回 ``(loop_ready, model_configured)``，只读注册表事实。"""

    component_id = agent.composition.get("model_provider")
    registered = runtime.components.get_optional(component_id) if component_id else None
    if registered is None or not isinstance(registered.instance, DeepSeekModelProvider):
        return False, False
    return True, registered.instance.api_key_configured


def _agent_channel_state(runtime: Runtime, agent: AgentSettings) -> tuple[ComponentStatus, bool]:
    """返回 ``(通道组件状态, 是否真的连着桥)``。"""

    channel, code, _ = resolve_channel(runtime, agent)
    if channel is None:
        return ComponentStatus.NOT_INITIALIZED, False
    if not channel.enabled:
        return ComponentStatus.NOT_CONNECTED, False
    if channel.connected:
        return ComponentStatus.AVAILABLE, True
    return ComponentStatus.NOT_CONNECTED, False


def _channel_views(runtime: Runtime) -> List[ChannelStatusView]:
    return [ChannelStatusView(**runtime.channel.status())]


def _orchestration_view(runtime: Runtime) -> OrchestrationStatusView:
    return OrchestrationStatusView(**runtime.orchestrator.status())


def _agent_summary(runtime: Runtime, agent: AgentSettings) -> AgentSummary:
    slots = resolve_composition(agent, runtime.components)
    bound = [slot["component_id"] for slot in slots if slot["component_id"]]
    unbound = [slot["slot"] for slot in slots if not slot["component_id"]]
    not_ready = [
        slot["slot"]
        for slot in slots
        if slot["component_id"] and slot["status"] != ComponentStatus.AVAILABLE
    ]
    loop_ready, model_configured = _agent_binding(runtime, agent)
    channel_status, channel_connected = _agent_channel_state(runtime, agent)
    if not loop_ready:
        detail = f"{PHASE}：该 Agent 未绑定可用的模型客户端，会话请求会被明确拒绝。"
    elif not model_configured:
        detail = (
            f"{PHASE}：循环与客户端已就绪，但模型凭据未配置；"
            "会话请求会返回不可用结果。"
        )
    else:
        detail = (
            f"{PHASE}：循环与客户端已就绪，凭据已配置；真实连通性未核对。"
            "通道适配只做接收与统计，不触发模型、不自动回复；记忆与检索仍缺失。"
        )
    if channel_connected:
        detail += " 当前有桥连接（事件只统计，不会进入模型）。"
    elif channel_status is ComponentStatus.NOT_CONNECTED:
        detail += " 通道未连接（默认未启用接收）。"
    return AgentSummary(
        agent_id=agent.agent_id,
        display_name=agent.display_name,
        description=agent.description,
        agent_ready=False,
        loop_ready=loop_ready,
        model_configured=model_configured,
        channel_status=channel_status,
        channel_connected=channel_connected,
        status=ComponentStatus.NOT_VERIFIED if loop_ready and model_configured else ComponentStatus.NOT_INITIALIZED,
        status_detail=detail,
        bound_component_ids=[item for item in bound if item],
        unbound_slots=unbound,
        not_ready_slots=not_ready,
    )


def _session_status(session: AgentSession) -> str:
    return "busy" if session.busy else "ready"


def _session_limits(runtime: Runtime) -> RuntimeLimitsView:
    return _runtime_limits(runtime)


def _error(status_code: int, code: str, message: str, **detail: Any) -> HTTPException:
    payload: Dict[str, Any] = {"error": code, "message": message}
    payload.update(detail)
    return HTTPException(status_code=status_code, detail=payload)


# --------------------------------------------------------------------------- #
# 应用工厂
# --------------------------------------------------------------------------- #


def create_app(
    settings: Optional[Settings] = None,
    runtime: Optional[Runtime] = None,
) -> FastAPI:
    resolved_runtime = runtime or build_runtime(settings or load_settings())
    resolved_settings = resolved_runtime.settings

    started_monotonic = time.monotonic()
    started_at = datetime.now(timezone.utc).isoformat()

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        logger.info(
            "pero %s (%s) 启动：%s:%s，组件 %d 个，工具 %d 个，凭据 %s",
            __version__,
            resolved_settings.phase,
            resolved_settings.service.host,
            resolved_settings.service.port,
            len(resolved_runtime.components),
            len(resolved_runtime.tools),
            "已配置" if resolved_runtime.provider.api_key_configured else "未配置",
        )
        from pero.access import service_for
        if resolved_runtime.memory.available:
            import asyncio
            access = service_for(resolved_runtime)
            event_loop = asyncio.get_running_loop()
            access.on_change = lambda old,new: event_loop.call_soon_threadsafe(resolved_runtime.orchestrator.private_access_changed, old, new)
            await access.initialize()
            # Access grants must be known before validating a persisted private configuration.
            def restore_private_configuration():
                configuration = resolved_runtime.configuration
                path = configuration.cache_path
                if path is not None and path.is_file():
                    import json
                    try:
                        configuration.apply(json.loads(path.read_text(encoding='utf-8-sig')))
                        configuration.load_error = None
                    except (ValueError, TypeError, OSError):
                        configuration.load_error = 'runtime_configuration_cache_invalid'
            await asyncio.to_thread(restore_private_configuration)
        if resolved_runtime.plugins is not None:
            await resolved_runtime.plugins.restore()
        await resolved_runtime.orchestrator.start()
        if resolved_runtime.archive is not None and resolved_runtime.chat_memory is None:
            await resolved_runtime.archive.start()
        if resolved_runtime.image_ingress is not None:
            await resolved_runtime.image_ingress.start()
        if resolved_runtime.amap is not None:
            await resolved_runtime.amap.start()
        if resolved_runtime.file_ingress is not None:
            await resolved_runtime.file_ingress.start()
        channel_started = await resolved_runtime.channel.start()
        kb_worker=asyncio.create_task(_knowledge_for(resolved_settings).worker())
        if not channel_started and resolved_runtime.channel.enabled:
            logger.warning("onebot11 通道启用但未能监听，详见启动错误码")
        try:
            yield
        finally:
            kb_worker.cancel()
            try:await kb_worker
            except asyncio.CancelledError:pass
            await resolved_runtime.channel.stop()
            await resolved_runtime.aclose()
            logger.info("pero 已停止监听")

    app = FastAPI(
        title="pero",
        version=__version__,
        description="pero 独立后端：只读状态接口、统一关闭思考的模型客户端与有界工具循环。",
        lifespan=lifespan,
    )
    from pero.configuration.core_api import management_router
    app.include_router(management_router(resolved_runtime))
    if resolved_runtime.plugins is not None:
        from pero.plugins.router import plugins_router
        app.include_router(plugins_router(resolved_runtime.plugins))
    from pero.configuration.core_controls import control_router
    app.include_router(control_router(resolved_runtime))
    from pero.api.internal_private import router as internal_private_router
    app.include_router(internal_private_router)  # Internal私聊APIs
    app.state.runtime = resolved_runtime
    app.state.settings = resolved_settings
    app.state.started_at = started_at
    app.state.started_monotonic = started_monotonic

    # ------------------------------------------------------------------ #
    # 只读状态
    # ------------------------------------------------------------------ #

    @app.get("/health", response_model=HealthResponse, summary="服务存活与阶段")
    def health() -> HealthResponse:
        agent = resolved_settings.default_agent()
        loop_ready, model_configured = _agent_binding(resolved_runtime, agent)
        return HealthResponse(
            draining=bool(getattr(resolved_runtime.orchestrator,'_draining',False)),
            ok=True,
            version=__version__,
            phase=resolved_settings.phase,
            agent_ready=False,
            loop_ready=loop_ready,
            model_configured=model_configured,
            default_agent_id=agent.agent_id,
            uptime_seconds=round(time.monotonic() - started_monotonic, 3),
            started_at=started_at,
            pid=os.getpid(),
            python_version=platform.python_version(),
            components_registered=len(resolved_runtime.components),
            config_loaded=resolved_settings.config_path is not None,
        )

    @app.get(
        "/api/v1/capabilities",
        response_model=CapabilitiesResponse,
        summary="API 版本、组件类型、关闭思考政策与能力状态",
    )
    async def capabilities() -> CapabilitiesResponse:
        # 间接包含 `orchestration`：状态必须在事件循环里同步组装（无 await），
        # 与 enqueue/worker/巡检同一所有者；改成 async 后不会落到线程池执行。
        return CapabilitiesResponse(
            api_version=API_VERSION,
            phase=resolved_settings.phase,
            service_version=__version__,
            component_types=_component_types(resolved_runtime.components),
            thinking_policy=_thinking_policy(resolved_settings,resolved_runtime.configuration),
            capability_status=_capability_status(resolved_runtime),
            memory_layers=_memory_layers(resolved_runtime),
            model_client=_model_client_info(resolved_runtime),
            runtime_limits=_runtime_limits(resolved_runtime),
            channels=_channel_views(resolved_runtime),
            orchestration=_orchestration_view(resolved_runtime),
        )

    @app.get(
        "/api/v1/channels",
        response_model=ChannelsResponse,
        summary="通道适配的脱敏运行元数据（实现 / 启用 / 连接分开报告）",
    )
    async def channels() -> ChannelsResponse:
        # 通道运行元数据同样由事件循环写：统一在循环里读，避免线程池迭代中被改动。
        return ChannelsResponse(
            api_version=API_VERSION,
            channels=_channel_views(resolved_runtime),
        )

    @app.get(
        "/api/v1/orchestration",
        response_model=OrchestrationResponse,
        summary="文本编排状态：开关、队列、唤醒窗口与投递计数（无正文）",
    )
    async def orchestration() -> OrchestrationResponse:
        # async 路由在事件循环中执行：状态组装与 enqueue/worker/巡检串行，
        # 读取队列/窗口/候选时不会与后台任务并发改动同一结构。
        return OrchestrationResponse(
            api_version=API_VERSION,
            orchestration=_orchestration_view(resolved_runtime),
        )

    # ------------------------------------------------------------------ #
    # P05 持久记忆（只读）
    # ------------------------------------------------------------------ #

    def _memory_for(agent_id: str) -> SqliteMemoryRepository:
        """按 Agent 的装配解析记忆仓库；未绑定/不可用给出明确错误，不用全局默认库。"""

        agent = resolved_settings.find_agent(agent_id)
        if agent is None:
            raise HTTPException(status_code=404, detail=f"未配置的 Agent：{agent_id}")
        memory, code, detail = resolve_memory(resolved_runtime, agent)
        if memory is None:
            raise HTTPException(
                status_code=409,
                detail=f"该 Agent 未绑定可用记忆仓库（{code}）：{detail}",
            )
        return memory

    def _check_time(value: Optional[str], field: str) -> Optional[str]:
        if not value:
            return None
        try:
            datetime.fromisoformat(value)
        except ValueError as exc:
            raise HTTPException(
                status_code=422,
                detail=f"{field} 必须是 ISO 8601 时间（例如 2026-11-14T00:00:00+00:00）",
            ) from exc
        return value

    @app.get(
        "/api/v1/memory/layers",
        response_model=MemoryLayersResponse,
        summary="记忆层级目录与存储初始化状态（无正文、无路径）",
    )
    async def memory_layers(
        agent_id: Optional[str] = Query(
            default=None, description="按哪个 Agent 的装配查看；默认 Agent"
        )
    ) -> MemoryLayersResponse:
        target = agent_id or resolved_settings.default_agent().agent_id
        memory = _memory_for(target)
        data = await memory.layers()
        return MemoryLayersResponse(
            api_version=API_VERSION,
            agent_id=target,
            storage=MemoryStorageView(**data["storage"]),
            layers=[MemoryLayerView(**item) for item in data["layers"]],
            counts=data["counts"],
            queryable_layers=data["queryable_layers"],
        )

    @app.get(
        "/api/v1/agents/{agent_id}/chat/window",
        summary="最近 N 条完整群聊（L1/L10/L2 是同一份流水的三个窗口）",
    )
    async def chat_window(
        agent_id: str,
        conversation_id: str = Query(..., max_length=250),
        limit: int = Query(0, ge=0, le=200),
    ):
        memory = _memory_for(agent_id)
        cid = await resource_scope(agent_id, conversation_id, None)
        # 原始聊天窗口（L1/L10 与独立聊天页使用）；L2 已改为独立交互视图，
        # 不再用"最近 N 条流水"表达，见 `/memory/l2`。
        windows = {
            "L1": int(resolved_settings.memory.chat_l1_messages),
            "L10": int(resolved_settings.memory.chat_l10_messages),
        }
        window = int(limit) or windows["L10"]
        items = await memory.chat_window(
            agent_id=agent_id, conversation_id=cid, limit=window,
            include_tools=True,
        )
        counts = await memory.chat_counts(agent_id=agent_id, conversation_id=cid)
        return {
            "api_version": API_VERSION,
            "agent_id": agent_id,
            "conversation_id": cid,
            "windows": windows,
            "requested": window,
            "returned": len(items),
            "counts": counts,
            "items": items,
        }

    @app.get(
        "/api/v1/agents/{agent_id}/chat/history",
        summary="聊天历史分页（倒序；查看更早记录不改变当前窗口）",
    )
    async def chat_history(
        agent_id: str,
        conversation_id: str = Query(..., max_length=250),
        limit: int = Query(20, ge=1, le=100),
        offset: int = Query(0, ge=0, le=100000),
        before_seq: int = Query(0, ge=0, le=10_000_000_000),
    ):
        memory = _memory_for(agent_id)
        cid = await resource_scope(agent_id, conversation_id, None)
        data = await memory.chat_history(
            agent_id=agent_id, conversation_id=cid, limit=limit, offset=offset,
            before_seq=before_seq or None,
        )
        return {
            "api_version": API_VERSION,
            "agent_id": agent_id,
            "conversation_id": cid,
            **data,
            "next_offset": offset + len(data["items"]) if data["items"] else None,
        }

    @app.get(
        "/api/v1/agents/{agent_id}/chat/status",
        summary="窗口容量、实际条数、归档游标/任务与 L3 候选状态",
    )
    async def chat_status(
        agent_id: str,
        conversation_id: str = Query(..., max_length=250),
    ):
        memory = _memory_for(agent_id)
        cid = await resource_scope(agent_id, conversation_id, None)
        counts = await memory.chat_counts(agent_id=agent_id, conversation_id=cid)
        archive = await memory.chat_archive_status(
            agent_id=agent_id, conversation_id=cid
        )
        service = getattr(resolved_runtime, "chat_memory", None)
        l2_status = await memory.l2_archive_status(
            agent_id=agent_id, conversation_id=cid
        )
        l2_config = await memory.l2_config_get(
            agent_id=agent_id, conversation_id=cid
        )
        return {
            "api_version": API_VERSION,
            "agent_id": agent_id,
            "conversation_id": cid,
            "windows": {
                "L1": int(resolved_settings.memory.chat_l1_messages),
                "L10": int(resolved_settings.memory.chat_l10_messages),
            },
            "counts": counts,
            "archive": archive,
            # L2 是独立交互视图：只报条数/阈值，不是"最近 N 条流水"。
            "l2": {
                "counts": l2_status["counts"],
                "pending": l2_status["pending"],
                "effective": _l2_effective(l2_config),
            },
            "background": service.metadata() if service is not None else None,
            "l3_enabled": bool(resolved_settings.memory.l3_enabled),
        }

    # ------------------------------------------------------------------ #
    # L2 独立交互（M06）：未归档交互窗口 / 归档状态 / 会话级配置
    # ------------------------------------------------------------------ #
    def _l2_effective(config: dict) -> dict:
        memory_settings = resolved_settings.memory
        trigger = int(config.get("trigger_count") or memory_settings.chat_l2_trigger)
        retain = int(config.get("retain_count") or memory_settings.chat_l2_retain)
        return {
            "trigger": trigger,
            "retain": retain,
            "batch": max(1, trigger - retain),
            "char_budget": int(memory_settings.chat_l2_char_budget),
            "enabled": config.get("enabled") is not False,
            "source": {
                "trigger": "conversation" if config.get("trigger_count") else "default",
                "retain": "conversation" if config.get("retain_count") else "default",
                "enabled": "conversation" if config.get("enabled") is not None else "default",
            },
        }

    def _l2_defaults() -> dict:
        memory_settings = resolved_settings.memory
        return {
            "trigger": int(memory_settings.chat_l2_trigger),
            "retain": int(memory_settings.chat_l2_retain),
            "batch": int(memory_settings.chat_l2_batch),
            "char_budget": int(memory_settings.chat_l2_char_budget),
        }

    @app.get(
        "/api/v1/agents/{agent_id}/memory/l2",
        summary="L2 独立交互分页（原文引用，附归档状态与工具证据）",
    )
    async def memory_l2(
        agent_id: str,
        conversation_id: str = Query(..., max_length=250),
        limit: int = Query(20, ge=1, le=100),
        offset: int = Query(0, ge=0, le=100000),
        state: str = Query("", max_length=16),
    ):
        memory = _memory_for(agent_id)
        cid = await resource_scope(agent_id, conversation_id, None)
        data = await memory.l2_list(
            agent_id=agent_id, conversation_id=cid,
            limit=limit, offset=offset, state=state,
        )
        return {
            "api_version": API_VERSION,
            "agent_id": agent_id,
            "conversation_id": cid,
            **data,
            "next_offset": offset + len(data["items"]) if data["items"] else None,
        }

    @app.get(
        "/api/v1/agents/{agent_id}/memory/l2/status",
        summary="L2 未归档条数/阈值、归档批次与本次加载预算状态",
    )
    async def memory_l2_status(
        agent_id: str,
        conversation_id: str = Query(..., max_length=250),
    ):
        memory = _memory_for(agent_id)
        cid = await resource_scope(agent_id, conversation_id, None)
        status = await memory.l2_archive_status(
            agent_id=agent_id, conversation_id=cid
        )
        config = await memory.l2_config_get(
            agent_id=agent_id, conversation_id=cid
        )
        orchestrator = getattr(resolved_runtime, "orchestrator", None)
        counters = getattr(orchestrator, "_counters", {}) or {}
        # 只用供应商真实返回的 usage 数值；没有就明确 unavailable，不估算命中率。
        view = getattr(orchestrator, "cache_usage_view", None)
        cache_usage = (
            view(agent_id, cid) if callable(view)
            else {"available": False, "reason": "provider_usage_unavailable",
                  "scope": "process"}
        )
        load_view = getattr(orchestrator, "l2_load_view", None)
        l2_load = (
            load_view(agent_id, cid) if callable(load_view)
            else {"available": False, "reason": "unavailable", "scope": "conversation"}
        )
        return {
            "api_version": API_VERSION,
            "agent_id": agent_id,
            "conversation_id": cid,
            "counts": status["counts"],
            "pending": status["pending"],
            "archives": status["items"],
            "config": config,
            "effective": _l2_effective(config),
            "defaults": _l2_defaults(),
            "budget": {
                "char_budget": int(resolved_settings.memory.chat_l2_char_budget),
                "loaded_items": int(counters.get("l2_history_items") or 0),
                "omitted_items": int(counters.get("l2_history_omitted") or 0),
                "loaded_scope": "process",
            },
            # 本会话最后一次真实装载（条数/字符/预算/时间）；未装载时 available=False。
            "l2_load": l2_load,
            "cache_usage": cache_usage,
        }

    @app.get(
        "/api/v1/agents/{agent_id}/memory/l2/config",
        summary="会话级 L2 归档配置（未设置项继承默认）",
    )
    async def memory_l2_config(
        agent_id: str,
        conversation_id: str = Query(..., max_length=250),
    ):
        memory = _memory_for(agent_id)
        cid = await resource_scope(agent_id, conversation_id, None)
        config = await memory.l2_config_get(
            agent_id=agent_id, conversation_id=cid
        )
        return {
            "api_version": API_VERSION,
            "agent_id": agent_id,
            "conversation_id": cid,
            **config,
            "effective": _l2_effective(config),
            "defaults": _l2_defaults(),
        }

    @app.patch(
        "/api/v1/agents/{agent_id}/memory/l2/config",
        dependencies=[Depends(require_internal)],
        summary="保存会话级 L2 配置（乐观锁；None 表示恢复默认）",
    )
    async def memory_l2_config_save(
        agent_id: str,
        request: Request,
        conversation_id: str = Query(..., max_length=250),
    ):
        memory = _memory_for(agent_id)
        cid = await resource_scope(agent_id, conversation_id, None)
        try:
            payload = await request.json()
        except Exception:  # noqa: BLE001
            raise HTTPException(422, detail={"code": "invalid_json"}) from None
        if not isinstance(payload, dict):
            raise HTTPException(422, detail={"code": "invalid_body"})
        try:
            expected = int(payload.get("expected_revision", 0))
        except (TypeError, ValueError):
            raise HTTPException(422, detail={"code": "invalid_revision"}) from None
        trigger = payload.get("trigger_count")
        retain = payload.get("retain_count")
        enabled = payload.get("enabled")
        for field, value in (("trigger_count", trigger), ("retain_count", retain)):
            if value is not None and (
                isinstance(value, bool) or not isinstance(value, int)
            ):
                raise HTTPException(422, detail={"code": f"invalid_{field}"})
        if enabled is not None and not isinstance(enabled, bool):
            raise HTTPException(422, detail={"code": "invalid_enabled"})
        current = await memory.l2_config_get(
            agent_id=agent_id, conversation_id=cid
        )
        # 区分「未提交该字段」（保持已存值，含 None）与「显式 null」（恢复继承默认）。
        candidate = {
            "trigger_count": current.get("trigger_count"),
            "retain_count": current.get("retain_count"),
            "enabled": current.get("enabled"),
        }
        for key in ("trigger_count", "retain_count", "enabled"):
            if key in payload:
                candidate[key] = payload.get(key)
        effective = _l2_effective(candidate)
        if not 1 <= int(effective["retain"]) < int(effective["trigger"]) <= 500:
            raise HTTPException(
                422, detail={"code": "invalid_l2_window", "message": "1 <= retain < trigger <= 500"}
            )
        try:
            receipt = await memory.l2_config_set(
                agent_id=agent_id, conversation_id=cid,
                expected_revision=expected,
                trigger_count=candidate["trigger_count"],
                retain_count=candidate["retain_count"],
                enabled=candidate["enabled"],
            )
        except ValueError as exc:
            if str(exc) == "revision_conflict":
                raise HTTPException(409, detail={"code": "revision_conflict"}) from None
            raise HTTPException(422, detail={"code": "invalid_l2_config"}) from None
        saved = await memory.l2_config_get(
            agent_id=agent_id, conversation_id=cid
        )
        return {
            "api_version": API_VERSION,
            "agent_id": agent_id,
            "conversation_id": cid,
            **saved,
            "saved_revision": receipt["revision"],
            "effective": _l2_effective(saved),
            "defaults": _l2_defaults(),
            "application": "next_archive",
        }

    @app.get(
        "/api/v1/agents/{agent_id}/chat/archive",
        summary="归档批次（只读；摘要成功后才推进游标）",
    )
    async def chat_archive(
        agent_id: str,
        conversation_id: str = Query(..., max_length=250),
        limit: int = Query(20, ge=1, le=100),
    ):
        memory = _memory_for(agent_id)
        cid = await resource_scope(agent_id, conversation_id, None)
        items = await memory.chat_archive_list(
            agent_id=agent_id, conversation_id=cid, limit=limit
        )
        status = await memory.chat_archive_status(
            agent_id=agent_id, conversation_id=cid
        )
        return {
            "api_version": API_VERSION,
            "agent_id": agent_id,
            "conversation_id": cid,
            "status": status,
            "items": items,
        }

    @app.get(
        "/api/v1/agents/{agent_id}/chat/events",
        summary="事件条目（参与者/来源序号/状态；与旧的每轮审计日记分开）",
    )
    async def chat_events(
        agent_id: str,
        conversation_id: str = Query(..., max_length=250),
        status: str = Query("", max_length=20),
        limit: int = Query(20, ge=1, le=100),
    ):
        memory = _memory_for(agent_id)
        cid = await resource_scope(agent_id, conversation_id, None)
        items = await memory.memory_event_list(
            agent_id=agent_id, conversation_id=cid, limit=limit, status=status
        )
        return {
            "api_version": API_VERSION,
            "agent_id": agent_id,
            "conversation_id": cid,
            "items": items,
        }

    @app.get(
        "/api/v1/agents/{agent_id}/chat/l3",
        summary="L3 事实（默认 active；可查候选/替代/撤回，附来源与证据）",
    )
    async def chat_l3(
        agent_id: str,
        conversation_id: str = Query(..., max_length=250),
        status: str = Query("active", max_length=20),
        identity_id: str = Query("", max_length=100),
        limit: int = Query(50, ge=1, le=200),
    ):
        memory = _memory_for(agent_id)
        cid = await resource_scope(agent_id, conversation_id, None)
        items = await memory.chat_l3_list(
            agent_id=agent_id, conversation_id=cid,
            status=status, identity_id=identity_id, limit=limit,
        )
        return {
            "api_version": API_VERSION,
            "agent_id": agent_id,
            "conversation_id": cid,
            "status": status,
            "items": items,
        }

    @app.post(
        "/api/v1/agents/{agent_id}/chat/l3/{fact_id}/status",
        summary="用户纠正/撤回/替代：事实状态流转并记录历史（不删除事实）",
        dependencies=[Depends(require_internal)],
    )
    async def chat_l3_status(
        agent_id: str,
        fact_id: str,
        conversation_id: str = Query(..., max_length=250),
        status: str = Query(..., max_length=20),
        reason: str = Query("", max_length=200),
        replaced_by: str = Query("", max_length=80),
    ):
        memory = _memory_for(agent_id)
        cid = await resource_scope(agent_id, conversation_id, None)
        ok = await memory.l3_set_status(
            agent_id=agent_id, conversation_id=cid, fact_id=fact_id,
            status=status, reason=reason, replaced_by=replaced_by,
        )
        if not ok:
            raise _error(404, "fact_not_found", "事实不存在或状态不合法")
        return {
            "api_version": API_VERSION,
            "ok": True,
            "fact_id": fact_id,
            "status": status,
        }

    @app.get('/api/v1/agents/{agent_id}/chat/tasks',dependencies=[Depends(require_internal)])
    async def chat_tasks(agent_id:str,conversation_id:str=Query(...)):
        cid=await resource_scope(agent_id,conversation_id,None)
        return {'items':await _memory_for(agent_id).chat_task_list(agent_id=agent_id,conversation_id=cid)}

    @app.post('/api/v1/agents/{agent_id}/chat/tasks/retry',dependencies=[Depends(require_internal)])
    async def chat_task_retry(agent_id:str,conversation_id:str=Query(...),kind:str=Query(...,pattern='^(facts|events|archive)$')):
        cid=await resource_scope(agent_id,conversation_id,None)
        ok=await _memory_for(agent_id).chat_task_manual_retry(agent_id=agent_id,conversation_id=cid,kind=kind)
        if not ok:raise _error(404,'chat_task_not_found','任务不存在')
        return {'ok':True,'state':'pending','automatic_inference_started':False}

    # ------------------------------------------------------------------ #
    # 群知识库（独立文档库；与 L4/资源库/L20 分离）
    # ------------------------------------------------------------------ #

    def _kb_error(exc: KnowledgeError):
        raise _error(400, exc.code, exc.detail)

    @app.get("/api/v1/agents/{agent_id}/knowledge/config", summary="群知识库配置")
    async def kb_config(agent_id: str, conversation_id: str = Query(..., max_length=250)):
        cid = await resource_scope(agent_id, conversation_id, None)
        kb = _knowledge_for(resolved_settings)
        return {"api_version": API_VERSION,
                **kb.get_config(agent_id=agent_id, conversation_id=cid)}

    @app.patch(
        "/api/v1/agents/{agent_id}/knowledge/config",
        dependencies=[Depends(require_internal)],
        summary="修改群知识库配置（revision 校验）",
    )
    async def kb_patch_config(
        agent_id: str, request: Request,
        conversation_id: str = Query(..., max_length=250),
        revision: int = Query(-1),
    ):
        cid = await resource_scope(agent_id, conversation_id, None)
        kb = _knowledge_for(resolved_settings)
        body = await request.json()
        try:
            values = kb.patch_config(
                agent_id=agent_id, conversation_id=cid, values=body,
                revision=None if revision < 0 else revision,
            )
        except KnowledgeError as exc:
            _kb_error(exc)
        return {"api_version": API_VERSION, **values}

    @app.get("/api/v1/agents/{agent_id}/knowledge/status", summary="知识库状态与能力")
    async def kb_status(agent_id: str, conversation_id: str = Query(..., max_length=250)):
        cid = await resource_scope(agent_id, conversation_id, None)
        kb = _knowledge_for(resolved_settings)
        return {"api_version": API_VERSION,
                **kb.status(agent_id=agent_id, conversation_id=cid)}

    @app.get("/api/v1/agents/{agent_id}/knowledge/documents", summary="文档目录（分页/过滤）")
    async def kb_documents(
        agent_id: str,
        conversation_id: str = Query(..., max_length=250),
        query: str = Query("", max_length=100),
        category: str = Query("", max_length=60),
        status: str = Query("", max_length=30),
        limit: int = Query(20, ge=1, le=100),
        offset: int = Query(0, ge=0, le=100000),
    ):
        cid = await resource_scope(agent_id, conversation_id, None)
        kb = _knowledge_for(resolved_settings)
        return {"api_version": API_VERSION, "agent_id": agent_id, "conversation_id": cid,
                **kb.list_documents(agent_id=agent_id, conversation_id=cid, query=query,
                                    category=category, status=status,
                                    limit=limit, offset=offset)}

    @app.get("/api/v1/agents/{agent_id}/knowledge/documents/{doc_id}", summary="文档详情与版本")
    async def kb_document(agent_id: str, doc_id: str,
                          conversation_id: str = Query(..., max_length=250)):
        cid = await resource_scope(agent_id, conversation_id, None)
        kb = _knowledge_for(resolved_settings)
        try:
            item = kb.get_document(agent_id=agent_id, conversation_id=cid, doc_id=doc_id)
        except KnowledgeError as exc:
            _kb_error(exc)
        return {"api_version": API_VERSION, **item}

    @app.get("/api/v1/agents/{agent_id}/knowledge/documents/{doc_id}/chunks",
             summary="限量片段预览")
    async def kb_chunks(agent_id: str, doc_id: str,
                        conversation_id: str = Query(..., max_length=250),
                        limit: int = Query(20, ge=1, le=50),
                        offset: int = Query(0, ge=0, le=100000)):
        cid = await resource_scope(agent_id, conversation_id, None)
        kb = _knowledge_for(resolved_settings)
        try:
            data = kb.list_chunks(agent_id=agent_id, conversation_id=cid,
                                  doc_id=doc_id, limit=limit, offset=offset)
        except KnowledgeError as exc:
            _kb_error(exc)
        return {"api_version": API_VERSION, **data}

    @app.post(
        "/api/v1/agents/{agent_id}/knowledge/documents/text",
        dependencies=[Depends(require_internal)],
        summary="新建文本知识条目",
    )
    async def kb_add_text(agent_id: str, request: Request,
                          conversation_id: str = Query(..., max_length=250)):
        cid = await resource_scope(agent_id, conversation_id, None)
        kb = _knowledge_for(resolved_settings)
        body = await request.json()
        try:
            result = await asyncio.to_thread(kb.add_text,
                agent_id=agent_id, conversation_id=cid,
                title=str(body.get("title") or ""), content=str(body.get("content") or ""),
                category=str(body.get("category") or ""),
                tags=body.get("tags") or (), filename=str(body.get("filename") or ""),
            )
        except KnowledgeError as exc:
            _kb_error(exc)
        return {"api_version": API_VERSION, **result}

    @app.post(
        "/api/v1/agents/{agent_id}/knowledge/documents/upload",
        dependencies=[Depends(require_internal)],
        summary="上传原件（JSON base64，流式原件由既有文件能力承担）",
    )
    async def kb_upload(agent_id: str, request: Request,
                        conversation_id: str = Query(..., max_length=250)):
        cid = await resource_scope(agent_id, conversation_id, None)
        kb = _knowledge_for(resolved_settings)
        body = await request.json()
        try:
            data = base64.b64decode(str(body.get("content_base64") or ""), validate=True)
        except Exception:
            raise _error(400, "invalid_base64", "上传内容不是有效 Base64")
        try:
            result = await asyncio.to_thread(kb.add_file,
                agent_id=agent_id, conversation_id=cid,
                title=str(body.get("title") or ""), filename=str(body.get("filename") or "upload.bin"),
                data=data, category=str(body.get("category") or ""), tags=body.get("tags") or (),
            )
        except KnowledgeError as exc:
            _kb_error(exc)
        return {"api_version": API_VERSION, **result}

    @app.post(
        "/api/v1/agents/{agent_id}/knowledge/documents/from-resource",
        dependencies=[Depends(require_internal)],
        summary="从当前群的文件资源加入（按资源 ID + 哈希校验）",
    )
    async def kb_from_resource(agent_id: str, request: Request,
                               conversation_id: str = Query(..., max_length=250)):
        cid = await resource_scope(agent_id, conversation_id, None)
        kb = _knowledge_for(resolved_settings)
        body = await request.json()
        resource_id = str(body.get("resource_id") or "")
        files_backend = getattr(resolved_runtime, "files", None)
        catalog = getattr(files_backend, "catalog", None) if files_backend is not None else None
        if catalog is None:
            raise _error(503, "resource_catalog_unavailable",
                         "文件资源未启用，无法从资源加入知识库")
        try:
            meta = await catalog.get(
                agent_id=agent_id, conversation_id=cid, resource_id=resource_id
            )
            path = await catalog.open_blob(
                agent_id=agent_id, conversation_id=cid, resource_id=resource_id
            )
        except Exception:
            raise _error(404, "resource_not_found", "资源不存在或不属于本群")
        data = await asyncio.to_thread(Path(path).read_bytes)
        import hashlib
        if not meta or hashlib.sha256(data).hexdigest()!=meta.get('sha256'):raise _error(422,'resource_integrity_error','资源哈希不匹配')
        try:
            result = await asyncio.to_thread(kb.add_from_resource,
                agent_id=agent_id, conversation_id=cid,
                title=str(body.get("title") or meta.get("name") or ""),
                filename=str(meta.get("name") or "resource.bin"), data=data,
                category=str(body.get("category") or ""), tags=body.get("tags") or (),
            )
        except KnowledgeError as exc:
            _kb_error(exc)
        return {"api_version": API_VERSION, **result}

    @app.post(
        "/api/v1/agents/{agent_id}/knowledge/documents/{doc_id}/versions",
        dependencies=[Depends(require_internal)],
        summary="追加新版本（索引成功后才切换）",
    )
    async def kb_add_version(agent_id: str, doc_id: str, request: Request,
                             conversation_id: str = Query(..., max_length=250)):
        cid = await resource_scope(agent_id, conversation_id, None)
        kb = _knowledge_for(resolved_settings)
        body = await request.json()
        raw = body.get("content_base64")
        try:
            result = await asyncio.to_thread(kb.add_version,
                agent_id=agent_id, conversation_id=cid, doc_id=doc_id,
                filename=str(body.get("filename") or ""),
                data=base64.b64decode(str(raw), validate=True) if raw else None,
                content=str(body.get("content") or ""),
            )
        except KnowledgeError as exc:
            _kb_error(exc)
        return {"api_version": API_VERSION, **result}

    @app.patch(
        "/api/v1/agents/{agent_id}/knowledge/documents/{doc_id}",
        dependencies=[Depends(require_internal)],
        summary="修改标题/分类/标签/启用状态",
    )
    async def kb_patch_document(agent_id: str, doc_id: str, request: Request,
                                conversation_id: str = Query(..., max_length=250)):
        cid = await resource_scope(agent_id, conversation_id, None)
        kb = _knowledge_for(resolved_settings)
        body = await request.json()
        try:
            item = kb.patch_document(agent_id=agent_id, conversation_id=cid,
                                     doc_id=doc_id, values=body)
        except KnowledgeError as exc:
            _kb_error(exc)
        return {"api_version": API_VERSION, **item}

    @app.delete(
        "/api/v1/agents/{agent_id}/knowledge/documents/{doc_id}",
        dependencies=[Depends(require_internal)],
        summary="软删除文档（不删除聊天资源与记忆）",
    )
    async def kb_delete_document(agent_id: str, doc_id: str,
                                 conversation_id: str = Query(..., max_length=250)):
        cid = await resource_scope(agent_id, conversation_id, None)
        kb = _knowledge_for(resolved_settings)
        try:
            result = kb.delete_document(agent_id=agent_id, conversation_id=cid, doc_id=doc_id)
        except KnowledgeError as exc:
            _kb_error(exc)
        return {"api_version": API_VERSION, **result}

    @app.post(
        "/api/v1/agents/{agent_id}/knowledge/documents/{doc_id}/reindex",
        dependencies=[Depends(require_internal)],
        summary="重建关键词索引（真实任务，同步完成后返回）",
    )
    async def kb_reindex(agent_id: str, doc_id: str,
                         conversation_id: str = Query(..., max_length=250)):
        cid = await resource_scope(agent_id, conversation_id, None)
        kb = _knowledge_for(resolved_settings)
        try:
            result = await asyncio.to_thread(kb.queue_reindex,agent_id=agent_id, conversation_id=cid, doc_id=doc_id)
        except KnowledgeError as exc:
            _kb_error(exc)
        return {"api_version": API_VERSION, **result}

    @app.get("/api/v1/agents/{agent_id}/knowledge/jobs", summary="索引任务列表")
    async def kb_jobs(agent_id: str, conversation_id: str = Query(..., max_length=250),
                      limit: int = Query(20, ge=1, le=100)):
        cid = await resource_scope(agent_id, conversation_id, None)
        kb = _knowledge_for(resolved_settings)
        return {"api_version": API_VERSION,
                **kb.list_jobs(agent_id=agent_id, conversation_id=cid, limit=limit)}

    @app.post(
        "/api/v1/agents/{agent_id}/knowledge/jobs/{job_id}/retry",
        dependencies=[Depends(require_internal)],
        summary="手动重试索引任务",
    )
    async def kb_retry_job(agent_id: str, job_id: str,
                           conversation_id: str = Query(..., max_length=250)):
        cid = await resource_scope(agent_id, conversation_id, None)
        kb = _knowledge_for(resolved_settings)
        try:
            result = kb.retry_job(agent_id=agent_id, conversation_id=cid, job_id=job_id)
        except KnowledgeError as exc:
            _kb_error(exc)
        return {"api_version": API_VERSION, **result}

    @app.post('/api/v1/agents/{agent_id}/knowledge/documents/upload-stream',dependencies=[Depends(require_internal)])
    async def kb_upload_stream(agent_id:str,request:Request,conversation_id:str=Query(...),filename:str=Query(...,max_length=200),title:str=Query('',max_length=200),category:str=Query('',max_length=60)):
        import tempfile
        cid=await resource_scope(agent_id,conversation_id,None)
        with tempfile.TemporaryFile() as spool:
            total=0
            async for chunk in request.stream():
                total+=len(chunk)
                if total>104857600:raise _error(413,'file_too_large','文件超过100MiB')
                await asyncio.to_thread(spool.write,chunk)
            def ingest():
                spool.seek(0)
                return _knowledge_for(resolved_settings).add_file(agent_id=agent_id,conversation_id=cid,title=title or filename,filename=filename,data=spool.read(),category=category)
            try:result=await asyncio.to_thread(ingest)
            except KnowledgeError as exc:_kb_error(exc)
        return {'api_version':API_VERSION,**result}

    @app.post("/api/v1/agents/{agent_id}/knowledge/search", summary="真实知识检索（不调用聊天模型）",dependencies=[Depends(require_internal)])
    async def kb_search(agent_id: str, request: Request,
                        conversation_id: str = Query(..., max_length=250)):
        cid = await resource_scope(agent_id, conversation_id, None)
        kb = _knowledge_for(resolved_settings)
        body = await request.json()
        return {"api_version": API_VERSION,
                **await kb.search_async(agent_id=agent_id, conversation_id=cid,
                            query=str(body.get("query") or ""),
                            limit=int(body.get("limit") or 5),
                            category=str(body.get("category") or ""))}

    @app.get(
        "/api/v1/agents/{agent_id}/memory/records",
        response_model=MemoryRecordsResponse,
        summary="按层/会话/身份/时间查询记录摘要（默认不返回整段正文）",
    )
    async def memory_records(
        agent_id: str,
        layer: str = Query(default="L2", description="raw / L2 / L20"),
        conversation_id: Optional[str] = Query(default=None),
        identity_id: Optional[str] = Query(default=None),
        since: Optional[str] = Query(default=None, description="ISO 8601，含边界"),
        until: Optional[str] = Query(default=None, description="ISO 8601，不含边界"),
        limit: Optional[int] = Query(default=None, ge=1, le=100),
        offset: int = Query(default=0, ge=0),
    ) -> MemoryRecordsResponse:
        if layer not in QUERYABLE_LAYERS:
            raise HTTPException(
                status_code=422,
                detail=f"layer 必须是 {', '.join(QUERYABLE_LAYERS)} 之一",
            )
        _check_time(since, "since")
        _check_time(until, "until")
        memory = _memory_for(agent_id)
        data = await memory.query_records(
            agent_id=agent_id,
            layer=layer,
            conversation_id=conversation_id,
            identity_id=identity_id,
            since=since,
            until=until,
            limit=limit,
            offset=offset,
        )
        return MemoryRecordsResponse(
            api_version=API_VERSION,
            agent_id=agent_id,
            layer=layer,
            items=[MemoryRecordItem(**item) for item in data["items"]],
            total=data["total"],
            limit=data["limit"],
            offset=data["offset"],
            next_offset=data["next_offset"],
            preview_chars=data["preview_chars"],
        )

    @app.get(
        "/api/v1/agents/{agent_id}/memory/records/{record_id}",
        response_model=MemoryRecordDetailResponse,
        summary="单条记录详情（正文按上限截断，带来源引用）",
    )
    async def memory_record_detail(
        agent_id: str, record_id: str
    ) -> MemoryRecordDetailResponse:
        memory = _memory_for(agent_id)
        record = await memory.get_record(agent_id=agent_id, record_id=record_id)
        if record is None:
            # 不存在与不属于该 Agent 统一 404，不泄露记录是否存在于别处。
            raise HTTPException(status_code=404, detail="记录不存在或不属于该 Agent")
        return MemoryRecordDetailResponse(
            api_version=API_VERSION, agent_id=agent_id, record=record
        )

    @app.get(
        "/api/v1/agents/{agent_id}/memory/identities",
        response_model=MemoryIdentitiesResponse,
        summary="按别名查候选身份（带作用域与歧义标记）",
    )
    async def memory_identities(
        agent_id: str,
        alias: str = Query(min_length=1, max_length=64, description="展示名/群名片"),
        account_id: Optional[str] = Query(default=None),
        conversation_id: Optional[str] = Query(default=None),
        limit: Optional[int] = Query(default=None, ge=1, le=100),
    ) -> MemoryIdentitiesResponse:
        if not account_id and not conversation_id:
            raise HTTPException(
                status_code=422,
                detail=(
                    "需要 account_id 或 conversation_id 限定作用域："
                    "不做跨账号/跨群的无条件合并"
                ),
            )
        memory = _memory_for(agent_id)
        try:
            data = await memory.find_identities(
                agent_id=agent_id,
                alias=alias,
                account_id=account_id,
                conversation_id=conversation_id,
                limit=limit,
            )
        except MemoryQueryError as exc:
            # 非法会话标识、账号冲突：受控输入错误，不降级成全库查询。
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        return MemoryIdentitiesResponse(
            api_version=API_VERSION,
            agent_id=agent_id,
            alias=data["alias"],
            account_id=data["account_id"],
            conversation_id=data["conversation_id"],
            candidates=[
                MemoryIdentityCandidate(
                    identity_id=item["identity_id"],
                    channel_id=item["channel_id"],
                    account_id=item["account_id"],
                    bridge_user_id=item["bridge_user_id"],
                    wxid_known=item["wxid_known"],
                    wxid_source=item["wxid_source"],
                    aliases=[MemoryAliasView(**alias_item) for alias_item in item["aliases"]],
                )
                for item in data["candidates"]
            ],
            ambiguous=data["ambiguous"],
            total=data["total"],
            has_more=data["has_more"],
            limit=data["limit"],
            wxid_note=data["wxid_note"],
        )

    @app.get(
        "/api/v1/agents/{agent_id}/memory/search",
        response_model=MemorySearchResponse,
        summary="只读检索：L4 明确保存 / 事件日记 / 原文来源（带状态与来源）",
    )
    async def memory_search(
        agent_id: str,
        q: str = Query(min_length=1, max_length=200, description="关键词或短句"),
        conversation_id: Optional[str] = Query(default=None),
        limit: Optional[int] = Query(default=None, ge=1, le=20),
    ) -> MemorySearchResponse:
        agent = resolved_settings.find_agent(agent_id)
        if agent is None:
            raise HTTPException(status_code=404, detail=f"未配置的 Agent：{agent_id}")
        retriever, code, detail = resolve_retriever(resolved_runtime, agent)
        if retriever is None:
            raise HTTPException(
                status_code=409,
                detail=f"该 Agent 未绑定可用检索组件（{code}）：{detail}",
            )
        memory, memory_code, memory_detail = resolve_memory(resolved_runtime, agent)
        if memory is None:
            raise HTTPException(
                status_code=409,
                detail=f"该 Agent 未绑定可用记忆仓库（{memory_code}）：{memory_detail}",
            )
        try:
            result = await retriever.retrieve(
                agent_id=agent_id,
                conversation_id=conversation_id,
                account_id=None,
                query=q,
                limit=limit,
            )
        except MemoryQueryError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        return MemorySearchResponse(
            api_version=API_VERSION,
            agent_id=agent_id,
            query=result.query,
            state=result.state,
            detail=result.detail,
            identity_id=result.identity_id,
            ambiguous_candidates=list(result.ambiguous_candidates),
            items=[
                MemorySearchItem(
                    layer=item.layer,
                    record_id=item.record_id,
                    occurred_at=item.occurred_at,
                    preview=item.preview,
                    score=item.score,
                    source_refs=list(item.source_refs),
                    subject_label=item.subject_label,
                )
                for item in result.hits
            ],
        )

    @app.get(
        "/api/v1/components",
        response_model=ComponentsResponse,
        summary="实际登记的组件、配置描述与状态",
    )
    def components() -> ComponentsResponse:
        views: List[ComponentView] = []
        for item in resolved_runtime.components.all():
            descriptor = item.descriptor
            views.append(
                ComponentView(
                    id=descriptor.id,
                    type=descriptor.type,
                    version=descriptor.version,
                    description=descriptor.description,
                    config_fields=descriptor.config_fields,
                    capabilities=descriptor.capabilities,
                    dependencies=descriptor.dependencies,
                    status=descriptor.status,
                    status_detail=descriptor.status_detail,
                    has_instance=item.has_instance,
                )
            )
        return ComponentsResponse(
            api_version=API_VERSION,
            components=views,
            component_types=_component_types(resolved_runtime.components),
            note=(
                "只列出真实登记的对象；未实现的通道、记忆、检索与会话策略"
                "不在此处伪装为可用组件。"
            ),
        )

    @app.get("/api/v1/agents", response_model=AgentsResponse, summary="真实配置的 Agent 列表")
    def agents() -> AgentsResponse:
        return AgentsResponse(
            api_version=API_VERSION,
            default_agent_id=resolved_settings.default_agent().agent_id,
            agents=[
                _agent_summary(resolved_runtime, agent) for agent in resolved_settings.agents
            ],
        )

    @app.get(
        "/api/v1/agents/{agent_id}/composition",
        response_model=CompositionResponse,
        summary="Agent 的组件绑定关系与尚未就绪槽位",
    )
    def composition(agent_id: str) -> CompositionResponse:
        agent = resolved_settings.find_agent(agent_id)
        if agent is None:
            raise _error(
                404,
                "unknown_agent",
                f"未配置的 Agent：{agent_id}",
                available_agents=[item.agent_id for item in resolved_settings.agents],
            )
        slots = [
            SlotView(
                slot=slot["slot"],
                expected_type=slot["expected_type"],
                component_id=slot["component_id"],
                status=slot["status"],
                status_detail=slot["status_detail"],
                capabilities=slot["capabilities"],
            )
            for slot in resolve_composition(agent, resolved_runtime.components)
        ]
        return CompositionResponse(
            api_version=API_VERSION,
            agent_id=agent.agent_id,
            display_name=agent.display_name,
            agent_ready=False,
            thinking_policy=_thinking_policy(resolved_settings,resolved_runtime.configuration),
            slots=slots,
        )

    # ------------------------------------------------------------------ #
    # 会话与运行（P15：HTTP 会话持久化 + 通用目录/回放，与通道同一记忆链）
    # ------------------------------------------------------------------ #

    def _mem_cid(agent_id: str, session_id: str) -> str:
        """HTTP 会话的记忆会话键（4 段，parse_conversation_id 兼容）。

        与资源作用域的**显式映射**（不是同一格式，也不静默换格式）：
        记忆 `http:{agent}:private:{session}` ↔ 资源键 `http:{agent}:{session}`
        （resource_scope 保持旧键，原件不失联）。
        """
        return f"http:{agent_id}:private:{session_id}"

    async def _persist_session(agent_id: str, session: AgentSession) -> None:
        memory = resolved_runtime.memory
        if not getattr(memory, "available", False):
            return
        try:
            await memory.session_upsert(
                session_id=session.session_id,
                agent_id=agent_id,
                channel_id="http",
                account_id=agent_id,
                conversation_id=_mem_cid(agent_id, session.session_id),
                conversation_type="private",
                target_ref=session.session_id,
                display_name="",
            )
        except (MemoryUnavailableError, MemoryWriteError, MemoryQueryErrorP15) as exc:
            logger.warning("session_persist_failed type=%s", type(exc).__name__)

    async def _ensure_session(agent_id: str, session_id: str) -> AgentSession:
        """取内存会话；不存在但持久目录有记录时**恢复**（与通道同一预算规则）。

        - `create_with_id` 原子创建/复用（并发恢复不重复建键）；
        - 历史走编排器 `restore_session_history`：完整轮次 + 字符/消息双预算
          （存储侧字符硬预算 + 为本轮预留空间的条数上限），P07 压缩规则复用；
        - L1/L10 内存态重启即空，与持久会话分开表达，不互相伪装。
        """
        session = resolved_runtime.sessions.get(agent_id, session_id)
        if session is not None:
            return session
        memory = resolved_runtime.memory
        if not getattr(memory, "available", False):
            raise _error(404, "unknown_session", f"未知会话：{session_id}")
        try:
            record = await memory.session_get(agent_id=agent_id, session_id=session_id)
        except (MemoryQueryError, MemoryQueryErrorP15) as exc:
            raise _error(422, "invalid_session_query", str(exc)) from None
        if record is None:
            raise _error(404, "unknown_session", f"未知会话：{session_id}")
        session = resolved_runtime.sessions.create_with_id(agent_id, session_id)
        # P15-R02：恢复期间**持有运行占用**——"恢复中"的会话不得被另一请求
        # 当作已就绪使用或覆盖正在重建的历史；恢复完成后释放，后续请求按
        # 正常占用流程 acquire（保留原子占用，不提前 release）。
        try:
            resolved_runtime.sessions.acquire(agent_id, session_id)
        except SessionBusyError as exc:
            raise _error(409, "session_busy", str(exc), session_id=session_id) from exc
        try:
            session.created_at = record.get("created_at") or session.created_at
            session.updated_at = record.get("updated_at") or session.updated_at
            session.run_count = int(record.get("run_count") or 0)
            await resolved_runtime.orchestrator.restore_session_history(
                session, _mem_cid(agent_id, session_id)
            )
        finally:
            resolved_runtime.sessions.release(agent_id, session_id)
        logger.info(
            "session_restored agent_id=%s session_id=%s history=%d",
            agent_id, session_id, len(session.history),
        )
        return session

    async def _run_http_turn(
        agent: AgentSettings,
        session: AgentSession,
        text: str,
        payload: SessionMessageRequest,
    ) -> RunResult:
        """HTTP 一轮：**委托编排器的共享轮次链**（P15-R01）。

        - 会话运行权已由调用方 `SessionStore.acquire` 原子占用（首次 await
          前占住），本方法所有出口 finally release；
        - memory/presave/检索/人格/ToolContext(ref+grants)/收尾全部在
          `Orchestrator.run_session_turn`（与通道共享，不复制逻辑）；
        - 记忆四段键与资源三段键的映射在 orchestrator 内显式传递；
        - 失败以 TurnBlockedError 抛出 → 受控 HTTP 错误；不把失败说成功。
        """
        resource_cid = f"http:{agent.agent_id}:{session.session_id}"
        memory_cid = _mem_cid(agent.agent_id, session.session_id)
        image_scope = (
            resolved_runtime.images.scope(agent.agent_id, resource_cid)
            if resolved_runtime.images is not None else None
        )
        files_scope = (
            resolved_runtime.files.scope(agent.agent_id, resource_cid)
            if resolved_runtime.files is not None else None
        )
        try:
            result = await resolved_runtime.orchestrator.run_session_turn(
                agent_id=agent.agent_id,
                session=session,
                text=text,
                resource_cid=resource_cid,
                memory_cid=memory_cid,
                image_ids=tuple(payload.image_ids or ()),
                image_scope=image_scope,
                files_scope=files_scope,
            )
        except TurnBlockedError as exc:
            raise _error(
                _blocked_status(exc.code),
                exc.code,
                str(exc) or exc.code,
                session_id=session.session_id,
            ) from None
        finally:
            resolved_runtime.sessions.release(agent.agent_id, session.session_id)
            if image_scope is not None:
                image_scope.clear()
        await _persist_session(agent.agent_id, session)
        await resolved_runtime.memory.session_touch(
            agent_id=agent.agent_id,
            session_id=session.session_id,
            run_count=session.run_count,
            last_run_id=result.run_id,
            last_status=result.status.value,
            last_error_code=result.error_code,
        )
        return result

    def _blocked_status(code: str) -> int:
        return {
            "session_busy": 409,
            "history_limit": 409,
            "input_too_long": 422,
            "image_context_budget": 422,
            "image_unavailable": 422,
            "context_budget": 422,
            "model_unavailable": 503,
            "memory_unavailable": 503,
            "api_key_missing": 503,
            "cancelled": 499,
        }.get(code, 422)

    @app.post(
        "/api/v1/agents/{agent_id}/sessions",
        response_model=SessionCreateResponse,
        status_code=201,
        summary="创建该 Agent 的会话（内存 + 持久目录）",
    )
    async def create_session(agent_id: str) -> SessionCreateResponse:
        agent = resolved_settings.find_agent(agent_id)
        if agent is None:
            raise _error(
                404,
                "unknown_agent",
                f"未配置的 Agent：{agent_id}",
                available_agents=[item.agent_id for item in resolved_settings.agents],
            )
        session = resolved_runtime.sessions.create(agent.agent_id)
        # 持久目录登记（失败不阻断会话创建，但如实记日志）。
        await _persist_session(agent.agent_id, session)
        logger.info("session_created agent_id=%s session_id=%s", agent.agent_id, session.session_id)
        return SessionCreateResponse(
            api_version=API_VERSION,
            agent_id=agent.agent_id,
            session_id=session.session_id,
            created_at=session.created_at,
            session_status=_session_status(session),
            runtime_limits=_session_limits(resolved_runtime),
        )

    async def _post_message_impl(
        agent_id: str,
        session_id: str,
        payload: SessionMessageRequest,
        *,
        session: Optional[AgentSession] = None,
    ) -> RunResponse:
        """按 Agent 的发送入口；generic 路径委托同一业务链（不复制处理逻辑）。"""
        agent = resolved_settings.find_agent(agent_id)
        if agent is None:
            raise _error(
                404,
                "unknown_agent",
                f"未配置的 Agent：{agent_id}",
                available_agents=[item.agent_id for item in resolved_settings.agents],
            )
        if session is None:
            session = await _ensure_session(agent.agent_id, session_id)

        text = payload.text.strip()
        if not text:
            raise _error(422, "empty_text", "消息正文不能为空")

        # 会话忙优先于模型可用性：**原子占用**（锁内置 busy），并发请求
        # 第二个直接 409，不会同时通过检查、不会覆盖彼此标记。
        try:
            acquired = resolved_runtime.sessions.acquire(agent.agent_id, session.session_id)
        except SessionBusyError as exc:
            raise _error(409, "session_busy", str(exc), session_id=session_id) from exc
        if acquired is None:
            raise _error(404, "unknown_session", f"未知会话：{session_id}")

        result = await _run_http_turn(agent, session, text, payload)
        # message.created(delivered) 只在**正常完成**后由共享链发出；
        # 失败/取消不标 delivered 成功（memory.changed/turn 状态如实区分）。
        return RunResponse(
            api_version=API_VERSION,
            agent_id=agent.agent_id,
            session_id=session.session_id,
            run_id=result.run_id,
            image_inputs=list(result.image_evidence),
            status=result.status.value,
            text=result.text or None,
            error_code=result.error_code,
            error_message=result.error_message,
            finish_reason=result.finish_reason,
            tool_calls=[
                RunToolView(**outcome.summary()) for outcome in result.tool_outcomes
            ],
            model_calls=result.model_calls,
            tool_call_count=result.tool_calls,
            started_at=result.started_at,
            finished_at=result.finished_at,
        )

    @app.post(
        "/api/v1/agents/{agent_id}/sessions/{session_id}/messages",
        response_model=RunResponse,
        summary="发送一条纯文本消息并运行有界 Agent 循环（原文/轮次持久化）",
    )
    async def post_message(
        agent_id: str,
        session_id: str,
        payload: SessionMessageRequest,
    ) -> RunResponse:
        return await _post_message_impl(agent_id, session_id, payload)

    @app.get(
        "/api/v1/agents/{agent_id}/sessions/{session_id}",
        response_model=SessionInfoResponse,
        summary="会话元数据与状态（不返回消息正文）",
    )
    async def get_session(agent_id: str, session_id: str) -> SessionInfoResponse:
        agent = resolved_settings.find_agent(agent_id)
        if agent is None:
            raise _error(
                404,
                "unknown_agent",
                f"未配置的 Agent：{agent_id}",
                available_agents=[item.agent_id for item in resolved_settings.agents],
            )
        session = await _ensure_session(agent.agent_id, session_id)

        last_run = session.last_run
        return SessionInfoResponse(
            api_version=API_VERSION,
            agent_id=agent.agent_id,
            session_id=session.session_id,
            session_status=_session_status(session),
            busy=session.busy,
            created_at=session.created_at,
            updated_at=session.updated_at,
            message_count=session.message_count,
            run_count=session.run_count,
            last_run=(
                LastRunView(
                    run_id=last_run.run_id,
                    status=last_run.status,
                    error_code=last_run.error_code,
                    started_at=last_run.started_at,
                    finished_at=last_run.finished_at,
                    model_calls=last_run.model_calls,
                    tool_calls=last_run.tool_calls,
                )
                if last_run
                else None
            ),
            runtime_limits=_session_limits(resolved_runtime),
        )

    # ------------------------------------------------------------------ #
    # P15：通用会话目录/回放、Agent 详情、per-Agent 记忆层、资源本体、SSE
    # ------------------------------------------------------------------ #

    @app.get("/api/v1/sessions", summary="持久会话目录（HTTP + 通道会话，按 Agent 过滤）")
    async def sessions_directory(
        agent_id: str = Query(..., max_length=80),
        limit: int = Query(20, ge=1, le=100),
        cursor: str = Query("", max_length=200),
    ):
        if resolved_settings.find_agent(agent_id) is None:
            raise _error(404, "unknown_agent", f"未配置的 Agent：{agent_id}")
        memory = resolved_runtime.memory
        if not getattr(memory, "available", False):
            raise _error(503, "memory_unavailable", "记忆存储未就绪，会话目录不可用")
        try:
            result = await memory.session_list(agent_id=agent_id, limit=limit, cursor=cursor)
        except (MemoryQueryError, MemoryQueryErrorP15) as exc:
            raise _error(422, "invalid_session_query", str(exc)) from None
        items = []
        for row in result["items"]:
            runtime_session = resolved_runtime.sessions.get(agent_id, row["session_id"])
            items.append({
                **row,
                "runtime": {
                    "in_memory": runtime_session is not None,
                    "busy": bool(runtime_session.busy) if runtime_session else False,
                },
                "storage": "persistent",
            })
        return {
            "api_version": API_VERSION,
            "agent_id": agent_id,
            "items": items,
            "total": result["total"],
            "limit": limit,
            "cursor": cursor,
            "next_cursor": result["next_cursor"] or None,
            "total_note": "total=该 Agent 的持久会话总数（目录口径，不含纯内存瞬时对象）",
        }

    async def _resolve_generic_session(
        session_id: str, agent_id: Optional[str]
    ) -> Dict[str, Any]:
        """generic 路径解析：精确 session_id；跨 Agent 歧义受控拒绝。"""
        memory = resolved_runtime.memory
        if not getattr(memory, "available", False):
            raise _error(503, "memory_unavailable", "记忆存储未就绪")
        try:
            record = await memory.session_find(session_id=session_id)
        except (MemoryQueryError, MemoryQueryErrorP15) as exc:
            raise _error(422, "invalid_session_query", str(exc)) from None
        if record is None:
            raise _error(404, "unknown_session", f"未知会话：{session_id}")
        if "__ambiguous__" in record:
            raise _error(
                409,
                "session_id_ambiguous",
                "该 session_id 对应多个 Agent 的会话；请显式指定 agent_id。",
                candidates=[r["agent_id"] for r in record["__ambiguous__"]],
            )
        resolved_agent = record["agent_id"]
        if agent_id and agent_id != resolved_agent:
            raise _error(
                404,
                "session_agent_mismatch",
                f"会话不属于 Agent：{agent_id}（不按 ID 相似猜归属）",
            )
        if resolved_settings.find_agent(resolved_agent) is None:
            raise _error(404, "unknown_agent", f"会话所属 Agent 未配置：{resolved_agent}")
        return record

    @app.get("/api/v1/sessions/{session_id}", summary="通用会话详情（跨 Agent 精确解析）")
    async def generic_session_detail(
        session_id: str,
        agent_id: Optional[str] = Query(None, max_length=80),
    ):
        record = await _resolve_generic_session(session_id, agent_id)
        runtime_session = resolved_runtime.sessions.get(
            record["agent_id"], session_id
        )
        return {
            "api_version": API_VERSION,
            **record,
            "runtime": {
                "in_memory": runtime_session is not None,
                "busy": bool(runtime_session.busy) if runtime_session else False,
            },
        }

    @app.get(
        "/api/v1/sessions/{session_id}/messages",
        summary="消息回放：原文/已确认正文/投递状态/关联 L20（分页，无 Base64/工具全文）",
    )
    async def generic_session_messages(
        session_id: str,
        agent_id: Optional[str] = Query(None, max_length=80),
        limit: int = Query(20, ge=1, le=100),
        cursor: str = Query("", max_length=200),
    ):
        record = await _resolve_generic_session(session_id, agent_id)
        memory = resolved_runtime.memory
        try:
            result = await memory.session_replay(
                agent_id=record["agent_id"],
                conversation_id=record["conversation_id"],
                limit=limit,
                cursor=cursor,
            )
        except (MemoryQueryError, MemoryQueryErrorP15) as exc:
            raise _error(422, "invalid_replay_query", str(exc)) from None
        return {
            "api_version": API_VERSION,
            "session_id": session_id,
            "agent_id": record["agent_id"],
            **result,
            "status_note": "status=sent 表示已确认投递；generated=仅生成；"
                           "run_failed/interrupted/unknown 如实区分",
        }

    @app.post(
        "/api/v1/sessions/{session_id}/messages",
        summary="通用会话输入（不经 webot；同一 Agent 处理链）",
        response_model=RunResponse,
    )
    async def generic_post_message(
        session_id: str,
        payload: SessionMessageRequest,
        agent_id: Optional[str] = Query(None, max_length=80),
    ):
        record = await _resolve_generic_session(session_id, agent_id)
        return await _post_message_impl(
            record["agent_id"], session_id, payload,
            session=resolved_runtime.sessions.get(record["agent_id"], session_id),
        )

    @app.get("/api/v1/agents/{agent_id}", summary="Agent 详情（配置摘要/人格粒度/通道状态）")
    def agent_detail(agent_id: str):
        agent = resolved_settings.find_agent(agent_id)
        if agent is None:
            raise _error(404, "unknown_agent", f"未配置的 Agent：{agent_id}")
        summary = _agent_summary(resolved_runtime, agent)
        return {
            "api_version": API_VERSION,
            **summary.model_dump(),
            "persona": {
                "granularity": "per_agent",
                "configured": bool(agent.persona.strip()),
                "note": "人格按 Agent 配置（稳定 SYSTEM，不入历史/SQLite）；"
                        "不同群绑定不同 Agent 可隔离，同一 Agent 多群共用人格；"
                        "当前没有群级人格覆盖，不虚报。",
            },
            "composition": [
                {
                    "slot": slot["slot"],
                    "component_id": slot["component_id"],
                    "status": slot["status"],
                    "status_detail": slot["status_detail"],
                }
                for slot in resolve_composition(agent, resolved_runtime.components)
            ],
        }

    async def _memory_layer_states(agent_id: str) -> Dict[str, Dict[str, Any]]:
        """按真实开关/内存快照给出层状态（没有真实实现不标 available）。"""
        states: Dict[str, Dict[str, Any]] = {}
        background = resolved_runtime.background
        states["L1"] = {
            "state": "available" if background is not None else "disabled",
            "detail": "进程内存有界背景缓冲；重启即空" if background else "未启用",
            "count": (background.snapshot().get("entries", 0)) if background else 0,
        }
        l10 = resolved_runtime.l10
        states["L10"] = {
            "state": "available" if l10 is not None else "disabled",
            "detail": "独立深缓冲；承接 L1 溢出；重启即空" if l10 else "未启用",
            "count": (l10.snapshot().get("entries", 0)) if l10 else 0,
        }
        states["L3"] = {
            "state": "available" if resolved_settings.memory.l3_enabled else "disabled",
            "detail": "持久人物事实（来源可核对的明确陈述/偏好）",
        }
        states["L2.5"] = {
            "state": "available" if resolved_settings.archive.enabled else "disabled",
            "detail": "归档/整理任务过程态（见 /memory/archive），不是独立记忆库",
        }
        semantic = resolved_runtime.semantic
        vector_state = semantic.status() if semantic is not None else {
            "state": "disabled", "provider_model": "",
        }
        vector_state["count"] = 0
        try:
            stats = await resolved_runtime.memory.vector_stats()
            vector_state["count"] = int(stats.get("chunks", 0))
        except Exception:
            pass
        states["vector"] = vector_state
        return states

    @app.get("/api/v1/agents/{agent_id}/memory/layers",
             summary="per-Agent 记忆层目录（职责/容量/持久性/状态，真实开关与内存快照）")
    async def agent_memory_layers(agent_id: str):
        agent = resolved_settings.find_agent(agent_id)
        if agent is None:
            raise _error(404, "unknown_agent", f"未配置的 Agent：{agent_id}")
        memory = resolved_runtime.memory
        if not getattr(memory, "available", False):
            raise _error(503, "memory_unavailable", "记忆存储未就绪")
        result = await memory.layers(runtime_states=await _memory_layer_states(agent_id))
        # P15-R01：per-Agent counts（按本 Agent 真实范围计算，不返全库数）。
        counts = await memory.layer_counts(agent_id)
        for item in result.get("layers", []):
            key = item.get("key")
            if key in counts:
                item["record_count"] = counts[key]
        result["counts"] = counts
        result["api_version"] = API_VERSION
        result["agent_id"] = agent_id
        return result

    @app.get("/api/v1/agents/{agent_id}/memory/live",
             summary="L1/L10 真实内存条目快照（当前进程内；重启即空，不伪装旧库）")
    async def memory_live(
        agent_id: str,
        layer: str = Query("L1", max_length=10),
        conversation_id: str = Query("", max_length=250),
        limit: int = Query(20, ge=1, le=50),
    ):
        agent = resolved_settings.find_agent(agent_id)
        if agent is None:
            raise _error(404, "unknown_agent", f"未配置的 Agent：{agent_id}")
        layer_key = layer.strip().upper()
        if layer_key not in {"L1", "L10"}:
            raise _error(422, "unsupported_live_layer", "live 快照只支持 L1/L10（其余层走 records）")
        if not conversation_id:
            raise _error(422, "scope_missing", "需要 conversation_id（按 Agent/会话范围查看）")
        # v6：旧入口转接完整聊天流水（同源窗口）；无流水时才回退进程内存快照。
        memory = _memory_for(agent_id)
        cid = await resource_scope(agent_id, conversation_id, None)
        try:
            items = await memory.chat_window(
                agent_id=agent_id, conversation_id=cid, limit=int(limit)
            )
        except Exception:  # noqa: BLE001
            items = []
        if items:
            return {
                "api_version": API_VERSION,
                "layer": layer_key,
                "state": "available",
                "entries": [
                    {
                        "seq": item.get("seq"),
                        "role": item.get("role"),
                        "display_name": item.get("display_name"),
                        "text": item.get("text"),
                        "occurred_at": item.get("occurred_at"),
                        "kind": item.get("kind"),
                    }
                    for item in items
                ],
                "count": len(items),
                "source": "chat_event",
                "legacy_compat": True,
                "reset_note": "旧兼容入口：已转接完整聊天流水；新页面使用 /chat/window",
            }
        if layer_key == "L1":
            background = resolved_runtime.background
            if background is None:
                return {"api_version": API_VERSION, "layer": "L1", "state": "disabled",
                        "entries": [], "count": 0}
            data = background.snapshot_entries(agent_id, conversation_id, limit=limit)
            return {"api_version": API_VERSION, "layer": "L1", "state": "available",
                    **data, "reset_note": "进程内存；重启即空（持久层走 records）"}
        l10 = resolved_runtime.l10
        if l10 is None:
            return {"api_version": API_VERSION, "layer": "L10", "state": "disabled",
                    "entries": [], "count": 0}
        data = l10.snapshot_entries(agent_id, conversation_id, limit=limit)
        return {"api_version": API_VERSION, "layer": "L10", "state": "available",
                **data, "reset_note": "进程内存深缓冲；重启即空（持久层走 records）"}

    @app.get("/api/v1/agents/{agent_id}/identities",
             summary="per-Agent 身份与别名（alias 缺省列最近身份）")
    async def agent_identities(
        agent_id: str,
        alias: str = Query("", max_length=100),
        limit: int = Query(20, ge=1, le=100),
        cursor: str = Query("", max_length=200),
    ):
        agent = resolved_settings.find_agent(agent_id)
        if agent is None:
            raise _error(404, "unknown_agent", f"未配置的 Agent：{agent_id}")
        memory = resolved_runtime.memory
        if not getattr(memory, "available", False):
            raise _error(503, "memory_unavailable", "记忆存储未就绪")
        if alias.strip():
            result = await memory.find_identities(
                agent_id=agent_id, alias=alias.strip(), limit=limit
            )
            return {"api_version": API_VERSION, "agent_id": agent_id, **result}
        try:
            result = await memory.identities_list(
                agent_id=agent_id, limit=limit, cursor=cursor
            )
        except (MemoryQueryError, MemoryQueryErrorP15) as exc:
            raise _error(422, "invalid_identity_query", str(exc)) from None
        return {"api_version": API_VERSION, "agent_id": agent_id, **result}

    @app.get("/api/v1/agents/{agent_id}/resources/{resource_id}/content",
             summary="显式作用域的资源本体读取/下载（哈希校验；HTML/脚本以下载返回；不自动挂下一轮模型）")
    async def resource_content(
        agent_id: str,
        resource_id: str,
        conversation_id: Optional[str] = Query(None, max_length=250),
        session_id: Optional[str] = Query(None, max_length=100),
    ):
        cid = await resource_scope(agent_id, conversation_id, session_id)
        catalog = resolved_runtime.files.catalog
        item = await catalog.get(
            agent_id=agent_id, conversation_id=cid, resource_id=resource_id
        )
        if item is None:
            raise _error(404, "resource_not_found", "文件不存在于当前作用域")
        try:
            data = await catalog.read_bytes(
                agent_id=agent_id, conversation_id=cid, resource_id=resource_id
            )
        except ResourceCatalogError as exc:
            status = 503 if exc.code in {"catalog_io_error", "catalog_closed"} else 422
            raise _error(status, exc.code, "原件读取失败（大小/哈希校验不通过或不可读）") from None
        # 只有图片/PDF 允许内联预览；HTML/脚本/未知类型一律以下载返回，
        # 不作为可执行页面（前端契约 §E）。
        mime = str(item.get("mime") or "application/octet-stream")
        name = str(item.get("name") or resource_id)
        inline_ok = mime.startswith("image/") or mime == "application/pdf"
        disposition = "inline" if inline_ok else "attachment"
        headers = {
            "Content-Disposition": disposition + "; filename*=UTF-8''" + __import__('urllib.parse',fromlist=['quote']).quote(name,safe=''),
            "X-Resource-Sha256": str(item.get("sha256") or ""),
            "X-Resource-Id": resource_id,
        }
        return Response(
            content=data,
            media_type="application/octet-stream" if not inline_ok else mime,
            headers=headers,
        )

    @app.get("/api/v1/events", summary="SSE 事件流（六类事件；有界队列；过期 Last-Event-ID 触发 resync）")
    async def events_stream(
        request: Request,
        agent_id: Optional[str] = Query(None, max_length=80),
        session_id: Optional[str] = Query(None, max_length=200),
    ):
        if agent_id and resolved_settings.find_agent(agent_id) is None:
            raise _error(404, "unknown_agent", f"未配置的 Agent：{agent_id}")
        bus = resolved_runtime.events
        if bus is None:
            raise _error(503, "events_unavailable", "事件流未装配")
        last_event_id = request.headers.get("last-event-id", "").strip()
        stale = bool(last_event_id) and not last_event_id.startswith(f"evt-{bus.boot_id}-")
        replay: list = []
        if last_event_id and not stale:
            # 同实例缺口：回放有界缓冲里该 ID 之后的、**按同一订阅过滤**的
            # 事件（与 live 同一谓词，其他 Agent 业务事件不因重连绕过）；
            # 滑出缓冲 → resync。
            replayed = bus.replay_from(
                last_event_id, agent_id=agent_id, session_id=session_id
            )
            if replayed["resync_required"]:
                stale = True
            else:
                replay = replayed["events"]
        subscription = bus.subscribe(agent_id=agent_id, session_id=session_id)

        async def generator():
            try:
                yield ": connected\n\n" if not stale else ": resync required\n\n"
                if stale:
                    # 浏览器可读的 resync 信号：控制事件帧 + 断开，客户端
                    # 重新拉 sessions/memory/layers/components 快照。
                    yield 'event: resync\ndata: {"reason":"stale_last_event_id"}\n\n'
                    return
                for event in replay:
                    yield format_sse(event)
                while True:
                    if subscription.overflow:
                        # 慢客户端丢事件：发控制事件后断开，客户端重拉快照，
                        # 不在不可靠的流里继续假装完整。
                        yield 'event: resync\ndata: {"reason":"overflow"}\n\n'
                        return
                    try:
                        event = await asyncio.wait_for(
                            subscription.queue.get(),
                            timeout=resolved_settings.events.heartbeat_seconds,
                        )
                        yield format_sse(event)
                    except asyncio.TimeoutError:
                        yield ": heartbeat\n\n"
                    except asyncio.CancelledError:
                        raise
            finally:
                bus.unsubscribe(subscription)

        headers = {"Cache-Control": "no-cache", "X-Accel-Buffering": "no"}
        if stale:
            headers["x-resync"] = "true"
        return StreamingResponse(
            generator(),
            media_type="text/event-stream",
            headers=headers,
        )

    async def resource_scope(agent_id, conversation_id, session_id):
        if resolved_settings.find_agent(agent_id) is None:
            raise _error(404,"unknown_agent","未配置的 Agent")
        if session_id:
            if conversation_id:
                raise _error(422,"resource_scope_conflict","仅指定 session_id 或 conversation_id")
            # 持久目录里的会话同样有效（重启后原件仍可访问，不静默换键）。
            await _ensure_session(agent_id, session_id)
            return f"http:{agent_id}:{session_id}"
        if not conversation_id:
            raise _error(422,"resource_scope_missing","需要明确会话作用域")
        from pero.channels.onebot11.mapping import parse_conversation_id
        parsed=parse_conversation_id(conversation_id)
        if parsed is None:
            raise _error(422,"invalid_conversation_id","通道会话标识无效")
        _channel,account,kind,target=parsed
        routes=resolved_settings.orchestration.routes
        from pero.access import service_for
        grant = service_for(resolved_runtime).grant_for_conversation(conversation_id, include_inactive=True) if kind == 'private' else None
        private_scope = bool(grant and grant['agent_id'] == agent_id)
        if not private_scope and not any(r.agent_id==agent_id and r.account==account and r.conversation_type==kind and r.target==target for r in routes):
            raise _error(404,"unknown_conversation","该 Agent 未绑定此通道会话")
        return conversation_id

    @app.get("/api/v1/file-tools", summary="文件能力元数据，不读取原件或调用API")
    async def file_tools_status():
        return {"api_version":API_VERSION,"files":resolved_runtime.files.metadata(),
            "accept_uploads":resolved_settings.files.accept_uploads}

    @app.get("/api/v1/image-inputs", summary="图片接收与本轮输入状态，不返回Base64")
    async def image_inputs():
        return {"api_version":API_VERSION,"enabled":resolved_settings.images.enabled,
            "input_format":"inline_base64","files_api_used":False,
            "remote_connectivity":"not_verified","extra_model_audit":False,
            "limits":resolved_settings.images.model_dump(),
            "ingress":resolved_runtime.image_ingress.status()}

    @app.post("/api/v1/agents/{agent_id}/resources", status_code=201,
              summary="接收文件原件，原始二进制正文，不唤醒模型")
    async def receive_resource(agent_id: str, request: Request,
            filename: str = Query(...,min_length=1,max_length=200),
            conversation_id: Optional[str] = Query(None,max_length=250),
            session_id: Optional[str] = Query(None,max_length=100),
            event_id: str = Query("",max_length=250)):
        cid=await resource_scope(agent_id,conversation_id,session_id)
        image_name=filename.rsplit(".",1)[-1].lower() in {"jpg","jpeg","png","gif","webp"}
        allow=resolved_settings.images.accept_uploads if image_name else resolved_settings.files.accept_uploads
        if not allow:
            raise _error(409,"uploads_disabled","原件接收默认关闭")
        if request.headers.get("content-type","").split(";",1)[0].strip()!="application/octet-stream":
            raise _error(415,"binary_body_required","使用 application/octet-stream 原始二进制正文")
        data=bytearray()
        async for chunk in request.stream():
            maximum=resolved_settings.images.max_bytes if image_name else resolved_settings.files.max_bytes
            if len(data)+len(chunk)>maximum:
                raise _error(413,"file_too_large","超过现有业务文件大小上限")
            data.extend(chunk)
        try:
            item=await resolved_runtime.files.catalog.ingest(agent_id=agent_id,
                conversation_id=cid,name=filename,data=bytes(data),event_id=event_id)
            if item["kind"]=="image":
                await resolved_runtime.files.catalog.register_image_source(agent_id=agent_id,conversation_id=cid,
                    resource_id=item["resource_id"],event_id=event_id or "http-upload:"+str(time.time_ns()),
                    sender_ref="http" if session_id else "unknown",observed_at=time.time())
        except ResourceCatalogError as exc:
            status=503 if exc.code in {"catalog_io_error","catalog_closed"} else 422
            raise _error(status,exc.code,"文件原件未保存") from None
        return {"api_version":API_VERSION,"item":item,"model_triggered":False}

    @app.get("/api/v1/agents/{agent_id}/resources", summary="当前作用域文件目录，无正文/云ID/磁盘路径")
    async def resource_list(agent_id: str,
            conversation_id: Optional[str]=Query(None,max_length=250),
            session_id: Optional[str]=Query(None,max_length=100),
            limit: int=Query(20,ge=1,le=100),offset: int=Query(0,ge=0),kind: str=Query('',pattern='^(|image|document)$')):
        cid=await resource_scope(agent_id,conversation_id,session_id)
        return {"api_version":API_VERSION,**await resolved_runtime.files.catalog.list(
            agent_id=agent_id,conversation_id=cid,limit=limit,offset=offset,kind=kind)}

    @app.get("/api/v1/agents/{agent_id}/resources/{resource_id}", summary="文件元数据详情，无正文")
    async def resource_detail(agent_id: str,resource_id: str,
            conversation_id: Optional[str]=Query(None,max_length=250),
            session_id: Optional[str]=Query(None,max_length=100)):
        cid=await resource_scope(agent_id,conversation_id,session_id)
        item=await resolved_runtime.files.catalog.get(agent_id=agent_id,conversation_id=cid,resource_id=resource_id)
        if item is None:
            raise _error(404,"resource_not_found","文件不存在于当前作用域")
        return {"api_version":API_VERSION,"item":item}

    @app.get("/api/v1/web-tools", summary="网页工具状态，不执行付费调用")
    async def web_status():
        return {"api_version": API_VERSION, "web": resolved_runtime.web.metadata()}

    @app.get(
        "/api/v1/agents/{agent_id}/stickers",
        summary="表情资产目录（有界分页、只元数据；无路径/Base64/整库）",
    )
    async def sticker_list(
        agent_id: str,
        conversation_id: str = Query(..., max_length=250),
        query: str = Query("", max_length=60),
        emotion: str = Query("", max_length=40),
        scene: str = Query("", max_length=60),
        limit: int = Query(100, ge=1, le=100),
        offset: int = Query(0, ge=0, le=10000),
    ):
        if resolved_runtime.stickers is None:
            raise _error(503, "stickers_unavailable", "表情资产后端未装配")
        cid = await resource_scope(agent_id, conversation_id, None)
        filters = dict(
            agent_id=agent_id,
            conversation_id=cid,
            query=query,
            emotion=emotion,
            scene=scene,
        )
        # sqlite 在线程执行（事件循环不做同步 IO）；total 按同一筛选条件计算。
        items = await asyncio.to_thread(
            resolved_runtime.stickers.store.find, limit=limit, offset=offset, **filters
        )
        total = await asyncio.to_thread(
            resolved_runtime.stickers.store.count_filtered, **filters
        )
        return {
            "api_version": API_VERSION,
            "enabled": resolved_settings.stickers.enabled,
            "auto_send_enabled": resolved_settings.stickers.auto_send_enabled,
            "total": total,
            "returned": len(items),
            "offset": offset,
            "has_more": offset + len(items) < total,
            "items": items,
        }

    @app.get(
        "/api/v1/agents/{agent_id}/stickers/{asset_id}",
        summary="表情资产详情（只元数据，校验作用域）",
    )
    async def sticker_detail(agent_id: str, asset_id: str,
            conversation_id: str = Query(..., max_length=250)):
        if resolved_runtime.stickers is None:
            raise _error(503, "stickers_unavailable", "表情资产后端未装配")
        cid = await resource_scope(agent_id, conversation_id, None)
        from pero.resources.stickers import valid_asset_id
        checked = valid_asset_id(asset_id)
        if checked is None:
            raise _error(422, "invalid_asset_id", "资产 ID 形态不合法")
        matches = await asyncio.to_thread(
            resolved_runtime.stickers.store.find,
            agent_id=agent_id, conversation_id=cid, asset_id=checked, limit=1,
            include_disabled=True,
        )
        if not matches:
            raise _error(404, "sticker_not_found", "该资产不在当前会话作用域内")
        return {"api_version": API_VERSION, "asset": matches[0]}

    @app.get("/api/v1/sticker-tools", summary="表情能力元数据，不发送消息")
    async def sticker_status():
        return {
            "api_version": API_VERSION,
            "stickers": resolved_settings.stickers.model_dump(),
            "bridge_extension": "not_verified",
        }

    @app.get("/api/v1/file-ingress", summary="桥文件补传状态（计数与最近结果，不含字节/路径）")
    async def file_ingress_status():
        if resolved_runtime.file_ingress is None:
            raise _error(503, "file_ingress_unavailable", "桥文件补传未装配（bridge_context.enabled=false）")
        return {
            "api_version": API_VERSION,
            "settings": resolved_settings.bridge_context.model_dump(),
            "metadata": resolved_runtime.file_ingress.metadata(),
            "status": resolved_runtime.file_ingress.status(),
        }

    @app.get("/api/v1/agents/{agent_id}/memory/archive", summary="脱敏归档任务状态")
    async def archive_status(agent_id: str, limit: int = Query(20, ge=1, le=100)):
        agent = resolved_settings.find_agent(agent_id)
        if agent is None:
            raise _error(404, "unknown_agent", "未配置的 Agent")
        memory, code, _detail = resolve_memory(resolved_runtime, agent)
        if memory is None:
            raise _error(409, code or "memory_unbound", "记忆仓库未就绪")
        status = await memory.archive_status(agent_id=agent_id, limit=limit)
        return {"api_version": API_VERSION, "agent_id": agent_id,
                "archive": resolved_runtime.archive.metadata(), "tasks": status}

    @app.exception_handler(ResourceCatalogError)
    async def resource_error(_request: Request, exc: ResourceCatalogError):
        status=(404 if exc.code=="resource_not_found" else
                503 if exc.code in {"catalog_io_error","catalog_closed"} else 422)
        return JSONResponse(status_code=status,content={"detail":{
            "error":exc.code,"message":"文件资源操作未完成"}})

    from pero.api.sticker_management import sticker_management_router
    app.include_router(sticker_management_router(resolved_runtime, resource_scope))
    return app


__all__ = ["create_app", "API_VERSION", "SLOT_TYPES"]
