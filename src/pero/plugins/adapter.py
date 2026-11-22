"""把插件声明的工具代理成 Pero 真实 Tool 协议对象。

注册表中的代理对象在启用后保持不变；停用、配置和版本更新只原子替换其
内部绑定。在途调用持有调用开始时取到的绑定，直到结束或取消。
"""

from __future__ import annotations

import inspect
import json
import re
import uuid
from dataclasses import dataclass
from typing import Any, Callable, Mapping, Optional

from pero.core.contracts import ToolSpec
from pero.plugins.errors import PluginError
from pero.plugins.manifest import PluginManifest
from pero.plugins.sdk import (
    PluginRequestContext,
    PluginSDK,
    PluginStorage,
    _scope_ref,
    sdk_scope,
)
from pero.tools.context import current_tool_context

_RESOURCE_ID = re.compile(r"^r-[0-9a-f]{32}$")
_STATUSES = frozenset({"complete", "partial", "unavailable", "failed"})
_MAX_DATA_CHARS = 3000


@dataclass(frozen=True)
class ToolBinding:
    spec: ToolSpec
    handler: Callable[..., Any]
    version: str
    config: Mapping[str, Any]
    generation: int


def _controlled_error(code: str, message: str = "") -> dict:
    return {
        "ok": False,
        "status": "failed",
        "error_code": str(code)[:80],
        "error": {"code": str(code)[:80], "message": str(message or code)[:200]},
        "data": {},
        "artifacts": [],
        "warnings": [],
    }


def _normalize(result: Any, *, plugin_id: str, version: str, tool: str) -> dict:
    if not isinstance(result, Mapping):
        raise PluginError("plugin_result_invalid", "插件结果必须是对象")
    ok = bool(result.get("ok", True))
    status = str(result.get("status") or ("complete" if ok else "failed"))
    if status not in _STATUSES:
        raise PluginError("plugin_result_invalid", "插件结果 status 不合法")
    data = result.get("data") or {}
    if not isinstance(data, Mapping):
        raise PluginError("plugin_result_invalid", "插件结果 data 必须是对象")
    try:
        encoded = json.dumps(dict(data), ensure_ascii=False)
    except (TypeError, ValueError):
        raise PluginError("plugin_result_invalid", "插件结果 data 无法编码") from None
    if len(encoded) > _MAX_DATA_CHARS:
        raise PluginError("plugin_result_too_large", "插件结果 data 超过长度上限")
    if "base64," in encoded or "data:image/" in encoded:
        raise PluginError("plugin_result_invalid", "插件结果不得携带 Base64 图片")
    artifacts = []
    for item in result.get("artifacts") or []:
        if not isinstance(item, Mapping):
            raise PluginError("plugin_result_invalid", "插件产物声明不合法")
        rid = str(item.get("resource_id") or "")
        if not _RESOURCE_ID.fullmatch(rid):
            raise PluginError("plugin_result_invalid", "插件产物编号不合法")
        artifacts.append({
            "kind": str(item.get("kind") or "image")[:32],
            "resource_id": rid,
            "mime": str(item.get("mime") or "")[:32],
            "size": int(item.get("size") or 0),
        })
        if len(artifacts) > 16:
            raise PluginError("plugin_result_invalid", "插件产物数量过多")
    warnings = []
    for item in result.get("warnings") or []:
        warnings.append(str(item)[:200])
        if len(warnings) > 10:
            raise PluginError("plugin_result_invalid", "插件警告数量过多")
    error = result.get("error") or {}
    if not isinstance(error, Mapping):
        raise PluginError("plugin_result_invalid", "插件错误声明不合法")
    error_out = {
        "code": str(error.get("code") or "plugin_rejected")[:80],
        "message": str(error.get("message") or "")[:200],
    } if (not ok or status == "failed") else None
    cache = result.get("cache") if isinstance(result.get("cache"),Mapping) else (data.get("cache") if isinstance(data.get("cache"), Mapping) else {})
    return {
        "ok": ok,
        "status": status,
        "tool": tool,
        "plugin": plugin_id,
        "version": version,
        "error_code": (error_out or {}).get("code"),
        "cache_hit": bool(cache.get("hit", False)),
        "data": dict(data),
        "artifacts": artifacts,
        "warnings": warnings,
        "error": error_out,
    }


class PluginToolAdapter:
    """注册表里登记的代理对象；不直接暴露插件模块或函数。"""

    def __init__(self, runtime: Any, tool_name: str) -> None:
        self._runtime = runtime
        self.tool_id = tool_name
        self.timeout_seconds = runtime.timeout_seconds
        self.backend = None

    def spec(self) -> ToolSpec:
        binding = self._runtime.binding(self.tool_id)
        if binding is None:
            # 停用后仍保留声明对象，避免破坏已构造的请求；模型侧由 available 过滤。
            binding = self._runtime.last_binding(self.tool_id)
        if binding is None:
            raise PluginError("plugin_unavailable", "插件工具声明不可用")
        if self.tool_id in self._runtime.manifest.background_tools:
            from dataclasses import replace
            return replace(binding.spec,description=binding.spec.description+'；这是后台工具，提交后先回复正在处理，不等待；完成会自动回报。')
        return binding.spec

    def available(self, context: Any) -> bool:
        if not self._runtime.enabled:
            return False
        worker=self._runtime.worker
        if worker is not None and not getattr(worker,"healthy",getattr(worker,"_proc",None) is not None):return False
        if self._runtime.binding(self.tool_id) is None:
            return False
        conversation_id = getattr(context, "conversation_id", "")
        return self._runtime.scope_allowed(conversation_id)

    async def run(self, arguments: Mapping[str, Any]) -> Mapping[str, Any]:
        if self.tool_id in self._runtime.manifest.background_tools:
            context=current_tool_context()
            if context is None or not self.available(context):return _controlled_error('scope_denied','工具未授权')
            submit=getattr(context,'background_submit',None)
            if not submit:return _controlled_error('background_unavailable','当前入口不支持自动后台回报')
            try:return await submit(self,dict(arguments))
            except PluginError as exc:return _controlled_error(exc.code,exc.message)
        return await self.run_now(arguments)

    async def run_now(self, arguments: Mapping[str, Any]) -> Mapping[str, Any]:
        binding = self._runtime.binding(self.tool_id)
        if binding is None or not self._runtime.enabled:
            return _controlled_error("plugin_disabled", "插件已停用或尚未启用")
        context = current_tool_context()
        if context is None or not self._runtime.scope_allowed(getattr(context, "conversation_id", "")):
            return _controlled_error("scope_denied", "当前会话未授权使用该插件")

        run_id = str(getattr(context, "run_id", "") or uuid.uuid4().hex)
        request = PluginRequestContext(
            run_id=run_id,
            agent_id=str(context.agent_id),
            conversation_id=str(context.conversation_id),
            scope_ref=_scope_ref(str(context.agent_id), str(context.conversation_id)),
            is_cancelled=getattr(context, "is_cancelled", lambda: False),
            channel_id=str(getattr(context, "channel_id", "") or ""),
            background_timeout=getattr(context,"background_timeout",None),
        )
        images = getattr(context, "images", None)
        catalog = getattr(getattr(images, "backend", None), "catalog", None) or getattr(getattr(getattr(context,"files",None),"backend",None),"catalog",None)
        sdk = PluginSDK(
            plugin_id=self._runtime.plugin_id,
            manifest=self._runtime.manifest,
            context=request,
            config=self._runtime.effective_config(request.conversation_id,binding.config),
            storage=PluginStorage(self._runtime.data_dir / "scopes" / request.scope_ref),
            catalog=catalog,
            images=images,
            events=self._runtime.events,
            network_hosts=self._runtime.network_hosts,
        )
        plugin = self._runtime.plugin_id
        version = binding.version
        try:
            with sdk_scope(sdk):
                outcome = binding.handler(dict(arguments), sdk)
                if inspect.isawaitable(outcome):
                    outcome = await outcome
            if isinstance(outcome,Mapping):
                outcome=dict(outcome)
                refs=[]
                for item in outcome.get('artifacts') or []:
                    if isinstance(item,Mapping) and item.get('handle'):
                        meta=sdk._published.get(item['handle'])
                        if meta is None:raise PluginError('artifact_not_found','未知本轮产物')
                        refs.append(meta)
                    else:refs.append(item)
                outcome['artifacts']=refs
            return _normalize(outcome, plugin_id=plugin, version=version, tool=self.tool_id)
        except PluginError as exc:
            return _controlled_error(exc.code, exc.message)
        except Exception as exc:
            return _controlled_error("plugin_error", type(exc).__name__)

    def deactivate(self) -> None:
        """停用不移除代理；在途调用不受影响，新调用由 runtime 状态拒绝。"""

        self._runtime.deactivate()

    def retarget(self, runtime: Any) -> None:
        """版本切换时把同一代理指向新运行态，不重复登记工具名。"""

        self._runtime = runtime


__all__ = ["PluginToolAdapter", "ToolBinding"]
