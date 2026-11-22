"""工具注册表：按固定名称登记真实工具对象，重复名称明确报错。

模型只能调用“本轮暴露”的工具：循环层拿 `specs()` 构造请求，
执行层只允许 `get(name)` 命中的对象，没有动态导入或按字符串调用任意函数。
"""

from __future__ import annotations

import threading
import json
from dataclasses import replace
from typing import Dict, List, Optional

from pero.core.contracts import Tool, ToolSpec


class ToolRegistryError(Exception):
    """注册表相关错误基类。"""


class DuplicateToolError(ToolRegistryError):
    """同名工具被重复登记。"""


class UnknownToolError(ToolRegistryError):
    """请求了未登记的工具。"""


class ToolRegistry:
    """进程级工具登记表；线程安全，只登记不热替换。"""

    def __init__(self) -> None:
        self._tools: Dict[str, Tool] = {}
        self._lock = threading.RLock()

    def register(self, tool: Tool) -> Tool:
        spec = tool.spec()
        if not isinstance(spec, ToolSpec):
            raise ToolRegistryError("tool.spec() 必须返回 ToolSpec")
        name = (spec.name or "").strip()
        if not name:
            raise ToolRegistryError("工具名不能为空")
        if tool.tool_id != name:
            raise ToolRegistryError(
                f"工具对象标识与声明名称不一致：{tool.tool_id!r} != {name!r}"
            )
        with self._lock:
            if name in self._tools:
                raise DuplicateToolError(f"工具名已登记，不允许覆盖：{name}")
            self._tools[name] = tool
        return tool

    def get(self, name: str) -> Tool:
        with self._lock:
            try:
                return self._tools[name]
            except KeyError as exc:
                raise UnknownToolError(f"未登记的工具：{name}") from exc

    def unregister(self, name: str, expected: Optional[Tool] = None) -> bool:
        """移除插件代理；`expected` 按对象身份核对，避免误删别的工具。

        内置工具没有生命周期，只有插件卸载/失败回滚会调用；在途调用持有
        旧代理引用，移除注册表不影响其完成。
        """

        with self._lock:
            current = self._tools.get(name)
            if current is None or (expected is not None and current is not expected):
                return False
            del self._tools[name]
            return True

    def has(self, name: str) -> bool:
        with self._lock:
            return name in self._tools

    def names(self) -> List[str]:
        with self._lock:
            return sorted(self._tools)

    def specs(self, configuration=None) -> List[ToolSpec]:
        with self._lock:
            tools = list(self._tools.values())
        result = []
        for tool in sorted(tools, key=lambda item: item.tool_id):
            from pero.tools.context import current_tool_context
            context=current_tool_context()
            if context is not None and hasattr(tool,'available') and not tool.available(context):continue
            config = configuration.tool(tool.tool_id) if configuration else {}
            if config.get('enabled') is False:
                continue
            spec = tool.spec()
            from pero.configuration.prompt_files import default_prompt
            spec = replace(spec,description=default_prompt(tool.tool_id,spec.description))
            params = json.loads(json.dumps(dict(spec.parameters)))
            defaults = config.get('defaults') or {}
            for name, value in defaults.items():
                if name in params.get('properties', {}):
                    params['properties'][name]['default'] = value
            if 'required' in params:
                params['required'] = [name for name in params['required'] if name not in defaults]
            description=config.get('prompt') or spec.description
            runtime=getattr(tool,'_runtime',None)
            manifest=getattr(runtime,'manifest',None)
            if tool.tool_id in getattr(manifest,'background_tools',()):
                description+='\n[宿主后台执行约定] 必须先实际调用本工具；仅说正在处理不会创建任务。只有返回 queued 和 job_id 才表示已提交，宿主随后负责自动回报。不能把旧助手承诺当成任务成功。'
            result.append(replace(spec, description=description, parameters=params))
        return result

    def __len__(self) -> int:
        with self._lock:
            return len(self._tools)

    def __contains__(self, name: object) -> bool:
        return isinstance(name, str) and self.has(name)
