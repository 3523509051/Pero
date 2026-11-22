"""工具层：注册表、参数校验、执行器与真实工具。

P06 起注册：`get_current_time`（本地只读）、`remember`（明确保存到 L4）、
`recall`（有界关键词检索）；remember/recall 依赖本轮绑定的工具上下文。
"""

from pero.tools.builtin import GetCurrentTimeTool
from pero.tools.context import ToolContext, current_tool_context, tool_scope
from pero.tools.executor import ToolExecutor, ToolOutcome
from pero.tools.memory_tools import RecallTool, RememberTool
from pero.tools.registry import (
    DuplicateToolError,
    ToolRegistry,
    ToolRegistryError,
    UnknownToolError,
)

__all__ = [
    "DuplicateToolError",
    "GetCurrentTimeTool",
    "RecallTool",
    "RememberTool",
    "ToolContext",
    "ToolExecutor",
    "ToolOutcome",
    "ToolRegistry",
    "ToolRegistryError",
    "UnknownToolError",
    "current_tool_context",
    "tool_scope",
]
