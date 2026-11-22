"""按 Agent 解析通道与会话策略的绑定结果。

编排器不持有"全局通道/全局策略实例"作为兜底：每个路由命中的 Agent 都必须
在注册表里真实绑定 `channel` 与 `conversation_policy` 槽位，类型与实例能力
也要匹配。解析失败时返回受控错误码，由调用方明确拒绝，而不是退回默认实例。

P04 只有一种会话策略实现（`policy.window_text`），但绑定检查照样生效：
未绑定 / 未登记 / 类型不符 / 实例不可用都会拒绝；同一个组件 ID 被多个 Agent
绑定时共享同一实例（显式共享），窗口状态按 `(agent_id, conversation_id)` 隔离。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Optional


@dataclass(frozen=True)
class AgentBinding:
    """一个 Agent 实际解出的通道、会话策略与（可选）持久记忆实例。"""

    agent_id: str
    channel_component_id: str
    policy_component_id: str
    channel: Any
    policy: Any
    #: 未绑定 memory_repository 时为 None（明确进入"仅内存"模式，不是静默降级）；
    #: 绑定但不可用时不会生成绑定结果，由 `resolve_agent_binding` 明确拒绝。
    memory: Any = None
    memory_component_id: Optional[str] = None
    memory_detail: str = ""
    #: 回答前检索组件（可选）。未绑定时本轮不检索，且不会偷偷退到全局库。
    retriever: Any = None
    retriever_component_id: Optional[str] = None
    retriever_detail: str = ""

    @property
    def channel_id(self) -> str:
        """通道实例自报的通道标识，用于校验入站消息是否来自该通道。"""

        return str(getattr(self.channel, "channel_id", "") or "")

    @property
    def persists(self) -> bool:
        return self.memory is not None


@dataclass(frozen=True)
class BindingResolution:
    """解析结果：成功时带 `binding`，失败时带受控错误码与说明。"""

    binding: Optional[AgentBinding] = None
    error_code: Optional[str] = None
    detail: str = ""

    @property
    def ready(self) -> bool:
        return self.binding is not None


#: `agent_id → BindingResolution`。由装配层注入，编排器只读缓存。
AgentBindingResolver = Callable[[str], BindingResolution]


def unresolved(code: str, detail: str) -> BindingResolution:
    """构造一个明确的未就绪结果（不做任何回退）。"""

    return BindingResolution(binding=None, error_code=code, detail=detail)


__all__ = [
    "AgentBinding",
    "AgentBindingResolver",
    "BindingResolution",
    "unresolved",
]
