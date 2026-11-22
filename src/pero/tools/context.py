"""单次运行的上下文（agent / 会话 / 身份 / 记忆与检索组件）。

工具是**进程级单例**，同一时刻可能有多个会话在跑；因此不能用实例字段存运行期状态。
这里用 `ContextVar`：编排层在进入本轮主循环前设置，工具在执行时读取，
并发任务之间互不可见，也不会把 A 群的会话信息带进 B 群。
"""

from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Iterator, Optional, Tuple


def _task_cancelling() -> bool:
    """当前任务是否已收到取消请求（3.11+ 的 ``Task.cancelling``；旧版本恒 False）。"""

    import asyncio

    task = asyncio.current_task()
    if task is None:
        return False
    cancelling = getattr(task, "cancelling", None)
    return bool(cancelling() if callable(cancelling) else False)


@dataclass(frozen=True)
class SaveGrant:
    """一条**可信保存授权**：由编排按真实原文识别出来的请求，不是模型参数。

    `remember` 只能引用这里已有的来源（`record_id`），不能凭参数新造事实：
    - `status` 是识别阶段的结果（已保存 / 需要补充 / 对象歧义 / 超长 / 失败）；
    - `memory_id` 只在事务提交后才有值；
    - `request_text` 是该条原文的真实内容，工具不得改用模型给的 `content`。
    """

    record_id: str
    status: str
    request_text: str = ""
    content: str = ""
    memory_id: Optional[str] = None
    subject_identity_id: Optional[str] = None
    subject_label: Optional[str] = None
    detail: str = ""


@dataclass(frozen=True)
class ToolContext:
    """本轮工具可用的受控上下文（不含 Key、不含正文原文的额外副本）。"""

    agent_id: str
    conversation_id: str
    channel_id: str
    account_id: str
    identity_id: Optional[str] = None
    turn_id: Optional[str] = None
    #: 单次运行标识（P12-R02）：每次 run 执行独立生成，与可选的持久 L2 turn_id
    #: 区分；运行期状态（如表情保存名额）按它归属与清理，不由工具参数自行设置。
    run_id: Optional[str] = None
    record_ids: Tuple[str, ...] = ()
    memory: Any = None
    retriever: Any = None
    files: Any = None
    images: Any = None
    configuration: Any = None
    credential_cache: dict = field(default_factory=dict, repr=False)
    execution_evidence: dict = field(default_factory=dict, repr=False)
    #: 会话引用（工具写 L4 时需要完整的通道/账号/目标信息）。
    ref: Any = None
    #: 本轮可信保存授权：`record_id → SaveGrant`（只有真实识别过的请求才在里面）。
    save_grants: Dict[str, SaveGrant] = field(default_factory=dict)
    #: 本轮是否已取消（等价旧插件 `event.is_current` 的受控检查）；
    #: 默认退回"当前任务 cancelling()"，编排可注入更精确的轮次级检查。
    is_cancelled: Callable[[], bool] = field(default_factory=lambda: _task_cancelling)
    extras: Tuple[Tuple[str, str], ...] = field(default_factory=tuple)
    background_submit: Optional[Callable[..., Any]] = field(default=None,repr=False)
    background_timeout: Optional[float] = None


_CURRENT: ContextVar[Optional[ToolContext]] = ContextVar(
    "pero_tool_context", default=None
)


def current_tool_context() -> Optional[ToolContext]:
    """当前任务绑定的工具上下文；未绑定时返回 `None`（工具应明确报错）。"""

    return _CURRENT.get()


@contextmanager
def tool_scope(context: ToolContext) -> Iterator[None]:
    """在本轮主循环内绑定工具上下文，退出时恢复（不泄漏到别的会话）。"""

    token = _CURRENT.set(context)
    try:
        yield
    finally:
        _CURRENT.reset(token)


__all__ = ["SaveGrant", "ToolContext", "current_tool_context", "tool_scope"]
