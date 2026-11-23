"""内存会话存储。

明确边界：
- 只在进程内存中，重启即丢失；**不等于 L2/L20 或任何持久记忆**。
- 会话按 `agent_id` + `session_id` 隔离，不与其他 Agent 混用。
- 同一会话正在运行时标记为 busy，第二个请求被拒绝，不会造成历史交错。
"""

from __future__ import annotations

import threading
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional

from pero.core.contracts import ModelMessage


def _now() -> str:
    from pero.core.timebase import now_iso

    return now_iso()


@dataclass
class RunSummary:
    """一次运行的脱敏摘要，不含正文。"""

    run_id: str
    status: str
    error_code: Optional[str]
    started_at: str
    finished_at: str
    model_calls: int
    tool_calls: int


@dataclass
class AgentSession:
    session_id: str
    agent_id: str
    created_at: str
    updated_at: str
    history: List[ModelMessage] = field(default_factory=list)
    run_count: int = 0
    last_run: Optional[RunSummary] = None
    busy: bool = False

    @property
    def message_count(self) -> int:
        return len(self.history)


class SessionStore:
    """进程内会话表；键为 (agent_id, session_id)。"""

    def __init__(self, *, max_history_messages: int) -> None:
        if max_history_messages <= 0:
            raise ValueError("max_history_messages 必须是正数")
        self._max_history_messages = max_history_messages
        self._sessions: Dict[tuple[str, str], AgentSession] = {}
        self._lock = threading.RLock()

    @property
    def max_history_messages(self) -> int:
        return self._max_history_messages

    def create(self, agent_id: str) -> AgentSession:
        session_id = f"s-{uuid.uuid4().hex[:16]}"
        now = _now()
        session = AgentSession(
            session_id=session_id,
            agent_id=agent_id,
            created_at=now,
            updated_at=now,
        )
        with self._lock:
            self._sessions[(agent_id, session_id)] = session
        return session

    def get(self, agent_id: str, session_id: str) -> Optional[AgentSession]:
        with self._lock:
            return self._sessions.get((agent_id, session_id))

    def acquire(self, agent_id: str, session_id: str) -> Optional[AgentSession]:
        """**原子占用**单会话运行权：锁内检查并置 busy，防并发重复进入。

        返回 None = 会话不存在；busy → SessionBusyError（延迟导入避免循环）。
        所有出口必须 release。
        """
        from pero.agent.loop import SessionBusyError  # 延迟导入：loop 反向依赖本模块

        with self._lock:
            session = self._sessions.get((agent_id, session_id))
            if session is None:
                return None
            if session.busy:
                raise SessionBusyError(
                    f"会话正在处理上一条消息（agent={agent_id}, session={session_id}）"
                )
            session.busy = True
            return session

    def release(self, agent_id: str, session_id: str) -> None:
        with self._lock:
            session = self._sessions.get((agent_id, session_id))
            if session is not None:
                session.busy = False

    def create_with_id(self, agent_id: str, session_id: str) -> AgentSession:
        """按显式 ID 原子创建/复用（恢复路径防并发重复创建）。"""
        with self._lock:
            existing = self._sessions.get((agent_id, session_id))
            if existing is not None:
                return existing
            session = AgentSession(
                session_id=session_id,
                agent_id=agent_id,
                created_at=_now(),
                updated_at=_now(),
            )
            self._sessions[(agent_id, session_id)] = session
            return session

    def busy_count(self) -> int:
        with self._lock:
            return sum(bool(session.busy) for session in self._sessions.values())

    def count(self) -> int:
        with self._lock:
            return len(self._sessions)

    def touch(self, session: AgentSession) -> None:
        session.updated_at = _now()
