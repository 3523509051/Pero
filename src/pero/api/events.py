"""SSE 事件流（P15）：六类真实事件，有界队列，慢客户端不阻塞主链路。

设计：
- 单实例 `EventBus`：每订阅者独立 `asyncio.Queue`（有界），发布用
  `put_nowait`——队列满即丢弃并给该订阅者标记 overflow，**绝不阻塞**
  模型运行或 WS 接收循环；
- 事件 ID 含**启动实例范围**（boot uuid + 序号）：旧 Last-Event-ID 一律
  视为过期，响应头带 `x-resync: true`，客户端重拉会话/记忆/组件快照，
  不做永久事件回放；
- 事件在**真实状态点**发出：memory.changed 在事务提交成功后、tool.finished
  带准确成功/失败/取消、消息区分 generated（生成候选）与 delivered（已交付）；
  组件未启用发 `unavailable`，不伪 connected；
- 载荷只放记录 ID 与短状态（有界截断），正文/工具全文/附件/错误体由
  分页与详情接口拉取，不进事件流；不含思维链（执行进度不是思维链）。
"""

from __future__ import annotations

import asyncio
import json
import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Mapping, Optional

logger = logging.getLogger(__name__)

EVENT_TYPES = (
    "agent.status",
    "message.created",
    "tool.started",
    "tool.finished",
    "memory.changed",
    "component.status",
    "plugin.status",
    "plugin.progress",
    "plugin.record",
)

#: **全局事件**（P15-R02）：按已声明的全局语义广播给全部订阅者，不按
#: Agent/会话过滤——与业务消息/工具/记忆事件（按订阅过滤）严格区分。
GLOBAL_EVENT_TYPES = frozenset({"component.status"})


def _matches(
    *,
    agent_id: Optional[str],
    session_id: Optional[str],
    event: Mapping[str, Any],
) -> bool:
    """live 与 replay 共用的**唯一过滤谓词**：一条规则，两条路径。

    - 全局事件（component.status 等，agent_id 为空）→ 命中全部订阅者；
    - 业务事件：订阅声明了 agent_id/session_id 就必须精确匹配——其他
      Agent 的业务事件不因重连/回放绕过过滤。
    """

    if event.get("type") in GLOBAL_EVENT_TYPES or not event.get("agent_id"):
        return True
    if agent_id and agent_id != event.get("agent_id"):
        return False
    if session_id and session_id != event.get("session_id"):
        return False
    return True


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class Subscription:
    queue: "asyncio.Queue[Dict[str, Any]]"
    agent_id: Optional[str]
    session_id: Optional[str]
    overflow: bool = False
    dropped: int = 0


@dataclass
class BusStats:
    published: int = 0
    dropped: int = 0
    subscribers: int = 0


class EventBus:
    """进程内事件总线；重启即空（事件 ID 实例范围因此是诚实的）。"""

    def __init__(self, *, queue_max: int = 64, buffer_limit: int = 256,
                 payload_max_chars: int = 500) -> None:
        self._queue_max = max(1, int(queue_max))
        self._buffer_limit = max(1, int(buffer_limit))
        self._payload_max_chars = max(1, int(payload_max_chars))
        self._boot_id = uuid.uuid4().hex[:8]
        self._seq = 0
        self._subs: List[Subscription] = []
        #: 最近事件的有界环形缓冲（只用于诊断计数，不做回放工程）。
        self._recent: List[Dict[str, Any]] = []
        self.stats = BusStats()

    # ------------------------------------------------------------------ #
    # 发布（同步、非阻塞；模型/WS 链路内安全调用）
    # ------------------------------------------------------------------ #

    def publish(
        self,
        event_type: str,
        *,
        agent_id: str = "",
        session_id: str = "",
        data: Optional[Mapping[str, Any]] = None,
    ) -> Optional[str]:
        """发布一条事件；返回事件 ID（失败返回 None，不影响调用方）。"""
        if event_type not in EVENT_TYPES:
            return None
        self._seq += 1
        event_id = f"evt-{self._boot_id}-{self._seq}"
        payload = _bounded(data or {}, self._payload_max_chars)
        event = {
            "id": event_id,
            "type": event_type,
            "agent_id": str(agent_id or "")[:120],
            "session_id": str(session_id or "")[:120],
            "occurred_at": _now(),
            "data": payload,
        }
        self.stats.published += 1
        self._recent.append(event)
        if len(self._recent) > self._buffer_limit:
            del self._recent[: len(self._recent) - self._buffer_limit]
        for sub in list(self._subs):
            if not _matches(
                agent_id=sub.agent_id, session_id=sub.session_id, event=event
            ):
                continue
            try:
                sub.queue.put_nowait(event)
            except asyncio.QueueFull:
                # 慢客户端：丢弃并标记 overflow，不阻塞生产者；
                # 订阅协程消费该标志后向客户端发 resync 并断开（客户端重拉快照）。
                sub.overflow = True
                sub.dropped += 1
                self.stats.dropped += 1
        return event_id

    def replay_from(
        self,
        last_event_id: str,
        *,
        agent_id: Optional[str] = None,
        session_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """同实例 Last-Event-ID 缺口回放（**与 live 同一过滤谓词**）。

        - ID 属于当前实例且仍在有界缓冲内 → 返回其后的、**按订阅过滤**的
          事件（按序）——其他 Agent 的业务事件不因重连绕过；
        - ID 属于当前实例但已滑出缓冲 → `resync_required=True`（缺口过大）；
        - ID 属于旧实例/不存在 → `resync_required=True`（客户端重拉快照）。
        """
        if not last_event_id.startswith(f"evt-{self._boot_id}-"):
            return {"events": [], "resync_required": True}
        for position, event in enumerate(self._recent):
            if event["id"] == last_event_id:
                replayed = [
                    candidate
                    for candidate in self._recent[position + 1:]
                    if _matches(
                        agent_id=agent_id, session_id=session_id, event=candidate
                    )
                ]
                return {"events": replayed, "resync_required": False}
        return {"events": [], "resync_required": True}

    # ------------------------------------------------------------------ #
    # 订阅
    # ------------------------------------------------------------------ #

    def subscribe(
        self, *, agent_id: Optional[str] = None, session_id: Optional[str] = None
    ) -> Subscription:
        sub = Subscription(
            queue=asyncio.Queue(maxsize=self._queue_max),
            agent_id=agent_id or None,
            session_id=session_id or None,
        )
        self._subs.append(sub)
        self.stats.subscribers = len(self._subs)
        return sub

    def unsubscribe(self, sub: Subscription) -> None:
        if sub in self._subs:
            self._subs.remove(sub)
        self.stats.subscribers = len(self._subs)

    @property
    def boot_id(self) -> str:
        return self._boot_id

    def snapshot_hint(self) -> Dict[str, Any]:
        return {
            "boot_id": self._boot_id,
            "recent_count": len(self._recent),
            "buffer_limit": self._buffer_limit,
            "note": "不做永久回放；断线/过期后重拉 sessions/memory/components 快照",
        }


def _bounded(data: Mapping[str, Any], max_chars: int) -> Dict[str, Any]:
    """载荷有界化：**整个事件一个总预算**，逐字段扣减（不是每字段各给满额）；
    嵌套值序列化后同样计入总预算；字段最多 12 个。"""
    out: Dict[str, Any] = {}
    remaining = max(1, int(max_chars))
    for key, value in list(data.items())[:12]:
        if remaining <= 0:
            break
        if isinstance(value, (int, float, bool)) or value is None:
            out[str(key)[:40]] = value
            remaining -= len(str(value))
            continue
        budget = remaining
        if isinstance(value, str):
            text = value[:budget]
            out[str(key)[:40]] = text + ("…" if len(value) > budget else "")
            remaining -= len(text)
        else:
            text = json.dumps(value, ensure_ascii=False, default=str)[:budget]
            out[str(key)[:40]] = text
            remaining -= len(text)
    return out


def format_sse(event: Mapping[str, Any]) -> str:
    """单条事件的 SSE 帧。"""
    return (
        f"id: {event['id']}\n"
        f"event: {event['type']}\n"
        f"data: {json.dumps(event, ensure_ascii=False, default=str)}\n\n"
    )
