"""有界 L1 短期背景（进程内存，重启即空）。

语义迁移自 webot 插件的 L1（`window.py`）：只保留"最近发生过什么"的短事件行，
供下一轮主回复做背景参考。与旧实现的差别：

- 按来源事件 ID、发送者稳定 ID、显示名、时间记录，正文截断；
- 允许的群消息**未唤醒也更新**背景，但绝不因此开窗、调模型或触发归档；
- 已被轮次消费（begin_turn 归属）的事件立即出队，天然与当前批次/已载入 L2 去重；
- 状态接口只返回计数，不返回背景正文。
"""

from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass
from typing import Callable, Deque, Dict, List, Optional, Sequence, Tuple

from pero.core.contracts import InboundMessage

MAX_TEXT_CHARS = 160


def _bj_stamp(timestamp: float, pattern: str) -> str:
    """把时间戳渲染成北京时间文本（不用 `time.localtime` 依赖宿主机时区）。"""

    from pero.core.timebase import to_beijing

    moment = to_beijing(float(timestamp))
    return moment.strftime(pattern) if moment is not None else "未知时间"


@dataclass
class BackgroundEntry:
    """一条短期背景事件；`text` 已截断，附件只有目录占位。"""

    event_id: str
    sender_ref: str
    display_name: str
    occurred_at: float
    text: str
    attachment_note: str = ""


class ConversationBackground:
    """按 `(Agent, 会话)` 隔离的有界背景缓冲。确定性实现，不调用模型。"""

    def __init__(
        self,
        *,
        max_turns: int,
        max_chars: int,
        idle_seconds: float,
        max_conversations: int,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._max_turns = max(int(max_turns), 1)
        self._max_chars = max(int(max_chars), 1)
        self._idle_seconds = max(float(idle_seconds), 1.0)
        self._max_conversations = max(int(max_conversations), 1)
        self._clock = clock
        self._entries: Dict[Tuple[str, str], Deque[BackgroundEntry]] = {}
        #: L1 → L10 溢出承接（可选注入；未注入时淘汰即丢弃）。
        self._overflow_sink = None

    # ------------------------------------------------------------------ #
    # 写入
    # ------------------------------------------------------------------ #

    def observe(self, agent_id: str, message: InboundMessage) -> bool:
        """记录一条允许群消息（未唤醒也可更新）；返回是否真的写入。

        只接受群聊、允许的会话；正文与附件说明都截断，不写 Base64/正文原文之外的
        任何东西（工具结果、网页正文等根本不会传进来）。
        """

        if message.conversation_type != "group":
            return False
        key = (agent_id, message.conversation_id)
        queue = self._entries.get(key)
        if queue is None:
            if len(self._entries) >= self._max_conversations:
                self._evict_idle(force_one=True)
                if len(self._entries) >= self._max_conversations:
                    return False
            queue = deque(maxlen=self._max_turns)
            self._entries[key] = queue
        text = (message.text or "").strip()
        if len(text) > MAX_TEXT_CHARS:
            text = text[:MAX_TEXT_CHARS] + "…"
        note = ""
        for attachment in message.attachments:
            kind = attachment.kind
            if kind == "image":
                note = f"[图片 {attachment.handle[:16]}]"
            elif kind in {"file", "audio", "video"}:
                note = f"[{kind} {attachment.handle[:16]}]"
            if note:
                break
        entry = BackgroundEntry(
            event_id=message.event_id,
            sender_ref=message.sender_ref,
            display_name=message.group_display_name or message.sender_ref,
            occurred_at=self._clock(),
            text=text,
            attachment_note=note,
        )
        # 同一事件重复投递不重复记录。
        if any(existing.event_id == entry.event_id for existing in queue):
            return False
        if len(queue) >= self._max_turns:
            self._emit_overflow(key, queue.popleft())
        queue.append(entry)
        self._trim(key)
        return True

    def discard(self, agent_id: str, event_ids: Sequence[str]) -> int:
        """把已被轮次消费的事件从背景移除（与当前批次/L2 去重）。

        只处理 `agent_id` 自己的缓冲：不同 Agent 的会话键互不影响，
        同一事件 ID 出现在别的 Agent 背景里不会被误删。
        """

        if not event_ids:
            return 0
        removed = 0
        wanted = set(event_ids)
        for key, queue in list(self._entries.items()):
            if key[0] != agent_id:
                continue
            kept = deque(
                (entry for entry in queue if entry.event_id not in wanted),
                maxlen=self._max_turns,
            )
            removed += len(queue) - len(kept)
            if kept:
                self._entries[key] = kept
            else:
                self._entries.pop(key, None)
        return removed

    # ------------------------------------------------------------------ #
    # 渲染（本轮临时参考，计入请求预算；不是用户事实）
    # ------------------------------------------------------------------ #

    def render(self, agent_id: str, conversation_id: str) -> str:
        """渲染有界背景块；超预算时从最旧的一侧丢弃整行，不截半行。"""

        queue = self._entries.get((agent_id, conversation_id))
        if not queue:
            return ""
        now = self._clock()
        lines: List[str] = []
        total = 0
        for entry in reversed(queue):  # 新的在前
            if now - entry.occurred_at > self._idle_seconds:
                break
            name = entry.display_name[:24]
            line = f"- {name}：{entry.text}" if entry.text else f"- {name}：{entry.attachment_note or '（无文字）'}"
            if entry.attachment_note and entry.text:
                line = f"- {name}：{entry.text}（附 {entry.attachment_note}）"
            if total + len(line) + 1 > self._max_chars:
                break
            lines.append(line)
            total += len(line) + 1
        if not lines:
            return ""
        lines.reverse()  # 恢复时间正序
        return "# 近期群聊背景（临时参考，不是本轮用户发言）\n" + "\n".join(lines)

    # ------------------------------------------------------------------ #
    # 状态与清理
    # ------------------------------------------------------------------ #

    def snapshot(self) -> Dict[str, int]:
        """只返回计数与状态，不返回任何背景正文。"""

        now = self._clock()
        conversations = 0
        entries = 0
        expired = 0
        for key, queue in list(self._entries.items()):
            fresh = deque(
                (e for e in queue if now - e.occurred_at <= self._idle_seconds),
                maxlen=self._max_turns,
            )
            expired += len(queue) - len(fresh)
            if fresh:
                self._entries[key] = fresh
                conversations += 1
                entries += len(fresh)
            else:
                self._entries.pop(key, None)
        return {
            "conversations": conversations,
            "entries": entries,
            "expired": expired,
            "max_turns": self._max_turns,
            "max_chars": self._max_chars,
            "state": "memory",
        }

    def snapshot_entries(
        self, agent_id: str, conversation_id: str, *, limit: int = 20
    ) -> Dict[str, Any]:
        """真实内存条目快照（L1）：只有当前进程内的条目，重启即空。"""
        queue = self._entries.get((agent_id, conversation_id))
        now = self._clock()
        fresh = [
            e for e in (queue or [])
            if now - e.occurred_at <= self._idle_seconds
        ]
        rows = [
            {
                "event_id": e.event_id,
                "sender_ref": e.sender_ref,
                "display_name": e.display_name,
                "occurred_at": _bj_stamp(e.occurred_at, "%Y-%m-%d %H:%M:%S"),
                "preview": (e.text or e.attachment_note or "")[:80],
            }
            for e in fresh[-max(1, int(limit)):]
        ]
        rows.reverse()
        return {"entries": rows, "count": len(fresh), "storage": "disk_snapshot" if getattr(self,'_persistent',False) else "memory"}

    def _trim(self, key: Tuple[str, str]) -> None:
        queue = self._entries.get(key)
        if queue is None:
            return
        while len(queue) > self._max_turns:
            self._emit_overflow(key, queue.popleft())
        total = sum(len(e.text) + len(e.attachment_note) + 24 for e in queue)
        while total > self._max_chars and queue:
            self._emit_overflow(key, queue.popleft())
            total = sum(len(e.text) + len(e.attachment_note) + 24 for e in queue)

    def _evict_idle(self, force_one: bool = False) -> None:
        now = self._clock()
        oldest_key: Optional[Tuple[str, str]] = None
        oldest_time = None
        for key, queue in list(self._entries.items()):
            latest = queue[-1].occurred_at if queue else 0.0
            if now - latest > self._idle_seconds:
                for entry in queue:
                    self._emit_overflow(key, entry)
                self._entries.pop(key, None)
                continue
            if oldest_time is None or latest < oldest_time:
                oldest_time = latest
                oldest_key = key
        if force_one and oldest_key is not None and len(self._entries) >= self._max_conversations:
            for entry in self._entries.get(oldest_key, deque()):
                self._emit_overflow(oldest_key, entry)
            self._entries.pop(oldest_key, None)

    # ------------------------------------------------------------------ #
    # L1 → L10 溢出承接（P15）：L1 淘汰的事件行交给可注入的深缓冲，
    # 不是"同一份 deque 改名"——L10 有自己的容量/TTL/预算选择。
    # ------------------------------------------------------------------ #

    def set_overflow_sink(self, sink) -> None:
        self._overflow_sink = sink

    def _emit_overflow(self, key: Tuple[str, str], entry: BackgroundEntry) -> None:
        sink = getattr(self, "_overflow_sink", None)
        if sink is None:
            return
        try:
            sink(key[0], key[1], entry)
        except Exception:  # noqa: BLE001 - 溢出承接失败不影响 L1
            pass


class DeepBackground:
    """L10：独立有界深缓冲（进程内存，重启即空）。

    - 承接 L1 溢出的事件行（来源事件 ID/人物/时间保留）；按 `(Agent, 会话)`
      隔离，事件 ID 去重，容量/TTL 双约束；
    - **按任务预算选用**（`render_budget`）：只取预算内的最近条目，不全部
      塞给模型；与当前批次/已载入 L2 去重（`discard` 已消费事件）；
    - 不收 Base64、文件/网页/工具全文——入口只接受 L1 溢出的短事件行；
    - 与 L1 是**两份独立数据**：L1 是"最近几条"，L10 是更深的缓冲历史。
    """

    def __init__(
        self,
        *,
        max_turns: int,
        max_chars: int,
        idle_seconds: float,
        max_conversations: int,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._max_turns = max(int(max_turns), 1)
        self._max_chars = max(int(max_chars), 1)
        self._idle_seconds = max(float(idle_seconds), 60.0)
        self._max_conversations = max(int(max_conversations), 1)
        self._clock = clock
        self._entries: Dict[Tuple[str, str], Deque[BackgroundEntry]] = {}

    def absorb(self, agent_id: str, conversation_id: str, entry: BackgroundEntry) -> bool:
        key = (agent_id, conversation_id)
        queue = self._entries.get(key)
        if queue is None:
            if len(self._entries) >= self._max_conversations:
                self._evict_oldest()
                if len(self._entries) >= self._max_conversations:
                    return False
            queue = deque(maxlen=self._max_turns)
            self._entries[key] = queue
        if any(existing.event_id == entry.event_id for existing in queue):
            return False
        queue.append(entry)
        self._trim(key)
        return True

    def discard(self, agent_id: str, event_ids: Sequence[str]) -> int:
        if not event_ids:
            return 0
        wanted = set(event_ids)
        removed = 0
        for key, queue in list(self._entries.items()):
            if key[0] != agent_id:
                continue
            kept = deque(
                (e for e in queue if e.event_id not in wanted),
                maxlen=self._max_turns,
            )
            removed += len(queue) - len(kept)
            if kept:
                self._entries[key] = kept
            else:
                self._entries.pop(key, None)
        return removed

    def render_budget(
        self,
        agent_id: str,
        conversation_id: str,
        *,
        budget_chars: int,
        exclude_event_ids: Sequence[str] = (),
    ) -> str:
        """按预算选用最近条目（新的优先），返回带来源行的有界块。"""
        queue = self._entries.get((agent_id, conversation_id))
        if not queue:
            return ""
        now = self._clock()
        excluded = set(exclude_event_ids)
        lines: List[str] = []
        total = 0
        for entry in reversed(queue):
            if now - entry.occurred_at > self._idle_seconds:
                break
            if entry.event_id in excluded:
                continue
            name = entry.display_name[:24]
            body = entry.text or entry.attachment_note or "（无文字）"
            occurred = _bj_stamp(entry.occurred_at, "%m-%d %H:%M")
            line = f"- [{occurred}] {name}：{body}"
            if total + len(line) + 1 > max(int(budget_chars), 1):
                break
            lines.append(line)
            total += len(line) + 1
        if not lines:
            return ""
        lines.reverse()
        return "# 较早背景（L10 深缓冲，临时参考）\n" + "\n".join(lines)

    def snapshot(self) -> Dict[str, int]:
        now = self._clock()
        conversations = 0
        entries = 0
        expired = 0
        for key, queue in list(self._entries.items()):
            fresh = deque(
                (e for e in queue if now - e.occurred_at <= self._idle_seconds),
                maxlen=self._max_turns,
            )
            expired += len(queue) - len(fresh)
            if fresh:
                self._entries[key] = fresh
                conversations += 1
                entries += len(fresh)
            else:
                self._entries.pop(key, None)
        return {
            "conversations": conversations,
            "entries": entries,
            "expired": expired,
            "max_turns": self._max_turns,
            "max_chars": self._max_chars,
            "state": "memory",
        }

    def snapshot_entries(
        self, agent_id: str, conversation_id: str, *, limit: int = 20
    ) -> Dict[str, Any]:
        """真实内存条目快照（L10 深缓冲）：当前进程内的条目，重启即空。"""
        queue = self._entries.get((agent_id, conversation_id))
        now = self._clock()
        fresh = [
            e for e in (queue or [])
            if now - e.occurred_at <= self._idle_seconds
        ]
        rows = [
            {
                "event_id": e.event_id,
                "sender_ref": e.sender_ref,
                "display_name": e.display_name,
                "occurred_at": _bj_stamp(e.occurred_at, "%Y-%m-%d %H:%M:%S"),
                "preview": (e.text or e.attachment_note or "")[:80],
            }
            for e in fresh[-max(1, int(limit)):]
        ]
        rows.reverse()
        return {"entries": rows, "count": len(fresh), "storage": "disk_snapshot" if getattr(self,'_persistent',False) else "memory"}

    def _trim(self, key: Tuple[str, str]) -> None:
        queue = self._entries.get(key)
        if queue is None:
            return
        while len(queue) > self._max_turns:
            queue.popleft()
        total = sum(len(e.text) + len(e.attachment_note) + 24 for e in queue)
        while total > self._max_chars and queue:
            queue.popleft()
            total = sum(len(e.text) + len(e.attachment_note) + 24 for e in queue)

    def _evict_oldest(self) -> None:
        if not self._entries:
            return
        oldest_key = min(
            self._entries,
            key=lambda key: self._entries[key][-1].occurred_at if self._entries[key] else 0.0,
        )
        self._entries.pop(oldest_key, None)
