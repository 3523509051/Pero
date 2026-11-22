"""Explicit wake and a reply-based sliding participation window.

Window admission only supplies candidates. The orchestrator confirms JOIN before
image preparation; only JOIN and confirmed replies refresh the idle deadline.
Image-only messages and PASS never refresh the window.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field, replace
from typing import Callable, Dict, List, Mapping, Optional, Sequence, Tuple

from pero.core.contracts import InboundMessage, ModelMessage
from pero.core.settings import OrchestrationSettings
from pero.core.message_content import is_image_only

POLICY_ID = "policy.window_text"

#: 窗口状态键：`(agent_id, conversation_id)`。
#: 同一策略实例被多个 Agent 绑定时，窗口状态按 Agent 隔离，不混作默认实例。
WindowKey = Tuple[str, str]


@dataclass
class WindowState:
    conversation_id: str
    activated_at: float
    expire_at: float
    accepted_count: int = 0
    #: 绑定该策略的 Agent；空串表示调用方未指定 Agent（兼容无 Agent 语义的调用）
    agent_id: str = ""


@dataclass(frozen=True)
class WakeDecision:
    """一次唤醒判定结果；`reason` 同时用于状态计数，不含正文。"""

    accepted: bool
    reason: str
    conversation_id: str
    window_remaining: float = 0.0
    #: 是否应对窗口续期（只有接受的消息才允许）
    should_touch: bool = False


class WindowConversationPolicy:
    """按 `(Agent, 会话)` 隔离的纯文本唤醒/窗口策略。确定性实现，不调用模型。

    同一实例被多个 Agent 绑定时，窗口键包含 `agent_id`：
    状态共享仅限于"绑定同一个组件 ID"这一显式配置，不会退化成全局默认实例。
    """

    policy_id = POLICY_ID

    def __init__(
        self,
        settings: OrchestrationSettings,
        *,
        max_text_chars: int,
        allow_images: bool = False,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._settings = settings
        self._max_text_chars = int(max_text_chars)
        self._allow_images = allow_images
        self._clock = clock
        self._windows: Dict[WindowKey, WindowState] = {}

    # ------------------------------------------------------------------ #
    # 判定
    # ------------------------------------------------------------------ #

    def evaluate(self, message: InboundMessage, *, agent_id: str = "") -> WakeDecision:
        """**无副作用**评估：只回答"会不会接受"，不开窗、不续期。

        用于入队前的资格预检：被拒绝的消息不占用队列/worker 等长期资源，
        已接受的消息在真正消费时再用 `decide()` 做一次有状态判定。
        """

        cid = message.conversation_id
        text = (message.text or "").strip()
        now = self._clock()

        if message.attachments and (not self._allow_images or any(a.kind!="image" for a in message.attachments)):
            return WakeDecision(False, "attachments_unsupported", cid)
        if is_image_only(message):
            return WakeDecision(False, "image_only_cached", cid)
        if not text and not message.mentions:
            return WakeDecision(False, "empty_text", cid)
        if cid not in self._settings.allowed_conversations:
            return WakeDecision(False, "conversation_not_allowed", cid)
        if message.conversation_type == "private" and not self._settings.allow_private:
            return WakeDecision(False, "private_not_allowed", cid)
        if len(text) > self._max_text_chars:
            return WakeDecision(False, "input_too_long", cid)

        overrides=getattr(self,'_wake_resolver',lambda cid:{})(cid)
        wake_word = self._match_wake_word(text,overrides)
        trusted_at = bool(
            overrides.get('at_is_trusted_mention',self._settings.at_is_trusted_mention) and message.mentions
        )

        if wake_word or trusted_at:
            reason = "wake_word" if wake_word else "trusted_at"
            if (
                not self._active(agent_id, cid, now)
                and self._active_count(agent_id, now) >= self._settings.max_active_conversations
            ):
                return WakeDecision(False, "active_limit", cid)
            return WakeDecision(
                True,
                reason,
                cid,
                window_remaining=round(self._settings.window_after_wake_seconds, 3),
            )

        if self._active(agent_id, cid, now):
            state = self._windows[(agent_id, cid)]
            projected = state.expire_at
            return WakeDecision(
                True,
                "window",
                cid,
                window_remaining=round(max(0.0, projected - now), 3),
                should_touch=False,
            )

        if message.mentions:
            # 桥的 at 只是昵称文本匹配：不当作"提到了机器人"的证据。
            return WakeDecision(False, "untrusted_at_only", cid)
        return WakeDecision(False, "no_wake", cid)

    def decide(self, message: InboundMessage, *, agent_id: str = "") -> WakeDecision:
        """Explicit wakes open provisionally; window candidates do not renew time."""

        decision = self.evaluate(message, agent_id=agent_id)
        if not decision.accepted:
            return decision
        now = self._clock()
        if decision.reason == "window":
            # Admission is not JOIN: PASS and unrelated chatter must not extend time.
            state = self._windows[(agent_id, decision.conversation_id)]
            state.accepted_count += 1
        else:
            state = self._activate(agent_id, decision.conversation_id, now)
        return replace(
            decision, window_remaining=round(max(0.0, state.expire_at - now), 3)
        )

    async def should_reply(
        self, message: InboundMessage, context: Mapping[str, object]
    ) -> bool:
        agent_id = str(context.get("agent_id") or "")
        return self.decide(message, agent_id=agent_id).accepted

    async def compose_context(
        self, message: InboundMessage, context: Mapping[str, object]
    ) -> Sequence[ModelMessage]:
        # 本阶段不做上下文组装（L1 注入属记忆阶段）。
        return ()

    # ------------------------------------------------------------------ #
    # 窗口状态
    # ------------------------------------------------------------------ #

    def _match_wake_word(self, text: str, overrides=None) -> Optional[str]:
        overrides=overrides or {}
        lowered = text.lower()
        for word in overrides.get('wake_words',self._settings.wake_words):
            candidate = word.lower()
            if not candidate:
                continue
            if overrides.get('wake_match',self._settings.wake_match) == "prefix":
                if lowered.startswith(candidate):
                    return word
            elif candidate in lowered:
                return word
        return None

    def _active(self, agent_id: str, cid: str, now: float) -> bool:
        key = (agent_id, cid)
        state = self._windows.get(key)
        if state is None:
            return False
        if now >= state.expire_at:
            self._windows.pop(key, None)
            return False
        if now - state.activated_at > self._settings.window_max_seconds:
            self._windows.pop(key, None)
            return False
        return True

    def _active_count(self, agent_id: str, now: float) -> int:
        return sum(
            1
            for key in list(self._windows)
            if key[0] == agent_id and self._active(key[0], key[1], now)
        )

    def _activate(self, agent_id: str, cid: str, now: float) -> WindowState:
        state = WindowState(
            conversation_id=cid,
            activated_at=now,
            expire_at=now + self._settings.window_after_wake_seconds,
            agent_id=agent_id,
        )
        self._windows[(agent_id, cid)] = state
        return state

    def confirm_join(self, conversation_id: str, *, agent_id: str = "") -> None:
        """Accepted JOIN / confirmed reply starts a fresh idle window.

        Reset the anchor as well: repeated JOINs are not cut off by the first
        wake's historical maximum lifetime. Mere admission never calls this.
        """
        now = self._clock()
        prior = self._windows.get((agent_id, conversation_id))
        state = self._activate(agent_id, conversation_id, now)
        state.expire_at = now + self._settings.window_idle_seconds
        state.accepted_count = prior.accepted_count if prior else 0

    def is_active(self, conversation_id: str, *, agent_id: str = "") -> bool:
        return self._active(agent_id, conversation_id, self._clock())

    def remaining(self, conversation_id: str, *, agent_id: str = "") -> float:
        state = self._windows.get((agent_id, conversation_id))
        if state is None:
            return 0.0
        return round(max(0.0, state.expire_at - self._clock()), 3)

    def snapshot(self, *, agent_id: Optional[str] = None) -> List[dict]:
        """窗口快照（不含正文）。``agent_id=None`` 表示全部绑定的 Agent。"""

        now = self._clock()
        items = []
        for (scope, cid), state in sorted(self._windows.items()):
            if agent_id is not None and scope != agent_id:
                continue
            if not self._active(scope, cid, now):
                continue
            items.append(
                {
                    "agent_id": scope,
                    "conversation_id": cid,
                    "remaining_seconds": round(max(0.0, state.expire_at - now), 1),
                    "accepted_count": state.accepted_count,
                }
            )
        return items

    @property
    def settings(self) -> OrchestrationSettings:
        return self._settings


__all__ = ["POLICY_ID", "WakeDecision", "WindowConversationPolicy", "WindowState"]
