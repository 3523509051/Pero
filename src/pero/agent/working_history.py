"""Bound model work history independently of persistent L2 archival thresholds.

v7 起，模型历史的主体是**逐条 L2 交互**（未归档交互 + 最近归档前情，见
`orchestrator._load_l2_history`）。这里只做"按协议消息预算裁剪"：从最早的整组
（用户 + 对应的助手回复）开始丢，绝不把历史改成"最近 N 条流水"再重写前缀。

已经写进历史的消息内容不再改写——裁剪只做删除，保证剩余前缀与上一轮**逐字节
相同**，模型侧的前缀缓存才有机会命中。
"""

from pero.core.contracts import ModelMessage, MessageRole
from pero.agent.legacy_commands import filter_history

#: 旧版聊天流水块的块头标记（迁移期兼容识别用）。
CHAT_WINDOW_MARKER = "[最近 "
#: L2 前情块块头（与 `orchestrator._load_l2_history` 保持一致）。
L2_SUMMARY_MARKER = "[已归档前情"


def _is_chat_block(message) -> bool:
    if getattr(message, "role", None) != MessageRole.USER:
        return False
    content = str(getattr(message, "content", "") or "")
    return content.startswith(CHAT_WINDOW_MARKER) and "群聊流水" in content[:80]


def _is_l2_summary(message) -> bool:
    if getattr(message, "role", None) != MessageRole.USER:
        return False
    return str(getattr(message, "content", "") or "").startswith(L2_SUMMARY_MARKER)


async def trim_working_history(owner, session, agent, cid, memory):
    limits = owner._settings.runtime
    reserve = 1 + limits.max_model_calls + limits.max_tool_calls
    if owner._settings.images.enabled:
        reserve += owner._settings.images.max_per_turn
    if owner._settings.files.enabled:
        reserve += 1
    budget = max(0, limits.max_history_messages - reserve)
    if len(session.history) <= budget or memory is None:
        return False

    history = list(session.history)
    has_l2 = any(
        not _is_l2_summary(message) and getattr(message, "role", None) is not None
        for message in history
    )
    if has_l2 or any(_is_l2_summary(message) for message in history):
        # L2 逐条历史：从最早一条开始删，保持"用户在前、助手在后"的配对，
        # 前情块只在实在放不下时才丢。内容不改写 → 剩余前缀稳定。
        changed = False
        while len(history) > budget:
            drop_at = None
            for index, message in enumerate(history):
                if _is_l2_summary(message):
                    continue
                drop_at = index
                break
            if drop_at is None:
                break
            history.pop(drop_at)
            # 删除后若下一条是孤立助手回复，一并删掉（协议要求配对）。
            if (
                drop_at < len(history)
                and getattr(history[drop_at], "role", None) == MessageRole.ASSISTANT
            ):
                history.pop(drop_at)
            changed = True
        if changed:
            session.history[:] = history
            owner._counters["working_history_refreshes"] = (
                owner._counters.get("working_history_refreshes", 0) + 1
            )
            owner._counters["l2_history_trimmed"] = (
                owner._counters.get("l2_history_trimmed", 0) + 1
            )
            return True

    for index, message in enumerate(history):
        if not _is_chat_block(message):
            continue
        # 旧版块：保留原逻辑（重建更小窗口），只在迁移期数据上会走到。
        window = max(1, int(owner._settings.memory.chat_l2_messages))
        try:
            items = await memory.chat_window(
                agent_id=agent, conversation_id=cid, limit=window
            )
        except Exception:  # noqa: BLE001 - 读失败不阻断本轮
            items = []
        if not items:
            history.pop(index)
        else:
            block, _shown, cut = owner._render_chat_window(items)
            history[index] = ModelMessage(role=MessageRole.USER, content=block)
            if cut:
                owner._counters["chat_window_cut"] += cut
        session.history[:] = history
        owner._counters["working_history_refreshes"] = (
            owner._counters.get("working_history_refreshes", 0) + 1
        )
        if len(history) <= budget:
            return True
        session.history[:] = [item for item in history if not _is_chat_block(item)]
        owner._counters["chat_window_dropped"] = (
            owner._counters.get("chat_window_dropped", 0) + 1
        )
        return True

    # 迁移期兼容：尚未产生 chat_event 的会话沿用旧轮次回放。
    loaded = await memory.load_history(agent_id=agent, conversation_id=cid)
    loaded = filter_history(owner, loaded, agent, cid)
    groups = []
    used = 0
    for turn in reversed(loaded.turns):
        group = [ModelMessage(role=MessageRole.USER, content=turn.render_user_content())]
        if turn.spoken:
            group.append(
                ModelMessage(role=MessageRole.ASSISTANT, content=turn.assistant_text)
            )
        if used + len(group) > budget:
            break
        groups.append(group)
        used += len(group)
    rebuilt = [message for group in reversed(groups) for message in group]
    # Canonical records and original delivery facts are untouched. Only publish
    # complete reloaded turns after the read has succeeded.
    session.history[:] = rebuilt
    owner._counters["working_history_refreshes"] = (
        owner._counters.get("working_history_refreshes", 0) + 1
    )
    return True
