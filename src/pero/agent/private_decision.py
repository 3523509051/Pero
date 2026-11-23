"""私聊专用判断机：5秒合并规则与JOIN/PASS决策。"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional

from pero.core.contracts import InboundMessage

log = logging.getLogger("pero.agent.private_decision")

#: 私聊判断Prompt：明确这是一对一对话场景
PRIVATE_DECISION_PROMPT = """你是一对一私聊对话的参与判断助手。

当前场景：用户在微信私聊中发来消息，你需要判断是否应该回复（JOIN）。

判断标准：
- 问题、追问、纠正、请求建议 → JOIN
- 分享感受、日常闲聊、持续话题 → JOIN
- 简短回应（"好的"、"谢谢"）如果是对话中的自然反馈 → JOIN
- 连续纯表情/单图无文字说明 → PASS
- 明确表示"不用回"、"自己说说" → PASS

输出JSON: {"decision": "JOIN"} 或 {"decision": "PASS", "reason": "..."}
严禁输出思考过程，直接给出结果。

最近消息：
{messages}

判断："""


@dataclass
class MessageBatch:
    """5秒窗口内合并的消息批次。"""

    conversation_id: str
    messages: List[InboundMessage] = field(default_factory=list)
    first_at: float = 0.0
    last_at: float = 0.0


class PrivateChatDecisionEngine:
    """私聊决策引擎：管理批次合并与JOIN/PASS判断。
    
    规则：
    - 授权私聊无需唤醒词
    - 同联系人5秒静默合并，最长30秒
    - 批次结束后调用模型判断
    - JOIN进入正常回复，PASS只记录
    """

    def __init__(
        self,
        silence_seconds: float = 5.0,
        max_batch_seconds: float = 30.0,
        clock=time.monotonic,
    ) -> None:
        self.silence_seconds = silence_seconds
        self.max_batch_seconds = max_batch_seconds
        self.clock = clock
        self._batches: Dict[str, MessageBatch] = {}
        self._finalize_tasks: Dict[str, asyncio.Task] = {}

    def add_message(self, message: InboundMessage) -> None:
        """加入批次，刷新5秒计时器。"""
        cid = message.conversation_id
        now = self.clock()

        if cid not in self._batches:
            self._batches[cid] = MessageBatch(
                conversation_id=cid, first_at=now, last_at=now
            )

        batch = self._batches[cid]
        batch.messages.append(message)
        batch.last_at = now

        log.debug(
            f"私聊消息加入批次: {cid}, 当前批次大小={len(batch.messages)}, "
            f"持续时间={now - batch.first_at:.1f}s"
        )

    def should_finalize(self, conversation_id: str) -> Optional[MessageBatch]:
        """检查批次是否应该结束。
        
        返回:
            MessageBatch if should finalize, None otherwise
        """
        batch = self._batches.get(conversation_id)
        if not batch:
            return None

        now = self.clock()
        silence = now - batch.last_at
        duration = now - batch.first_at

        # 5秒静默 或 30秒硬上限
        if silence >= self.silence_seconds or duration >= self.max_batch_seconds:
            log.info(
                f"批次达到结束条件: {conversation_id}, "
                f"静默={silence:.1f}s, 持续={duration:.1f}s, 消息数={len(batch.messages)}"
            )
            return self._batches.pop(conversation_id)

        return None

    async def decide_for_batch(
        self, batch: MessageBatch, model_client
    ) -> tuple[str, str]:
        """调用模型判断批次是否应该回复。
        
        Returns:
            (decision, reason) where decision is 'JOIN' or 'PASS'
        """
        # 格式化消息
        messages_text = []
        for msg in batch.messages[-10:]:  # 最多看最近10条
            from pero.core.timebase import now_iso as _now_iso

            time_str = msg.received_at or _now_iso()
            text = (msg.text or "").strip()
            if not text and msg.attachments:
                text = f"[{len(msg.attachments)}个附件]"
            messages_text.append(f"{time_str}: {text}")

        prompt = PRIVATE_DECISION_PROMPT.format(messages="\n".join(messages_text))

        try:
            # 调用模型（禁用思考，快速决策）
            response = await model_client.complete(
                messages=[{"role": "user", "content": prompt}],
                temperature=0.0,
                max_tokens=50,
                thinking_budget=0,
            )

            content = response.get("content", "").strip()
            result = json.loads(content)
            decision = result.get("decision", "").upper()

            if decision not in ("JOIN", "PASS"):
                log.warning(f"模型返回无效决策: {decision}, 默认JOIN")
                return "JOIN", "invalid_response"

            reason = result.get("reason", "")
            return decision, reason

        except json.JSONDecodeError as e:
            log.error(f"判断机响应解析失败: {e}, 默认JOIN")
            return "JOIN", "parse_error"
        except Exception as e:
            log.error(f"判断机调用失败: {e}, 默认JOIN")
            return "JOIN", f"error: {str(e)}"

    def get_pending_count(self) -> int:
        """返回当前待处理批次数量。"""
        return len(self._batches)

    def clear_batch(self, conversation_id: str) -> None:
        """清理指定会话的批次（例如授权被撤销时）。"""
        if conversation_id in self._batches:
            del self._batches[conversation_id]
            log.info(f"清理私聊批次: {conversation_id}")


def format_messages_for_display(messages: List[InboundMessage]) -> str:
    """格式化消息列表为可读文本（用于日志/调试）。"""
    lines = []
    for msg in messages:
        time_str = msg.received_at or "未知时间"
        sender = msg.display_name or msg.sender_ref
        text = (msg.text or "").strip() or "[无文字]"
        lines.append(f"[{time_str}] {sender}: {text}")
    return "\n".join(lines)


__all__ = [
    "PrivateChatDecisionEngine",
    "MessageBatch",
    "PRIVATE_DECISION_PROMPT",
    "format_messages_for_display",
]
