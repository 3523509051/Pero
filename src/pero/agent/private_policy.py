"""Private chat access policy and decision engine."""

from enum import Enum
from typing import Dict, Optional, Any
from dataclasses import dataclass
from pathlib import Path
import time

from pero.access import PrivateChatAuthenticator


class PrivateChatDecision(Enum):
    """Decisions for private chat processing."""
    ALLOW = "allow"           # 允许处理
    DENY = "deny"             # 拒绝处理
    UNKNOWN_USER = "unknown"  # 用户未授权
    BLOCKED = "blocked"       # 用户被阻止
    RATE_LIMITED = "limited"  # 超过速率限制


@dataclass
class PrivateChatPolicyContext:
    """Context for policy evaluation."""
    channel_id: str
    account_id: str
    user_ref: str
    user_wxid: Optional[str]
    is_owner: bool
    conversation_id: str
    message_count_today: int
    last_message_time: Optional[float]


class PrivateChatPolicy:
    """Policy evaluator for private chat access."""
    
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.rate_limit_per_hour = 30
        self.owner_rate_limit = 1000
        self.authenticator = PrivateChatAuthenticator(db_path)
    
    def evaluate(self, ctx: PrivateChatPolicyContext) -> PrivateChatDecision:
        """Evaluate if private chat should be processed.
        
        Synchronous version for immediate checks.
        """
        
        # Owner always allowed
        if ctx.is_owner:
            return PrivateChatDecision.ALLOW
        
        # Check if user is authorized
        grant = self.authenticator.get_grant_by_user_ref(
            ctx.channel_id, ctx.account_id, ctx.user_ref
        )
        
        if not grant or grant["status"] != "active":
            return PrivateChatDecision.UNKNOWN_USER
        
        # Check rate limit
        if ctx.message_count_today >= self.rate_limit_per_hour:
            return PrivateChatDecision.RATE_LIMITED
        
        return PrivateChatDecision.ALLOW


class PrivateChatDecisionEngine:
    """Decides how to handle private chat messages."""
    
    def __init__(self):
        self.owner_timeout_seconds = 300  # 5分钟
        self.user_timeout_seconds = 60    # 1分钟
    
    def should_process_message(
        self, 
        decision: PrivateChatDecision, 
        is_owner: bool
    ) -> bool:
        """Determine if message should be processed."""
        
        if decision == PrivateChatDecision.ALLOW:
            return True
        
        return False
    
    def get_error_reply(
        self, 
        decision: PrivateChatDecision
    ) -> Optional[str]:
        """Get user-friendly error message."""
        
        if decision == PrivateChatDecision.UNKNOWN_USER:
            return "您暂未开通对话权限。如有开通码，请直接发送。"
        elif decision == PrivateChatDecision.BLOCKED:
            return "您的访问已被禁用。"
        elif decision == PrivateChatDecision.RATE_LIMITED:
            return "请求过于频繁，请稍后再试。"
        
        return None
    
    def process_response(
        self, 
        assistant_text: Optional[str],
        turn_status: str,
        max_length: int = 4000
    ) -> Dict[str, Any]:
        """Process model response for private chat delivery.
        
        Returns:
            {
                'success': bool,
                'reply': str (truncated if needed),
                'truncated': bool,
                'metadata': {...}
            }
        """
        
        if not assistant_text or turn_status != "completed":
            return {
                'success': False,
                'reply': '处理出错，请稍后重试',
                'truncated': False,
            }
        
        # Truncate if needed
        if len(assistant_text) > max_length:
            reply = assistant_text[:max_length - 10] + "...(已截断)"
            truncated = True
        else:
            reply = assistant_text
            truncated = False
        
        return {
            'success': True,
            'reply': reply,
            'truncated': truncated,
            'metadata': {
                'original_length': len(assistant_text),
                'delivered_length': len(reply),
            }
        }


__all__ = [
    "PrivateChatDecision",
    "PrivateChatPolicyContext",
    "PrivateChatPolicy",
    "PrivateChatDecisionEngine",
]
