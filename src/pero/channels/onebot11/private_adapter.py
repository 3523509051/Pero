"""OneBot v11 private message event adapter for Pero."""

from typing import Dict, Any, Optional
import logging

log = logging.getLogger("pero.channels.onebot11.private")


def parse_private_message(event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Convert OneBot v11 private message event to Pero format.
    
    Args:
        event: Raw OneBot v11 event
        
    Returns:
        Pero private message record or None if should skip
    """
    
    if event.get("message_type") != "private":
        return None
    
    # Extract message content
    segments = event.get("message", [])
    text_parts = []
    
    for seg in segments:
        if seg.get("type") == "text":
            text_parts.append(seg.get("data", {}).get("text", ""))
    
    text = "".join(text_parts).strip()
    if not text:
        return None
    
    # Extract Pero extensions
    pero_ext = event.get("pero_ext", {})
    
    return {
        # Basic fields
        "message_type": "private",
        "user_id": event.get("user_id"),
        "message_id": event.get("message_id"),
        "self_id": event.get("self_id"),
        "text": text,
        "segments": segments,
        "timestamp": event.get("time"),
        
        # Sender info
        "sender_nickname": event.get("sender", {}).get("nickname", ""),
        
        # Pero extensions (from bridge)
        "sender_wxid": pero_ext.get("sender_wxid"),
        "local_id": pero_ext.get("local_id"),
        "session": pero_ext.get("session"),
        "is_authorized": pero_ext.get("is_authorized", False),
        
        # Raw event for audit
        "raw_event": event,
    }


def extract_user_ref(user_id: int) -> str:
    """Extract user reference from OneBot user_id.
    
    User ID in OneBot is computed from WeChat ID,
    we use it as the user_ref (along with sender_wxid for binding).
    """
    return f"ob11_{user_id}"


def format_private_response(
    reply_text: str,
    segments: Optional[list] = None
) -> list:
    """Format reply into OneBot message segments.
    
    Args:
        reply_text: Text response from model
        segments: Optional image/media segments to include
        
    Returns:
        OneBot message segments list
    """
    
    result = []
    
    # Add text
    if reply_text:
        result.append({
            "type": "text",
            "data": {"text": reply_text}
        })
    
    # Add media if provided
    if segments:
        result.extend(segments)
    
    return result


def build_private_action(
    user_id: int,
    reply_text: str,
    action_id: str = ""
) -> Dict[str, Any]:
    """Build OneBot action for sending private message.
    
    Args:
        user_id: OneBot user ID
        reply_text: Message text
        action_id: Optional business action ID for tracking
        
    Returns:
        OneBot action dict ready for send
    """
    
    return {
        "action": "send_private_msg",
        "params": {
            "user_id": user_id,
            "message": format_private_response(reply_text),
        },
        "echo": action_id or f"private_{user_id}_{int(time.time())}",
    }


import time


__all__ = [
    "parse_private_message",
    "extract_user_ref",
    "format_private_response",
    "build_private_action",
]
