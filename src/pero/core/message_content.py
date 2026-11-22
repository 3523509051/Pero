"""Distinguish platform media labels from actual user questions."""
from __future__ import annotations
import re

_MEDIA_LABEL = re.compile(r"\[(?:图片(?:\s*#[^\]\r\n]{1,300})?(?:（[^）\r\n]{0,100}）)?|表情(?:包)?|动画表情)\]")

def substantive_text(text: str) -> str:
    return _MEDIA_LABEL.sub('', text or '').strip()

def is_image_only(message) -> bool:
    if message.mentions or substantive_text(message.text or ''):
        return False
    return (any(a.kind == 'image' for a in message.attachments)
            or bool(_MEDIA_LABEL.search(message.text or '')))
