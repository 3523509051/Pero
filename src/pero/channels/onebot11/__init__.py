"""OneBot v11 适配：反向 WebSocket 接收、事件归一化、动作编码与回执。"""

from pero.channels.onebot11.mapping import (
    CHANNEL_ID,
    DEDUPE_SCOPE,
    ChannelActionError,
    assert_action_scope,
    build_action_payload,
    encode_action,
    normalize_payload,
    parse_conversation_id,
    receipt_from_echo,
    valid_account_id,
)
from pero.channels.onebot11.server import OneBotChannelServer

__all__ = [
    "CHANNEL_ID",
    "DEDUPE_SCOPE",
    "ChannelActionError",
    "OneBotChannelServer",
    "assert_action_scope",
    "build_action_payload",
    "encode_action",
    "normalize_payload",
    "parse_conversation_id",
    "receipt_from_echo",
    "valid_account_id",
]
