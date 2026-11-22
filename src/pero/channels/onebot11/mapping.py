"""OneBot v11 事件 → pero 通用契约；以及 pero 动作 → OneBot 动作。

字段依据本地桥 `D:\\webot\\bridge`（只读参考）：
- `onebot11.py::send_group_message`：标准 OneBot v11 字段 + P13 起可选顶层
  `pero_ext`（版本化身份/引用/文件扩展；标准字段一字不改，缺扩展时行为不变）；
- `main.py::_fake_id`：group_id / user_id 是 `zlib.crc32(wxid) % 10**9` 的稳定假 ID，
  不是微信原始 wxid；
- `main.py::build_segments`：at 段来自**正文里的 @昵称**匹配，qq 固定为配置的 self_id；
- `main.py::build_media_segments` + `image_segment`：图片是 `base64://...`；
  P13 起文件入库成功会带 `pero_ext.file` 结构化事实，正文仍是文本占位。
详见 `docs/channel-contract.md` 与 `docs/bridge-context-p13.md`。
"""

from __future__ import annotations

import hashlib
import json
import re
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from pero.core.contracts import (
    AttachmentRef,
    ChannelEvent,
    ChannelEventKind,
    InboundMessage,
    OutboundAction,
    SendReceipt,
    SendStatus,
)

CHANNEL_ID = "onebot11"

#: 桥的 message_id 是桥进程序列号（时间种子 + 自增），重启后不沿用，
#: 因此去重只在单个桥进程生命周期内可靠。
DEDUPE_SCOPE = "bridge_process"

_GROUP = "group"
_PRIVATE = "private"

#: 桥侧发送动作可能带出的投递状态（见 bridge/main.py + send_result.py）。
_DELIVERY_UNKNOWN = "unknown"
_DELIVERY_FAILED = "failed"


class ChannelActionError(ValueError):
    """动作无法编码成桥可执行的形式（目标不合法、内容为空、动作不支持）。"""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


#: 账号标识允许的字符集与长度：桥的 self_id 是配置的数字 ID。
#: 这里同时挡住 dict/list/bool/float 被 `str()` 转成"看起来像 ID"的垃圾值。
_ACCOUNT_CHARS = set(
    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-"
)


def valid_account_id(value: Any) -> Optional[str]:
    """校验并规范化账号标识；不合法返回 None（调用方必须拒绝，不能猜）。"""

    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, int):
        return str(value) if value > 0 else None
    if isinstance(value, str):
        text = value.strip()
        if not text or len(text) > 64:
            return None
        if any(ch not in _ACCOUNT_CHARS for ch in text):
            return None
        return text
    return None


def _iso_from_unix(value: Any) -> Optional[str]:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(float(value), timezone.utc).isoformat()
        except (OverflowError, OSError, ValueError):
            return None
    if isinstance(value, str) and value.isdigit():
        return _iso_from_unix(int(value))
    return None


def _reject(reason: str, code: str, account_id: str = "") -> ChannelEvent:
    return ChannelEvent(
        kind=ChannelEventKind.UNKNOWN,
        channel_id=CHANNEL_ID,
        account_id=account_id,
        reject_code=code,
        reject_reason=reason,
    )


def classify_payload(payload: Any) -> ChannelEventKind:
    if not isinstance(payload, Mapping):
        return ChannelEventKind.UNKNOWN
    if "echo" in payload and "action" not in payload:
        return ChannelEventKind.ACTION_ECHO
    post_type = payload.get("post_type")
    if post_type == "message":
        return ChannelEventKind.MESSAGE
    if post_type == "meta_event":
        return ChannelEventKind.META
    return ChannelEventKind.UNKNOWN


def normalize_payload(payload: Any) -> ChannelEvent:
    """把一帧 OneBot 事件归一化为通用事件信封；不保留正文原文。"""

    if not isinstance(payload, Mapping):
        return _reject("事件不是 JSON 对象", "invalid_payload")

    kind = classify_payload(payload)
    account_id = valid_account_id(payload.get("self_id"))

    if kind is ChannelEventKind.ACTION_ECHO:
        # echo 通常不带 self_id：归属由"收到它的连接"决定，这里不要求伪造字段。
        return ChannelEvent(
            kind=kind,
            channel_id=CHANNEL_ID,
            account_id=account_id or "",
            event_id=str(payload.get("echo") or "") or None,
        )

    if account_id is None:
        # 缺账号或账号类型不合法：不生成空账号会话，也不能按昵称/连接去猜。
        return _reject("事件缺少可用的 self_id", "missing_account_id")

    if kind is ChannelEventKind.META:
        meta_type = payload.get("meta_event_type")
        return ChannelEvent(
            kind=kind,
            channel_id=CHANNEL_ID,
            account_id=account_id,
            event_time=_iso_from_unix(payload.get("time")),
            meta_event_type=str(meta_type) if isinstance(meta_type, str) else None,
        )

    if kind is ChannelEventKind.MESSAGE:
        return _normalize_message(payload, account_id=account_id)

    post_type = payload.get("post_type")
    if isinstance(post_type, str):
        return _reject(f"未支持的 post_type：{post_type}", "unsupported_post_type", account_id)
    return _reject("事件缺少可识别的 post_type", "unknown_event_shape", account_id)


def _parse_pero_ext(payload: Mapping[str, Any]) -> Dict[str, Any]:
    """解析桥版本化扩展 `pero_ext`（P13，v=1）。

    信任边界：本帧必须已通过账号归属校验（连接注册 + self_id 一致），
    扩展**不是**放宽账号隔离的入口；字段全部做形态与长度校验，
    不合法的一律丢弃该字段（不影响标准字段处理）。
    """

    ext = payload.get("pero_ext")
    if not isinstance(ext, Mapping) or ext.get("v") != 1:
        return {}

    def _text(value: Any, limit: int) -> str:
        if not isinstance(value, str):
            return ""
        cleaned = value.strip()
        return cleaned[:limit] if cleaned else ""

    parsed: Dict[str, Any] = {}
    group_wxid = _text(ext.get("group_wxid"), 64)
    if group_wxid:
        parsed["group_wxid"] = group_wxid
    sender_wxid = _text(ext.get("sender_wxid"), 64)
    if sender_wxid:
        # 无前缀消息桥侧就是空：省略即"unknown"，绝不拿昵称/桥 ID 反推 wxid。
        parsed["sender_wxid"] = sender_wxid
    sender_name = _text(ext.get("sender_name"), 60)
    if sender_name and sender_name.lower() != "unknown":
        parsed["sender_name"] = sender_name

    quote = ext.get("quote")
    if isinstance(quote, Mapping) and quote.get("v") == 1:
        parsed["quote"] = {
            "source": _text(quote.get("source"), 40) or "refermsg_xml",
            "author_display": _text(quote.get("author_display"), 60),
            "author_wxid": _text(quote.get("author_wxid"), 64),
            "image_md5": _text(quote.get("image_md5"), 32),
            "refer_time": _text(quote.get("refer_time"), 20),
            "refer_text": _text(quote.get("refer_text"), 200),
        }
    file_offer = ext.get("file")
    if isinstance(file_offer, Mapping) and file_offer.get("v") == 1:
        file_id = _text(file_offer.get("file_id"), 40)
        size = file_offer.get("size")
        if file_id and re.fullmatch(r"f_\d{4,8}", file_id) and isinstance(size, int):
            parsed["file_offer"] = {
                "file_id": file_id,
                "name": _text(file_offer.get("name"), 200),
                "size": size,
                "sha256": _text(file_offer.get("sha256"), 64),
                "group_wxid": _text(file_offer.get("group_wxid"), 64),
                "sender_name": _text(file_offer.get("sender_name"), 60),
                # P13-R02：桥入库时记录的消息 local_id（可信 DB 元数据），
                # 是"这一次投递"的明确来源限定；缺省留空，不猜。
                "source_local_id": _text(file_offer.get("source_local_id"), 64),
                "source": _text(file_offer.get("source"), 40) or "bridge_file_library",
            }
    session = _text(ext.get("session"), 40)
    local_id = _text(ext.get("local_id"), 64)
    if session and local_id:
        # 桥消息 ID 是进程自增号：带会话/本地号限定说明来源范围，
        # 仍然**不**声称跨重启去重（dedupe_scope 维持 bridge_process 语义家族）。
        parsed["source_scope"] = f"bridge_session:{session[:24]}"
    return parsed


def _normalize_message(payload: Mapping[str, Any], *, account_id: str) -> ChannelEvent:
    message_type = payload.get("message_type")
    if message_type == _GROUP:
        conversation_type = _GROUP
        target_raw = payload.get("group_id")
    elif message_type == _PRIVATE:
        conversation_type = _PRIVATE
        target_raw = payload.get("user_id")
    else:
        return _reject("消息事件缺少 group/private 类型", "unsupported_message_type", account_id)

    target_ref = str(target_raw).strip() if target_raw is not None else ""
    if not target_ref:
        return _reject("消息事件缺少目标 ID", "missing_target_id", account_id)

    message_id = payload.get("message_id")
    if message_id is None or str(message_id).strip() == "":
        # 没有稳定来源 ID 时明确拒绝，不用随机 ID 冒充可靠来源。
        return _reject("消息事件缺少 message_id，无法去重", "missing_event_id", account_id)

    sender = payload.get("sender")
    sender = sender if isinstance(sender, Mapping) else {}
    sender_ref = sender.get("user_id", payload.get("user_id"))
    if sender_ref is None or str(sender_ref).strip() == "":
        return _reject("消息事件缺少发送者 ID", "missing_sender_id", account_id)
    sender_ref = str(sender_ref)

    display_name = sender.get("nickname")
    display_name = str(display_name).strip() if isinstance(display_name, str) else None
    card = sender.get("card")
    group_display_name = str(card).strip() if isinstance(card, str) and card.strip() else None

    text, mentions, attachments = _parse_message_body(payload.get("message"), account_id=account_id)
    if not text.strip() and not attachments and not mentions:
        return _reject("消息没有任何可用内容", "empty_message", account_id)

    event_id = f"{CHANNEL_ID}:{account_id}:{conversation_type}:{target_ref}:{message_id}"
    conversation_id = f"{CHANNEL_ID}:{account_id}:{conversation_type}:{target_ref}"

    # P13：版本化桥扩展（可信连接的元数据）；缺扩展时行为与升级前完全一致。
    ext = _parse_pero_ext(payload)
    raw_target_ref = ext.get("group_wxid") or None
    sender_raw_ref = ext.get("sender_wxid") or None
    if not display_name and ext.get("sender_name"):
        display_name = str(ext["sender_name"])
    dedupe_scope = DEDUPE_SCOPE
    if ext.get("source_scope"):
        # 带会话限定的来源说明（不声称跨重启去重）。
        dedupe_scope = str(ext["source_scope"])

    message = InboundMessage(
        event_id=event_id,
        channel_id=CHANNEL_ID,
        account_id=account_id,
        conversation_id=conversation_id,
        sender_ref=sender_ref,
        text=text,
        display_name=display_name,
        mentions=mentions,
        attachments=attachments,
        quoted_event_id=_quoted_event(payload.get("message"), account_id, conversation_type, target_ref),
        received_at=_iso_from_unix(payload.get("time")),
        conversation_type=conversation_type,
        target_ref=target_ref,
        raw_target_ref=raw_target_ref,
        sender_raw_ref=sender_raw_ref,
        group_display_name=group_display_name,
        dedupe_scope=dedupe_scope,
        quote_meta=ext.get("quote"),
        file_offer=ext.get("file_offer"),
    )
    return ChannelEvent(
        kind=ChannelEventKind.MESSAGE,
        channel_id=CHANNEL_ID,
        account_id=account_id,
        event_id=event_id,
        event_time=message.received_at,
        message=message,
    )


def _quoted_event(body, account_id, conversation_type, target_ref):
    if isinstance(body,list):
        for part in body:
            if isinstance(part,dict) and part.get("type")=="reply" and isinstance(part.get("data"),dict):
                value=part["data"].get("id")
                if not isinstance(value,bool) and isinstance(value,(str,int)) and str(value).strip() and len(str(value))<=128:
                    return f"{CHANNEL_ID}:{account_id}:{conversation_type}:{target_ref}:{str(value).strip()}"
    return None


def _parse_message_body(
    body: Any, *, account_id: str
) -> Tuple[str, Tuple[str, ...], Tuple[AttachmentRef, ...]]:
    """解析 messages 段；字符串形式只当纯文本，不反解 CQ 码猜 at。"""

    if isinstance(body, str):
        return body, (), ()

    if not isinstance(body, Sequence) or isinstance(body, (bytes, bytearray)):
        return "", (), ()

    texts: List[str] = []
    mentions: List[str] = []
    attachments: List[AttachmentRef] = []

    for segment in body:
        if not isinstance(segment, Mapping):
            continue
        stype = segment.get("type")
        data = segment.get("data")
        data = data if isinstance(data, Mapping) else {}

        if stype == "text":
            value = data.get("text")
            if isinstance(value, str):
                texts.append(value)
            continue
        if stype == "at":
            qq = data.get("qq")
            if qq is not None and str(qq).strip():
                mentions.append(str(qq))
            continue
        if stype == "reply":
            continue
        if stype == "image":
            ref = _image_attachment(data)
            if ref is not None:
                attachments.append(ref)
            continue
        if stype == "file":
            attachments.append(_file_attachment(data))
            continue
        if isinstance(stype, str) and stype:
            # 其它段（表情/语音/视频/合并转发等）只登记类型与句柄标识，
            # 不猜内容、不读二进制。
            digest = _digest(json.dumps(data, ensure_ascii=False, sort_keys=True))
            attachments.append(
                AttachmentRef(handle=f"{CHANNEL_ID}:{stype}:{digest}", kind=stype)
            )
        continue

    return "".join(texts), tuple(mentions), tuple(attachments)


def _digest(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8", errors="replace")).hexdigest()[:16]


def _image_attachment(data: Mapping[str, Any]) -> Optional[AttachmentRef]:
    """图片段只留稳定句柄。

    桥把图片以 `base64://...` 内联上报；本阶段**不解码、不保存正文**，
    仅用其摘要作为同一个图片的稳定句柄，`size`/`sha256` 保持未知，
    避免把内联内容当成文件读取授权或塞进上下文。
    """

    file_field = data.get("file")
    if not isinstance(file_field, str) or not file_field:
        url = data.get("url")
        file_field = url if isinstance(url, str) else ""
    if not file_field:
        return None
    return AttachmentRef(
        handle=f"{CHANNEL_ID}:image:{_digest(file_field)}",
        kind="image",
        name=None,
        size=None,
        sha256=None,
    )


def _file_attachment(data: Mapping[str, Any]) -> AttachmentRef:
    name = data.get("name") or data.get("file")
    size = data.get("size")
    return AttachmentRef(
        handle=f"{CHANNEL_ID}:file:{_digest(json.dumps(data, ensure_ascii=False, sort_keys=True))}",
        kind="file",
        name=str(name) if isinstance(name, str) and name else None,
        size=int(size) if isinstance(size, int) and not isinstance(size, bool) else None,
        sha256=None,
    )


# --------------------------------------------------------------------------- #
# 出站动作编码
# --------------------------------------------------------------------------- #


def build_action_payload(
    *,
    action_id: str,
    conversation_type: str,
    target_ref: str,
    text: str,
    mention_refs: Sequence[str] = (),
    wire_echo: Optional[str] = None,
) -> Dict[str, Any]:
    """编码 send_group_msg / send_private_msg；目标必须是桥可执行的数字 ID。

    `action_id` 是稳定的**业务**标识（对外可见、用于重复拒绝）；
    `wire_echo` 是本次实际发送的**传输**关联标识（每次唯一）。
    两者分离，避免复用业务 ID 时把旧动作迟到/重复的 echo 认成新动作。
    """

    if not action_id:
        raise ChannelActionError("missing_action_id", "动作缺少 action_id")
    if conversation_type not in {_GROUP, _PRIVATE}:
        raise ChannelActionError(
            "unsupported_target_type", f"不支持的会话类型：{conversation_type}"
        )
    if not str(target_ref).strip().isdigit():
        # 桥的 group_id/user_id 是数字假 ID；昵称或 wxid 不能当路由目标。
        raise ChannelActionError("invalid_target", "目标 ID 必须是桥的可执行数字 ID")

    body_text = text or ""
    mentions = [str(ref) for ref in mention_refs if str(ref).strip()]
    if not body_text.strip() and not mentions:
        raise ChannelActionError("empty_action", "动作没有正文也没有 @ 目标")

    segments: List[Dict[str, Any]] = [
        {"type": "at", "data": {"qq": ref}} for ref in mentions
    ]
    if body_text:
        segments.append({"type": "text", "data": {"text": body_text}})

    params: Dict[str, Any] = {"message": segments}
    if conversation_type == _GROUP:
        params["group_id"] = int(target_ref)
        action = "send_group_msg"
    else:
        params["user_id"] = int(target_ref)
        action = "send_private_msg"

    return {"action": action, "params": params, "echo": wire_echo or action_id}


#: P12：图片原件允许的 MIME 签名（与资源目录 sniff 一致的四类）。
_MEDIA_MAGIC = (
    (b"\x89PNG\r\n\x1a\n", "image/png"),
    (b"\xff\xd8\xff", "image/jpeg"),
    (b"GIF8", "image/gif"),
)


def media_mime_from_bytes(data: bytes) -> Optional[str]:
    """按字节签名判断真实图片类型；未知类型返回 None（不得猜测发送）。"""

    if isinstance(data, bytes):
        for magic, mime in _MEDIA_MAGIC:
            if data.startswith(magic):
                return mime
    return None


def build_media_action_payload(
    *,
    action: str,
    action_id: str,
    conversation_type: str,
    target_ref: str,
    text: str,
    image_base64: str,
    image_name: str,
    image_sha256: str,
    wire_echo: Optional[str] = None,
) -> Dict[str, Any]:
    """编码版本化媒体动作 `pero_media_send_v1`；桥只做传输与 GUI 执行。

    边界：
    - 图片只传 Base64 原件字节与元数据（名称/哈希），**禁止任何磁盘路径**；
    - 文字与图片都必须非空（本协议不支持纯图）；
    - 业务 `action_id` 与传输 `wire_echo` 分离，与 send_* 动作同一套隔离规则。
    """

    if not action_id:
        raise ChannelActionError("missing_action_id", "媒体动作缺少 action_id")
    if conversation_type not in {_GROUP, _PRIVATE}:
        raise ChannelActionError(
            "unsupported_target_type", f"不支持的会话类型：{conversation_type}"
        )
    if not str(target_ref).strip().isdigit():
        raise ChannelActionError("invalid_target", "目标 ID 必须是桥的可执行数字 ID")
    if not str(image_base64).strip():
        raise ChannelActionError("empty_media", "媒体动作缺少图片 Base64")
    if not str(text or "").strip():
        raise ChannelActionError("empty_action", "媒体动作缺少文字正文")
    params: Dict[str, Any] = {
        # 业务 ID 显式随协议传递（与传输 wire_echo 分离）；桥用它做
        # 账号+目标+业务 ID 的有界防重，同动作重复不会再次 GUI 操作。
        "action_id": str(action_id)[:80],
        "text": str(text),
        "image_base64": str(image_base64),
        "image_name": str(image_name or "sticker")[:120],
        "image_sha256": str(image_sha256 or "")[:64],
    }
    if conversation_type == _GROUP:
        params["group_id"] = int(target_ref)
    else:
        params["user_id"] = int(target_ref)
    return {"action": action, "params": params, "echo": wire_echo or action_id}


def media_receipt_from_echo(
    payload: Mapping[str, Any], *, action_id: str
) -> Tuple[str, Dict[str, Any]]:
    """解析媒体动作回执，返回 ``(text_state, 分项事实)``。

    桥侧约定（`pero_media_send_v1`）：
    - `data.text_state` / `data.image_state` ∈ sent/failed/unknown（分项事实）；
    - `data.text_message_id` / `data.image_message_id` 只在对应 GUI 确认后出现；
    - 整体 status/retcode 只作聚合参考，不能替代分项事实。
    """

    status = payload.get("status")
    data = payload.get("data")
    data = data if isinstance(data, Mapping) else {}
    text_state = str(data.get("text_state") or "").strip().lower()
    image_state = str(data.get("image_state") or "").strip().lower()
    if text_state not in {"sent", "failed", "unknown"}:
        text_state = "unknown"
    if image_state not in {"sent", "failed", "unknown", "skipped"}:
        image_state = "unknown"
    facts = {
        # 经校验的 text_state 进入 facts（P12-R02）：返回值与 facts 一致，
        # 下游（DeliveryOutcome/L20）不需要从聚合 status 反推文字分项。
        "text_state": text_state,
        "image_state": image_state,
        "text_message_id": str(data.get("text_message_id") or "")[:80],
        "image_message_id": str(data.get("image_message_id") or "")[:80],
        "evidence": str(data.get("evidence") or "")[:200],
        "status": str(status or "")[:20],
        "retcode": payload.get("retcode"),
    }
    return text_state, facts


#: 批量媒体协议单次最多图片张数（与 stickers.max_stickers_per_reply 上限一致）。
MEDIA_BATCH_MAX_IMAGES = 6


def build_media_batch_payload(
    *,
    action: str,
    action_id: str,
    conversation_type: str,
    target_ref: str,
    text: str,
    images: Sequence[Mapping[str, Any]],
    wire_echo: Optional[str] = None,
    max_payload_bytes: int = 24 * 1024 * 1024,
) -> Dict[str, Any]:
    """编码版本化批量媒体动作 `pero_media_send_v2`：文字+多图一次投递。

    与 v1 相同边界：只传 Base64 原件与元数据，禁止磁盘路径；文字非空；
    每项带稳定序号（index）、资产 ID、名称、SHA256。整批编码后大小在发送
    前校验（低于 WebSocket 帧上限），超限抛 `batch_too_large`，不做 GUI 操作。
    """

    if not action_id:
        raise ChannelActionError("missing_action_id", "批量媒体动作缺少 action_id")
    if conversation_type not in {_GROUP, _PRIVATE}:
        raise ChannelActionError(
            "unsupported_target_type", f"不支持的会话类型：{conversation_type}"
        )
    if not str(target_ref).strip().isdigit():
        raise ChannelActionError("invalid_target", "目标 ID 必须是桥的可执行数字 ID")
    if not str(text or "").strip():
        raise ChannelActionError("empty_action", "批量媒体动作缺少文字正文")
    items = list(images or ())
    if not items:
        raise ChannelActionError("empty_media", "批量媒体动作缺少图片")
    if len(items) > MEDIA_BATCH_MAX_IMAGES:
        raise ChannelActionError(
            "too_many_images", f"单次最多 {MEDIA_BATCH_MAX_IMAGES} 张图片"
        )
    encoded: List[Dict[str, Any]] = []
    for index, item in enumerate(items):
        if not isinstance(item, Mapping):
            raise ChannelActionError("invalid_media_item", "图片项结构不合法")
        b64 = str(item.get("image_base64") or "").strip()
        if not b64:
            raise ChannelActionError("empty_media", f"第 {index + 1} 张图片缺少 Base64")
        encoded.append(
            {
                "index": index,
                "asset_id": str(item.get("asset_id") or "")[:80],
                "image_base64": b64,
                "image_name": str(item.get("image_name") or f"sticker-{index}")[:120],
                "image_sha256": str(item.get("image_sha256") or "")[:64],
            }
        )
    params: Dict[str, Any] = {
        "action_id": str(action_id)[:80],
        "text": str(text),
        "images": encoded,
    }
    if conversation_type == _GROUP:
        params["group_id"] = int(target_ref)
    else:
        params["user_id"] = int(target_ref)
    payload = {"action": action, "params": params, "echo": wire_echo or action_id}
    try:
        body_size = len(json.dumps(payload, ensure_ascii=False).encode("utf-8"))
    except (TypeError, ValueError) as exc:  # pragma: no cover - 元数据已校验
        raise ChannelActionError("invalid_media_item", f"批量动作无法编码：{exc}") from exc
    if int(max_payload_bytes) > 0 and body_size > int(max_payload_bytes):
        raise ChannelActionError(
            "batch_too_large",
            f"批量图片编码后 {body_size} 字节超过上限 {max_payload_bytes}",
        )
    return payload


def media_batch_receipt_from_echo(
    payload: Mapping[str, Any],
    *,
    action_id: str,
    expected: int,
    expected_asset_ids: Optional[Sequence[str]] = None,
) -> Tuple[str, Dict[str, Any]]:
    """解析 `pero_media_send_v2` 回执：文字分项 + 逐图状态 + 整批聚合。

    桥侧约定：
    - `data.text_state` ∈ sent/failed/unknown；
    - `data.images[]` 每项 `{index, state}`，state ∈ sent/failed/unknown/skipped；
    - `data.batch_state` 只作一致性提示，**不采纳**为最终状态。

    严格性（R01-05）：
    - `index` 必须是真实整数（bool/小数/字符串一律不接受），越界忽略；
    - 重复 `index`：该项标 unknown（`duplicate_receipt`），不保留先到的 sent；
    - 缺失项：unknown（`missing_receipt`）；
    - 与请求资产不一致的项：unknown（`asset_mismatch`），不让回执里的
      其他资产冒充本次目标；输出项 `asset_id` 始终是**请求侧身份**，
      回执资产只写进独立诊断字段 `reported_asset_id`；
    - 兼容旧桥：调用方未提供 `expected_asset_ids` 时不做资产比较，
      `asset_id` 回退为回执值；回执未带资产时 `asset_id` 仍取请求资产、
      `reported_asset_id` 为空串；
    - 最终 `batch_state` 由已验证的文字与逐项状态推导；上游自报不一致时
      只记入 `evidence`，不覆盖事实。
    """

    total = max(0, int(expected))
    expected_ids = list(expected_asset_ids or [])
    status = payload.get("status")
    data = payload.get("data")
    data = data if isinstance(data, Mapping) else {}
    text_state = str(data.get("text_state") or "").strip().lower()
    if text_state not in {"sent", "failed", "unknown"}:
        text_state = "unknown"
    raw_items = data.get("images")
    raw_items = raw_items if isinstance(raw_items, (list, tuple)) else []
    by_index: Dict[int, Dict[str, Any]] = {}
    duplicates: set = set()
    invalid_index = 0
    for raw in raw_items:
        if not isinstance(raw, Mapping):
            invalid_index += 1
            continue
        index = raw.get("index")
        # bool 是 int 的子类，必须单独排除；小数/字符串不做静默转换。
        if isinstance(index, bool) or not isinstance(index, int):
            invalid_index += 1
            continue
        if index < 0 or index >= total:
            invalid_index += 1
            continue
        if index in by_index:
            duplicates.add(index)
            continue
        state = str(raw.get("state") or "").strip().lower()
        if state not in {"sent", "failed", "unknown", "skipped"}:
            state = "unknown"
        reported_asset = str(raw.get("asset_id") or "")[:80]
        expected_asset = (
            str(expected_ids[index] or "")[:80] if index < len(expected_ids) else ""
        )
        error = str(raw.get("error") or "")[:120]
        item = {
            "index": index,
            "state": state,
            # 规范身份 = 请求侧资产 ID；回执资产只作诊断，不覆盖请求身份。
            # 调用方未提供请求资产（旧调用/旧桥）时才回执资产兜底。
            "asset_id": expected_asset or reported_asset,
            "reported_asset_id": reported_asset,
            "error": error,
        }
        if expected_asset and reported_asset and reported_asset != expected_asset:
            item["state"] = "unknown"
            item["error"] = "asset_mismatch"
        by_index[index] = item
    images: List[Dict[str, Any]] = []
    for index in range(total):
        item = by_index.get(index)
        if item is None:
            images.append(
                {
                    "index": index,
                    "state": "unknown",
                    "asset_id": (
                        str(expected_ids[index] or "")[:80]
                        if index < len(expected_ids)
                        else ""
                    ),
                    "reported_asset_id": "",
                    "error": "missing_receipt",
                }
            )
            continue
        if index in duplicates:
            item = {**item, "state": "unknown", "error": "duplicate_receipt"}
        images.append(item)
    states = [item["state"] for item in images]
    confirmed = (1 if text_state == "sent" else 0) + sum(
        1 for state in states if state == "sent"
    )
    uncertain = (1 if text_state == "unknown" else 0) + sum(
        1 for state in states if state == "unknown"
    )
    failed = (1 if text_state == "failed" else 0) + sum(
        1 for state in states if state == "failed"
    )
    units = 1 + total
    if confirmed == units:
        derived_state = "sent"
    elif confirmed > 0:
        derived_state = "partial"
    elif uncertain > 0:
        derived_state = "unknown"
    elif failed > 0:
        derived_state = "failed"
    else:
        derived_state = "unknown"
    upstream_state = str(data.get("batch_state") or "").strip().lower()
    evidence = str(data.get("evidence") or "")[:200]
    if upstream_state and upstream_state != derived_state:
        evidence = f"{evidence} / upstream_batch_state={upstream_state}"[:200].strip(" /")
    notes: List[str] = []
    if duplicates:
        notes.append(f"duplicate_receipt={sorted(duplicates)}")
    if invalid_index:
        notes.append(f"invalid_index={invalid_index}")
    if notes:
        evidence = f"{evidence} / {','.join(notes)}"[:200].strip(" /")
    facts = {
        "text_state": text_state,
        "images": images,
        "batch_state": derived_state,
        "upstream_batch_state": upstream_state,
        "text_message_id": str(data.get("text_message_id") or "")[:80],
        "evidence": evidence,
        "status": str(status or "")[:20],
        "retcode": payload.get("retcode"),
    }
    return text_state, facts


def parse_conversation_id(value: Any) -> Optional[Tuple[str, str, str, str]]:
    """解析 `onebot11:{account}:{group|private}:{target}`；不合法返回 None。"""

    if not isinstance(value, str):
        return None
    parts = value.split(":")
    if len(parts) != 4:
        return None
    channel, account, conversation_type, target = parts
    if channel != CHANNEL_ID:
        return None
    if conversation_type not in {_GROUP, _PRIVATE}:
        return None
    normalized_account = valid_account_id(account)
    if normalized_account != account:
        return None
    if not target.isdigit():
        return None
    return channel, account, conversation_type, target


def assert_action_scope(
    action: OutboundAction,
    *,
    account_id: str,
    conversation_type: str,
    target_ref: str,
) -> None:
    """动作的会话标识、显式账号、类型与目标必须完全一致。

    防止"带着 A 会话的标识发到 B 目标"，或把某个账号的动作路由到另一个账号的连接。
    """

    if not isinstance(action, OutboundAction):
        raise ChannelActionError("invalid_action", "只接受 core.contracts.OutboundAction")
    normalized_account = valid_account_id(account_id)
    if normalized_account is None or normalized_account != str(account_id):
        raise ChannelActionError("invalid_account", "动作缺少合法账号上下文")

    parsed = parse_conversation_id(action.conversation_id)
    if parsed is None:
        raise ChannelActionError(
            "invalid_conversation_id",
            f"会话标识不合法：{action.conversation_id!r}",
        )
    _, conv_account, conv_type, conv_target = parsed
    if conv_account != normalized_account:
        raise ChannelActionError(
            "conversation_account_mismatch",
            "会话标识的账号与动作账号不一致",
        )
    if conv_type != conversation_type:
        raise ChannelActionError(
            "conversation_type_mismatch",
            f"会话标识类型 {conv_type} 与目标类型 {conversation_type} 不一致",
        )
    if conv_target != str(target_ref):
        raise ChannelActionError(
            "conversation_target_mismatch",
            "会话标识的目标与显式目标不一致",
        )


def encode_action(
    action: OutboundAction,
    *,
    account_id: str,
    conversation_type: str,
    target_ref: str,
    wire_echo: Optional[str] = None,
) -> Dict[str, Any]:
    """把一个标准出站动作编码为 OneBot 动作帧；不支持的动作明确失败。

    `wire_echo` 由调用方为**每一次实际发送**生成唯一值；省略时退回业务 `action_id`。
    """

    assert_action_scope(
        action,
        account_id=account_id,
        conversation_type=conversation_type,
        target_ref=target_ref,
    )
    if action.reply_to_event_id:
        # 桥没有引用回复动作，不伪造 None 目标。
        raise ChannelActionError("unsupported_reply", "桥不支持引用回复动作")
    action_id = action.action_id or f"act-{uuid.uuid4().hex[:12]}"
    return build_action_payload(
        action_id=action_id,
        conversation_type=conversation_type,
        target_ref=target_ref,
        text=action.text,
        mention_refs=action.mention_refs,
        wire_echo=wire_echo,
    )


# --------------------------------------------------------------------------- #
# 回执
# --------------------------------------------------------------------------- #


def receipt_from_echo(payload: Mapping[str, Any], *, action_id: str) -> SendReceipt:
    """把动作 echo 响应翻译成回执。

    桥的证据链（只读核对 `D:\\webot\\bridge`）：
    - `onebot11.py::_on_raw`：`send_*` 动作若处理器没返回带 `message_id` 的 dict，
      会抛 `ActionFailed`，即 `status=ok` 且带 `message_id` 才代表 GUI 确认发送；
    - `main.py` 发送分支：`SendResult` 为 sent 才返回 `message_id`；
      `unknown` / `failed` 会抛 `ActionFailed(delivery_state=..., retcode=1201/1200)`；
    - `send_result.py`：GUI "已操作发送"/"未确认" 归为 unknown，不是 sent。
    """

    status = payload.get("status")
    data = payload.get("data")
    data = data if isinstance(data, Mapping) else {}
    retcode = payload.get("retcode")

    if status == "ok":
        if data.get("message_id"):
            return SendReceipt(
                status=SendStatus.SENT,
                detail="桥已确认发送",
                action_id=action_id,
                evidence="status=ok 且返回 message_id（桥侧仅在 GUI 确认后返回）",
            )
        return SendReceipt(
            status=SendStatus.UNKNOWN,
            detail="桥受理但未提供 message_id",
            action_id=action_id,
            evidence=f"status=ok 但缺少 message_id（retcode={retcode}）",
        )

    if status == "failed":
        delivery_state = data.get("delivery_state")
        if delivery_state == _DELIVERY_FAILED:
            return SendReceipt(
                status=SendStatus.FAILED,
                detail="桥报告发送失败",
                action_id=action_id,
                evidence=f"status=failed，delivery_state=failed（retcode={retcode}）",
            )
        if delivery_state == _DELIVERY_UNKNOWN:
            return SendReceipt(
                status=SendStatus.UNKNOWN,
                detail="桥无法确认是否已发送",
                action_id=action_id,
                evidence=f"status=failed，delivery_state=unknown（retcode={retcode}）",
            )
        return SendReceipt(
            status=SendStatus.UNKNOWN,
            detail="桥报告失败但未给出投递状态",
            action_id=action_id,
            evidence=f"status=failed，缺少 delivery_state（retcode={retcode}）",
        )

    return SendReceipt(
        status=SendStatus.UNKNOWN,
        detail="动作回执不可判定",
        action_id=action_id,
        evidence=f"未知 status={status!r}",
    )
