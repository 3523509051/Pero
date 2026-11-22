"""独立业务逻辑：不导入 Pero，不调用模型，不访问宿主路径。

这里用纯标准库做有界摘要和一张 PNG 缩略图，足以验证真实产物导入、
图片挂载和缓存命中；它不是视频模块，视频业务另见独立实现规范。
"""

from __future__ import annotations

import hashlib
import re
import struct
import time
import zlib
from collections import OrderedDict
from pathlib import Path
from typing import Any, Mapping

_CACHE: "OrderedDict[str, dict]" = OrderedDict()
_CACHE_LIMIT = 32


def _sheet(width: int, height: int, rgb: tuple[int, int, int]) -> bytes:
    def chunk(tag: bytes, payload: bytes) -> bytes:
        return (struct.pack(">I", len(payload)) + tag + payload
                + struct.pack(">I", zlib.crc32(tag + payload) & 0xFFFFFFFF))

    raw = b"".join(b"\x00" + bytes(rgb) * width for _ in range(height))
    return (b"\x89PNG\r\n\x1a\n"
            + chunk(b"IHDR", struct.pack(">IIBBBBB", width, height, 8, 2, 0, 0, 0))
            + chunk(b"IDAT", zlib.compress(raw, 9))
            + chunk(b"IEND", b""))


async def inspect_note(text: str, options: Mapping[str, Any]) -> dict:
    """返回有界结构化结果；图片是独立字节，不塞进 JSON 正文。"""

    if not isinstance(text, str) or not text.strip():
        raise ValueError("empty_text")
    text = text.strip()
    max_chars = int(options.get("max_chars") or 400)
    include_image = bool(options.get("include_image", True))
    refresh = bool(options.get("refresh", False))
    digest = hashlib.sha256(f"{text}\0{max_chars}\0{include_image}".encode("utf-8")).hexdigest()
    key = digest[:24]
    hit = (not refresh) and key in _CACHE
    if hit:
        item = _CACHE.pop(key)
        _CACHE[key] = item
        result = dict(item["result"])
        result["data"] = dict(result["data"])
        result["data"]["cache"] = {**result["data"]["cache"], "hit": True}
        return result
    pieces = [re.sub(r"\s+", " ", part).strip() for part in re.split(r"[\n。！？!?；;]+", text)]
    pieces = [part for part in pieces if part]
    summary = "；".join(pieces)
    if len(summary) > max_chars:
        summary = summary[: max_chars - 1] + "…"
    sheet = _sheet(320, 80, (58, 106, 190)) if include_image else b""
    result = {
        "ok": True,
        "status": "complete",
        "data": {
            "summary": summary,
            "characters": len(text),
            "cache": {"hit": False, "key": key, "created_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())},
        },
        "image": {"bytes": sheet, "name": "note-sheet.png", "mime": "image/png"} if sheet else None,
        "warnings": [] if include_image else ["插件配置关闭了图片产物"],
    }
    _CACHE[key] = {"result": result}
    while len(_CACHE) > _CACHE_LIMIT:
        _CACHE.popitem(last=False)
    return dict(result)


__all__ = ["inspect_note"]
