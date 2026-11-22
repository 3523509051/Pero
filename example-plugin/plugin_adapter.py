"""示例插件的宿主接线层：只把业务函数适配成工具声明。"""

from __future__ import annotations

import sys
from pathlib import Path

_ROOT = str(Path(__file__).resolve().parent)
_INSERTED = _ROOT not in sys.path
if _INSERTED:
    sys.path.insert(0, _ROOT)
try:
    from business.digest import inspect_note
finally:
    if _INSERTED:
        sys.path.remove(_ROOT)


def register():
    """由宿主在启用时调用；返回值必须与 plugin.json 的工具声明一致。"""

    async def handler(arguments, sdk):
        text = arguments.get("text", "")
        refresh = bool(arguments.get("refresh", False))
        options = {
            "max_chars": int(sdk.config.get("max_chars", 400)),
            "include_image": bool(sdk.config.get("include_image", True)),
            "refresh": refresh,
        }
        result = await inspect_note(text, options)

        try:
            count = await sdk.storage.read_json("calls.json")
            count = int(count.get("count", 0)) + 1
        except Exception:
            count = 1
        await sdk.storage.write_json("calls.json", {"count": count})

        artifacts = []
        warnings = list(result.get("warnings") or [])
        image = result.get("image")
        if image and image.get("bytes"):
            try:
                resource_id = await sdk.artifacts.publish_image(
                    bytes(image["bytes"]), str(image["name"])
                )
                evidence = await sdk.images.attach(resource_id, reason="example.note")
                artifacts.append({
                    "kind": "contact_sheet",
                    "resource_id": resource_id,
                    "mime": evidence.get("mime") or "image/png",
                    "size": evidence.get("size") or 0,
                })
            except Exception as exc:
                code = getattr(exc, "code", None) or type(exc).__name__
                warnings.append("图片产物未挂载：" + str(code)[:40])
        data = dict(result.get("data") or {})
        data["call_count"] = count
        sdk.events.emit("plugin.record", {"tool": "note_digest", "cache_hit": data.get("cache", {}).get("hit", False)})
        return {
            "ok": bool(result.get("ok", True)),
            "status": result.get("status", "complete"),
            "data": data,
            "artifacts": artifacts,
            "warnings": warnings,
        }

    return [{
        "name": "note_digest",
        "description": "把用户给出的文字整理为有界摘要；可选刷新缓存。图片产物由插件自行生成并导入当前会话。",
        "parameters": {
            "type": "object",
            "properties": {
                "text": {"type": "string", "minLength": 1, "maxLength": 4000},
                "refresh": {"type": "boolean"},
            },
            "required": ["text"],
            "additionalProperties": False,
        },
        "handler": handler,
    }]
