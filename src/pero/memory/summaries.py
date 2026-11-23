"""工具结果的受控摘要：只保留可公开的短事实，绝不落正文。

规则：
- 已知工具（当前只有 `get_current_time`）可以保存一条短摘要；
- 未登记摘要规则的工具**只保存元数据**（工具名、状态、错误码、耗时），
  不把 `json.dumps(result)` 整段入库；
- 任何情况下都不保存工具参数、网页/文件原文、base64、Key、Authorization 或思维链。
"""

from __future__ import annotations

import json
from typing import Any, Mapping, Sequence, Tuple

from pero.tools.executor import ToolOutcome

SUMMARY_MAX_CHARS = 200

#: 允许生成正文摘要的工具白名单；新增工具必须在这里显式登记。
SUMMARY_RULES: Tuple[str, ...] = ("get_current_time", "remember", "recall", "web_search", "read_website", "read_file", "list_files", "look_image", "list_images", "amap_weather", "amap_places", "amap_route", "list_stickers", "save_sticker", "rename_sticker")

#: 单条证据最多保留多少个记录引用（避免引用列表膨胀）。
MAX_RESOURCE_REFS = 5


def _clip(text: str, limit: int = SUMMARY_MAX_CHARS) -> Tuple[str, bool]:
    if len(text) <= limit:
        return text, False
    return text[:limit], True


def _parse_result(content: str) -> Mapping[str, Any]:
    try:
        payload = json.loads(content)
    except (TypeError, ValueError):
        return {}
    if not isinstance(payload, dict):
        return {}
    result = payload.get("result")
    return result if isinstance(result, dict) else {}


def _summarize_tool_outcome(outcome: ToolOutcome) -> Tuple[str, Sequence[str], bool]:
    """返回 ``(summary, resource_refs, summary_truncated)``。

    `summary` 最多 200 字符；未登记工具只给一句元数据说明。
    """

    name = (outcome.name or "").strip() or "unknown_tool"
    if not outcome.ok:
        code = outcome.error_code or "tool_error"
        text, truncated = _clip(f"{name} 调用未成功（{code}）")
        return text, (), truncated

    if name in {'search_knowledge','read_knowledge'}:
        result=_parse_result(outcome.content);items=result.get('items') or []
        refs=tuple(f"{x.get('doc_id','')} / {x.get('version_id','')} / {x.get('chunk_id','')}" for x in items[:10])
        text,truncated=_clip(f"知识库：{result.get('state','unknown')}，检索 {len(items)} 个片段，模式 {result.get('mode','unknown')}")
        return text,refs,truncated

    result = _parse_result(outcome.content)
    plugin = str(result.get("plugin") or "").strip()[:80]
    if plugin:
        # 插件工具只落受控事实：插件名、状态、缓存命中与产物引用。
        # data 里的摘要/字幕正文和图片字节一律不进 L20。
        status = str(result.get("status") or "unknown")[:24]
        cache = "待执行" if result.get("status")=="queued" else ("命中" if result.get("cache_hit") else "未命中")
        artifacts = result.get("artifacts") if isinstance(result.get("artifacts"), list) else []
        refs = tuple(str(item.get("resource_id") or "")[:64] for item in artifacts
                     if isinstance(item, Mapping) and item.get("resource_id"))[:MAX_RESOURCE_REFS]
        job = str((result.get("data") or {}).get("job_id") or "")[:40] if status=="queued" and isinstance(result.get("data"),Mapping) else ""
        text, truncated = _clip(f"插件 {plugin} 工具 {name}：{status}；缓存{cache}；产物 {len(artifacts)} 项 {job}")
        return text, refs, truncated

    if name not in SUMMARY_RULES:
        text, truncated = _clip(f"{name} 已执行；结果未持久化（无受控摘要规则）")
        return text, (), truncated

    if name == "get_current_time":
        result = _parse_result(outcome.content)
        stamp = str(result.get("beijing_iso") or "").strip()
        date = str(result.get("beijing_date") or "").strip()
        weekday = str(result.get("weekday") or "").strip()
        parts = [part for part in (stamp, date, weekday) if part]
        detail = "，".join(parts) if parts else "未返回可读时间字段"
        text, truncated = _clip(f"北京时间：{detail}")
        return text, (), truncated

    if name in {"web_search", "read_website"}:
        result = _parse_result(outcome.content)
        stamp = str(result.get("retrieved_at") or "")[:50]
        if name == "web_search":
            query = str(result.get("query") or "")[:128]
            items = result.get("results") or []
            refs = tuple(str(item.get("url") or "")[:500] for item in items[:MAX_RESOURCE_REFS] if isinstance(item,dict))
            text, truncated = _clip(f"搜索：{query}；命中 {len(items)} 条；时间 {stamp}")
        else:
            refs = (str(result.get("url") or "")[:500],)
            text, truncated = _clip(f"网页：{str(result.get('title') or '')[:100]}；读取成功；时间 {stamp}")
        return text, refs, truncated

    if name == "read_file":
        result=_parse_result(outcome.content)
        rid=str(result.get("resource_id") or "")[:100]
        title=str(result.get("name") or "")[:100]
        state="已提供给模型" if result.get("provided_to_model") else "已挂载，尚未确认提供给模型"
        text,truncated=_clip(f"文件 {rid} / {title} / v{result.get('version')}：{state}；范围=整份文件，无分页保证")
        return text,(rid,) if rid else (),truncated

    if name == "look_image":
        result=_parse_result(outcome.content)
        rid=str(result.get("resource_id") or "")[:100]
        title=str(result.get("name") or "")[:100]
        state="已提供给模型" if result.get("provided_to_model") else "已挂载，未确认提供给模型"
        text,truncated=_clip(f"图片 {title} / {rid}：{state}；不代表语义识别已验证")
        return text,(rid,) if rid else (),truncated

    if name == "list_images":
        result=_parse_result(outcome.content)
        items=result.get("items") or []
        text,truncated=_clip(f"图片目录：返回 {len(items)} 项，未提供图片内容")
        return text,tuple(str(item.get("resource_id") or "")[:100] for item in items[:5]),truncated

    if name == "list_files":
        result=_parse_result(outcome.content)
        items=result.get("items") or []
        text,truncated=_clip(f"文件目录：返回 {len(items)} 项，未读取正文")
        return text,tuple(str(item.get("resource_id") or "")[:100] for item in items[:5]),truncated

    if name == "remember":
        result = _parse_result(outcome.content)
        status = str(result.get("status") or "unknown").strip()
        memory_id = str(result.get("memory_id") or "").strip()
        subject = str(result.get("subject") or "").strip()
        parts = [f"明确保存：{status}"]
        if memory_id:
            parts.append(f"记录 {memory_id}")
        if subject:
            parts.append(f"对象 {subject}")
        text, truncated = _clip("，".join(parts))
        return text, (memory_id,) if memory_id else (), truncated

    if name == "recall":
        result = _parse_result(outcome.content)
        state = str(result.get("state") or "unknown").strip()
        raw_hits = result.get("hits")
        hits = raw_hits if isinstance(raw_hits, list) else []
        refs = tuple(
            str(item.get("record_id"))
            for item in hits[:MAX_RESOURCE_REFS]
            if isinstance(item, dict) and item.get("record_id")
        )
        text, truncated = _clip(f"检索：{state}，命中 {len(hits)} 条")
        return text, refs, truncated

    if name == "amap_weather":
        # 受控摘要：只落地区/更新时间/实况或预报标记，不复制天气数据正文。
        result = _parse_result(outcome.content)
        city = str(result.get("city") or "")[:50]
        adcode = str(result.get("adcode") or "")[:12]
        reporttime = str(result.get("reporttime") or "")[:30]
        kind = "预报" if result.get("forecast") else "实况"
        parts = [f"高德天气（{kind}）"]
        if city:
            parts.append(city)
        if adcode:
            parts.append(f"adcode {adcode}")
        parts.append(f"更新时间 {reporttime}" if reporttime else "更新时间未知")
        text, truncated = _clip("，".join(parts))
        return text, (), truncated

    if name == "amap_places":
        # 受控摘要：只落命中数量与查询词，地点明细仅本轮使用。
        result = _parse_result(outcome.content)
        pois = result.get("pois") if isinstance(result.get("pois"), list) else []
        city = str(result.get("query_city") or "")[:40]
        text, truncated = _clip(
            f"高德地点搜索{'（' + city + '）' if city else ''}：返回 {len(pois)} 项；明细仅本轮使用"
        )
        refs = tuple(
            str(poi.get("id") or "")[:100]
            for poi in pois[:MAX_RESOURCE_REFS]
            if isinstance(poi, dict) and poi.get("id")
        )
        return text, refs, truncated

    if name == "amap_route":
        # 受控摘要：只落城市/方式/段数与起讫顺序，路线正文仅本轮使用。
        result = _parse_result(outcome.content)
        city = str(result.get("city") or "")[:40]
        mode = str(result.get("mode") or "")[:20]
        legs = result.get("legs") if isinstance(result.get("legs"), list) else []
        endpoints = "→".join(
            filter(None, (
                str((leg or {}).get("from") or "")[:30] for leg in legs[:1]
            ))
        ) or "未知起点"
        if legs:
            endpoints += "→" + str(legs[-1].get("to") or "")[:30]
        text, truncated = _clip(
            f"高德路线：{city or '未知城市'} / {mode or '未知方式'}，{len(legs)} 段（{endpoints}）；"
            "耗时距离为高德估算，仅本轮使用"
        )
        return text, (), truncated

    if name == "list_stickers":
        # 受控摘要：只落命中数/总数/翻页事实，不复制资产明细。
        result = _parse_result(outcome.content)
        if not result.get("ok", True):
            text, truncated = _clip(f"表情目录检索未成功（{result.get('error')}）")
            return text, (), truncated
        returned = len(result.get("items") or [])
        total = int(result.get("total") or 0)
        has_more = bool(result.get("has_more"))
        text, truncated = _clip(
            f"表情目录：返回 {returned} 项 / 共 {total} 项"
            + ("，还有更多" if has_more else "")
        )
        return text, (), truncated

    if name == "save_sticker":
        # 受控摘要：名称 + 资产 ID + 来源资源引用；不落完整元数据/原图。
        result = _parse_result(outcome.content)
        if not result.get("ok", True):
            text, truncated = _clip(f"表情保存未成功（{result.get('error')}）")
            return text, (), truncated
        asset = result.get("asset") or {}
        source_ref = str(asset.get("source_resource_id") or "")
        text, truncated = _clip(
            f"{'复用' if result.get('reused') else '保存'}表情「{asset.get('name')}」"
            f"（{asset.get('asset_id')}）"
        )
        refs = (source_ref[:100],) if source_ref else ()
        return text, refs, truncated

    if name == "rename_sticker":
        # 受控摘要：资产 ID 与新旧名称；只改元数据的事实。
        result = _parse_result(outcome.content)
        if not result.get("ok", True):
            text, truncated = _clip(f"表情重命名未成功（{result.get('error')}）")
            return text, (), truncated
        asset = result.get("asset") or {}
        source_ref = str(asset.get("source_resource_id") or "")
        text, truncated = _clip(
            f"重命名表情（{asset.get('asset_id')}）为「{asset.get('name')}」"
        )
        refs = (source_ref[:100],) if source_ref else ()
        return text, refs, truncated

    text, truncated = _clip(f"{name} 已执行")
    return text, (), truncated


def plugin_tool_facts(outcome) -> dict:
    """插件 L20 受控事实；没有插件标记时返回空字典。"""

    result = _parse_result(getattr(outcome, "content", ""))
    plugin = str(result.get("plugin") or "").strip()[:80]
    if not plugin:
        return {}
    artifacts = result.get("artifacts") if isinstance(result.get("artifacts"), list) else []
    refs = tuple(str(item.get("resource_id") or "")[:64] for item in artifacts
                 if isinstance(item, Mapping) and item.get("resource_id"))[:MAX_RESOURCE_REFS]
    return {
        "plugin_id": plugin,
        "cache_hit": bool(result.get("cache_hit")) if result.get("cache_hit") is not None else None,
        "artifact_refs": refs,
    }


__all__ = ["SUMMARY_MAX_CHARS", "SUMMARY_RULES", "summarize_tool_outcome", "plugin_tool_facts"]


def summarize_tool_outcome(outcome):
    text, refs, truncated = _summarize_tool_outcome(outcome)
    analysis = getattr(outcome, 'analysis', None)
    if analysis:
        text = '[工具模型分析：' + str(analysis.get('state')) + '] ' + text
        if len(text) > SUMMARY_MAX_CHARS:text=text[:SUMMARY_MAX_CHARS];truncated=True
    return text, refs, truncated
