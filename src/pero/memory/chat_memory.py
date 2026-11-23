"""聊天流水的长期记忆后台：归档、人物事实与事件条目（R02）。

L3 只在 L2 压缩成功时积累，与摘要共用一次请求；最多三条长期事实，来源
绑定本批成员原话。普通消息、空闲扫描和明确保存不再独立提取 L3。
原始聊天归档与事件整理保持各自任务；L3 失败不回滚已完成的 L2 前情。
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import uuid
import re
from contextvars import ContextVar
_ERRORS=ContextVar('chat_scan_errors',default=None)
from typing import Any, Dict, List, Optional

from pero.core.contracts import MessageRole, ModelMessage, ModelPurpose, ModelRequest

logger = logging.getLogger(__name__)

ARCHIVE_SYSTEM = (
    "你是群聊归档助手。把给定聊天片段整理成 1 段简明摘要（200 字以内），"
    "保留关键事件、约定与结论；不编造片段里没有的内容。"
    '只输出 JSON：{"summary": "..."}'
)

EVENT_SYSTEM = (
    "你是群聊事件整理助手。只从给定片段提取**明确发生过或已约定**的事件，"
    "每条含 title（20 字内）、detail（100 字内）、status（ongoing/done/planned）、"
    "participants（必须取自给定显示名）、occurred_at（原文出现的日期，否则空）。"
    "没有明确事件就返回空数组；不推测、不把玩笑当约定。"
    '只输出 JSON：{"events": [...]}'
)

#: L2 独立交互归档：把「此前已确认前情 + 本批最早交互原文」累积整理成一段前情。
#: 与 `ARCHIVE_SYSTEM`（原始聊天摘要）分开：L2 只处理机器人**实际参与**的交互。
L2_ARCHIVE_SYSTEM = (
    "你是长期前情整理助手。输入包含「此前已确认前情」和「本批新增交互原文」。"
    "请把它们合并成 1 段可继续使用的前情摘要（600 字以内）："
    "保留未结束的话题、指代对象、约定、待办及其结果；"
    "只写原文支持的内容，不推断、不编造。"
    '只输出 JSON：{"summary": "..."}'
)

_ARCHIVE_MAX_ITEMS = 60
_ARCHIVE_INPUT_CHARS = 12000
_L2_SUMMARY_CHARS = 600
_L2_ARCHIVE_INPUT_CHARS = 12000
_L3_MAX_ITEMS = 40
_L3_MAX_FACTS = 2
_EVENT_MAX_ITEMS = 40
_EVENT_MAX_EVENTS = 2

_EXPLICIT_EVIDENCE = {"explicit_statement", "explicit_preference"}

#: 明确纠正的触发词（只处理"用户在纠正某人"的场景）。
_CORRECTION_WORDS = ("不是", "改成", "纠正", "不对", "其实", "说错", "撤回")

#: 事件类别边界：只有安排/约定/截止/活动类措辞才可能成为事件。
_EVENT_CUES = (
    "约", "约定", "定", "计划", "安排", "报名", "集合", "出发", "截止",
    "提交", "交作业", "开会", "会议", "活动", "比赛", "考试", "聚会",
    "生日", "上线", "开播", "更新", "发布", "见面", "开始", "结束",
)

#: 明确的非事件措辞：偏好、寒暄、媒体行为与即时状态。
_NON_EVENT_WORDS = (
    "喜欢你", "爱吃", "不吃", "讨厌", "最爱", "你好", "晚安", "早安",
    "哈哈", "发送图片", "发了图片", "发送表情", "表情包", "在吗", "谢谢",
)

_RELATIVE_DAYS = (("大后天", 3), ("后天", 2), ("明天", 1), ("明晚", 1),
                  ("今天", 0), ("今晚", 0))


def _is_non_event_wording(text: str) -> bool:
    return any(word in (text or "") for word in _NON_EVENT_WORDS)


def _has_event_cue(text: str) -> bool:
    return any(cue in (text or "") for cue in _EVENT_CUES)


def _source_date(value: str):
    """来源原文的发生日期（用户时区）；解析失败返回 None。"""

    from datetime import datetime

    try:
        from zoneinfo import ZoneInfo

        tz = ZoneInfo("Asia/Shanghai")
    except Exception:  # noqa: BLE001
        from datetime import timedelta, timezone

        tz = timezone(timedelta(hours=8))
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).strip())
    except ValueError:
        return None
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(tz)
    return parsed.date()


_CN_DIGITS = {"零": 0, "〇": 0, "一": 1, "二": 2, "两": 2, "三": 3, "四": 4,
              "五": 5, "六": 6, "七": 7, "八": 8, "九": 9}


def _chinese_number(value) -> Optional[int]:
    """把 "3" / "三" / "十一" / "二十三" 解析成整数；无法解析返回 None。"""

    if value is None or isinstance(value, bool):
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.isdigit():
        return int(text)
    if "十" in text:
        head, _, tail = text.partition("十")
        tens = 1 if head == "" else _CN_DIGITS.get(head)
        ones = 0 if tail == "" else _CN_DIGITS.get(tail)
        if tens is None or ones is None:
            return None
        return tens * 10 + ones
    if len(text) == 1 and text in _CN_DIGITS:
        return _CN_DIGITS[text]
    return None


def _bj_tz():
    """北京时间（UTC+08:00）；Windows 缺 tzdata 时回退固定偏移。"""

    from datetime import timedelta, timezone

    try:
        from zoneinfo import ZoneInfo

        return ZoneInfo("Asia/Shanghai")
    except Exception:  # noqa: BLE001
        return timezone(timedelta(hours=8))


#: 时段词分组：同一时段对"十二点"的解释不同，必须分开处理。
_AM_WORDS = ("凌晨", "清晨", "早上", "上午", "半夜")
_NOON_WORDS = ("中午",)
_PM_WORDS = ("下午", "傍晚")
_NIGHT_WORDS = ("晚上", "今晚", "夜里")


def _apply_day_part(hour: Optional[int], text: str) -> tuple[Optional[int], int]:
    """把时段词换算成 `(24 小时制时钟, 跨天数)`。

    边界约定（写进文档，不静默指到午夜）：
    - 中午十二点 = 12:00；下午/傍晚十二点 = 12:00；
    - 晚上/今晚/夜里十二点 = **次日** 00:00（跨一天，用 day_offset 表达）；
    - 凌晨/半夜十二点 = 00:00（当天零点）；
    - 上午十二点存在用语歧义，按 12:00（正午）处理，**不**指到午夜。
    """

    if hour is None:
        return None, 0
    value = text or ""
    if any(word in value for word in _NIGHT_WORDS):
        if hour == 12:
            return 0, 1
        return (hour + 12 if hour < 12 else hour), 0
    if any(word in value for word in _PM_WORDS):
        if hour == 12:
            return 12, 0
        return (hour + 12 if hour < 12 else hour), 0
    if any(word in value for word in _NOON_WORDS):
        if hour == 12:
            return 12, 0
        return (hour + 12 if hour < 6 else hour), 0
    if any(word in value for word in _AM_WORDS):
        if hour == 12:
            return (0, 0) if any(
                word in value for word in ("凌晨", "半夜")
            ) else (12, 0)
        return hour, 0
    return hour, 0


def _normalise_event_time(raw: str, source_occurred_at: str) -> tuple[str, str]:
    """把时间说法规范化成 `(occurred_at, planned_at)`（北京时间）；不确定就留空。

    - 绝对日期（2026-11-25 / 2026/11/25 / 11月25日）先解析校验，非法一律丢弃；
    - "明天下午三点"这类相对说法结合**来源原话发生时间**换算，并处理
      上午/下午/中午/晚上/凌晨等时段词，不再把下午三点算成 03:00；
    - 已发生的写 `occurred_at`，将来的安排写 `planned_at`，都保留明确钟点，
      ISO8601 明确带 `+08:00`；
    - 只有日期没有钟点时保留日期（不编造 00:00 的钟点）；
    - 解析不出就两个都留空（调用方保留 unknown），不截字符串当日期。
    """

    from datetime import date, datetime, timedelta

    text = str(raw or "").strip()
    if not text:
        return "", ""
    matches = re.findall(
        r"(\d{4})\s*[-/.年]\s*(\d{1,2})\s*[-/.月]\s*(\d{1,2})", text
    )
    if not matches:
        # "11月25日"（无年份）按来源年份补全，避免整条时间丢失。
        partial = re.search(r"(\d{1,2})\s*月\s*(\d{1,2})\s*[日号]", text)
        base_date = _source_date(source_occurred_at)
        if partial and base_date is not None:
            matches = [(str(base_date.year), partial.group(1), partial.group(2))]
    absolute = None
    for year, month, day in matches:
        try:
            absolute = date(int(year), int(month), int(day))
        except ValueError:
            continue
        break
    base = _source_date(source_occurred_at)
    relative = None
    for word, delta in _RELATIVE_DAYS:
        if word in text:
            if base is not None:
                relative = base + timedelta(days=delta)
            break
    clock = re.search(
        r"(\d{1,2}|[一二两三四五六七八九十]{1,3})\s*[点:：]\s*(\d{1,2}|[一二三四五六七八九十]{1,2})?",
        text,
    )
    hour = minute = None
    if clock:
        hour = _chinese_number(clock.group(1))
        minute = _chinese_number(clock.group(2)) if clock.group(2) else 0
        if hour is None or minute is None or not (0 <= hour <= 23 and 0 <= minute <= 59):
            hour = minute = None
    hour, day_offset = _apply_day_part(hour, text)
    if relative is None and absolute is None and hour is None:
        return "", ""
    target = absolute or relative or base
    if target is None:
        return "", ""

    tz = _bj_tz()
    now = datetime.now(tz)
    if hour is None:
        # 只有日期：不补 00:00 的假钟点，按日期与"今天"比较决定发生/计划。
        if target > now.date():
            return "", target.isoformat()
        return target.isoformat(), ""

    moment = datetime(
        target.year, target.month, target.day, int(hour), int(minute or 0), tzinfo=tz
    )
    if day_offset:
        moment = moment + timedelta(days=day_offset)
    if moment > now:
        return "", moment.isoformat(timespec="minutes")
    return moment.isoformat(timespec="minutes"), ""


def _date_literal_supported(value: str, evidence: str) -> bool:
    """模型给的日期是否能在这条原文/引用里找到字面依据。

    "能 parse" 不等于"有来源"：模型不能凭自己编一个合法日期就改写事件时间。
    """

    text = str(value or "")
    haystack = str(evidence or "")
    if not text or not haystack:
        return False
    full = re.findall(r"(\d{4})\s*[-/.年]\s*(\d{1,2})\s*[-/.月]\s*(\d{1,2})", text)
    for year, month, day in full:
        if f"{int(month)}月{int(day)}日" in haystack or f"{int(month)}月{int(day)}号" in haystack:
            continue
        if f"{month}月{day}" in haystack.replace("-", "月").replace("/", "月"):
            continue
        if f"{month}/{day}" in haystack or f"{month}-{day}" in haystack:
            continue
        return False
    partial = re.findall(r"(\d{1,2})\s*月\s*(\d{1,2})\s*[日号]", text)
    for month, day in partial:
        if f"{month}月{day}" not in haystack:
            return False
    return bool(full or partial)


#: 否定词：detail 与证据的极性必须一致，不能把"不去"写成"去"。
_NEGATIONS = "不没未别勿"


def _detail_supported(detail: str, evidence: str) -> bool:
    """detail 的**全部**信息点都要能在证据里字面找到（保守）。

    相似度（如二元组 60% 覆盖）不能证明地点、人数、对象、动作、否定正确：
    "明天下午三点在图书馆集合" → detail "…在体育馆集合" 只差两个字却是另一件事。
    因此这里要求 detail 的每一个中文二元组、数字、英文词都在证据里出现；
    不满足就由调用方回退到逐字 quote。
    """

    text = str(detail or "")
    haystack = str(evidence or "")
    if not text or not haystack:
        return False
    for token in re.findall(r"[0-9]+|[A-Za-z_]{2,}", text):
        if token not in haystack:
            return False
    for char in set(text) & set(_NEGATIONS):
        if char not in haystack:
            return False
    grams = [
        text[index:index + 2]
        for index in range(len(text) - 1)
        if all("\u4e00" <= char <= "\u9fff" for char in text[index:index + 2])
    ]
    if not grams:
        return True
    return all(gram in haystack for gram in grams)


def _title_supported(title: str, evidence: str) -> bool:
    """标题不得引入证据里没有的字（典型：地点/人数被换掉）。

    字符必须全部出现，且中文二元组至少一半能在证据里找到；否则调用方回退到
    原话片段。字符集检查能拦住"体育馆"这种换了地点的改写。
    """

    text = str(title or "")
    haystack = str(evidence or "")
    if not text or not haystack:
        return False
    if any(char not in haystack for char in text if "\u4e00" <= char <= "\u9fff"):
        return False
    for char in set(text) & set(_NEGATIONS):
        if char not in haystack:
            return False
    grams = [
        text[index:index + 2]
        for index in range(len(text) - 1)
        if all("\u4e00" <= char <= "\u9fff" for char in text[index:index + 2])
    ]
    if not grams:
        return True
    hit = sum(1 for gram in grams if gram in haystack)
    return hit / len(grams) >= 0.5


class ChatMemoryService:
    """归档 / 事实 / 事件的后台执行器（每会话单飞）。"""

    def __init__(self, settings) -> None:
        self._settings = settings
        self._locks: Dict[str, asyncio.Lock] = {}
        self._counters = {
            "archive_runs": 0,
            "archive_completed": 0,
            "archive_failed": 0,
            "archive_cursor_repaired": 0,
            "l2_runs": 0,
            "l2_completed": 0,
            "l2_failed": 0,
            "l2_interactions_archived": 0,
            "facts_scans": 0,
            "facts_written": 0,
            "facts_candidates": 0,
            "facts_failed": 0,
            "facts_replaced": 0,
            "facts_blocked": 0,
            "events_blocked": 0,
            "events_detail_reduced": 0,
            "events_title_reduced": 0,
            "events_scans": 0,
            "events_written": 0,
            "events_failed": 0,
            "llm_calls": 0,
        }

    def metadata(self) -> Dict[str, Any]:
        memory = self._settings.memory
        return {
            "windows": {
                "L1": int(memory.chat_l1_messages),
                "L10": int(memory.chat_l10_messages),
                "L2": int(memory.chat_l2_messages),
                "L2_char_budget": int(memory.chat_l2_char_budget),
            },
            "archive": {
                "trigger_messages": int(memory.chat_archive_trigger),
                "batch_messages": int(memory.chat_archive_batch),
                "window_messages": int(memory.chat_archive_window),
                "enabled": bool(self._settings.archive.enabled),
                "input_chars": _ARCHIVE_INPUT_CHARS,
            },
            # L2 独立交互（按**条数**计：用户与已确认送达的助手各 1 条）
            "l2": {
                "trigger": int(memory.chat_l2_trigger),
                "retain": int(memory.chat_l2_retain),
                "batch": int(memory.chat_l2_batch),
                "char_budget": int(memory.chat_l2_char_budget),
                "summary_chars": _L2_SUMMARY_CHARS,
                "input_chars": _L2_ARCHIVE_INPUT_CHARS,
                "counter_unit": "interaction",
                "enabled": True,
            },
            "l3": {
                "trigger": "l2_archive_committed",
                "max_facts_per_archive": 3,
                "enabled": bool(memory.l3_enabled),
                # 事实只随 L2 归档成功写入，没有独立扫描任务。
                "mode": "l2_archive_derived",
                "independent_scan": False,
            },
            # 事件与事实来自**同一次** L2 归档请求；旧的独立事件游标任务已退役。
            "events": {
                "enabled": bool(getattr(memory, "events_enabled", True)),
                "source": "l2_archive_request",
                "mode": "l2_archive_derived",
                "independent_cursor": False,
                "independent_scan": False,
                "retired_independent_task": True,
            },
            "counters": dict(self._counters),
        }

    async def _guarded(self, *, memory, agent_id: str, conversation_id: str,
                       kind: str, provider, force: bool, scanner) -> int:
        """持久任务状态：失败退避、上限后需手动重试；新消息不能绕过失败区间。"""

        if kind=='facts':return 0  # Retired independent job; only L2 archive may add L3.

        cursor_kind={'facts':'l3','events':'event','archive':'archive'}[kind]
        cursor = await memory.chat_cursor_get(
            agent_id=agent_id, conversation_id=conversation_id, kind=cursor_kind
        )
        counts=await memory.chat_counts(agent_id=agent_id,conversation_id=conversation_id)
        if kind=='facts':
            if not getattr(self,'facts_window_active',lambda a,c:False)(agent_id,conversation_id):return 0
            eligible=await memory.chat_l3_eligible(agent_id=agent_id,conversation_id=conversation_id,after_seq=cursor)
            if len(eligible)<5:return 0
        pending=int(counts['last_seq'])-cursor
        threshold=5 if kind=='facts' else int(self._settings.memory.chat_archive_trigger if kind=='archive' else self._settings.memory.l3_scan_every_messages)
        if pending<=0 or ((not force or kind=='archive') and pending<threshold):
            return 0
        if not (self._settings.archive.enabled if kind=='archive' else self._settings.memory.l3_enabled):return 0
        task = await memory.chat_task_begin(
            agent_id=agent_id, conversation_id=conversation_id,
            kind=kind, cursor_seq=cursor,
        )
        if task.get("manual_required"):
            self._counters[f"{kind}_blocked"] = self._counters.get(f'{kind}_blocked',0)+1
            return 0
        next_at = str(task.get("next_allowed_at") or "")
        if next_at:
            # 退避时间按**绝对时刻**比较（旧数据可能是 Z 或 +08:00 混排，
            # 字符串字典序会把"稍后可重试"判成"还要等"）。
            from pero.core import timebase

            if timebase.later_than(next_at, timebase.now()):
                return 0
        token=_ERRORS.set({})
        written=0
        try:
            failures=self._counters.get('archive_failed',0)
            written=await scanner(agent_id=agent_id,conversation_id=conversation_id,memory=memory,provider=provider,force=True if kind=='facts' else force)
            error=self._last_error.get(kind)
            if kind=='archive' and self._counters.get('archive_failed',0)>failures:error='archive_failed'
        except asyncio.CancelledError:
            await asyncio.shield(memory.chat_task_fail(agent_id=agent_id,conversation_id=conversation_id,kind=kind,cursor_seq=cursor,error_code='interrupted',retry_seconds=300))
            raise
        except Exception as exc:
            error=type(exc).__name__
        finally:_ERRORS.reset(token)
        new_cursor = await memory.chat_cursor_get(
            agent_id=agent_id, conversation_id=conversation_id, kind=cursor_kind
        )
        counts = await memory.chat_counts(
            agent_id=agent_id, conversation_id=conversation_id
        )
        last = int(counts["last_seq"])
        if error:
            attempts = int(task.get("attempts") or 0)
            backoff = min(3600.0, 300.0 * (2 ** min(attempts, 4)))
            failed = await memory.chat_task_fail(
                agent_id=agent_id, conversation_id=conversation_id, kind=kind,
                cursor_seq=new_cursor,
                error_code=error,
                retry_seconds=backoff,
            )
            if failed.get("manual_required"):
                self._counters[f"{kind}_blocked"] = self._counters.get(f'{kind}_blocked',0)+1
        else:
            await memory.chat_task_success(
                agent_id=agent_id, conversation_id=conversation_id,
                kind=kind, cursor_seq=new_cursor, pending=new_cursor<last,
                cooldown_seconds=600 if kind=='facts' else 0,
            )
        return written

    @property
    def _last_error(self):
        errors=_ERRORS.get()
        if errors is None:
            errors={};_ERRORS.set(errors)
        return errors

    def _lock(self, key: str) -> asyncio.Lock:
        if key not in self._locks:
            if len(self._locks) > 256:
                self._locks.clear()
            self._locks[key] = asyncio.Lock()
        return self._locks[key]

    async def run(self, *, agent_id: str, conversation_id: str, memory,
                  provider, force: bool = False) -> Dict[str, Any]:
        """跑一轮后台：归档 + 事实 + 事件；返回处理位置与真实调用次数。"""

        result: Dict[str, Any] = {
            "archive": None, "l2": None, "facts": 0, "events": 0, "llm_calls": 0,
            "processed_seq": 0, "last_seq": 0,
        }
        if memory is None:
            return result
        before_calls = self._counters["llm_calls"]
        if provider is not None:
            # 唯一常规归档：L2 交互归档（摘要 + 事实 + 事件同一次请求）。
            # 原始全群摘要与独立事件循环已退役，不再默认调用。
            result["l2"] = await self.maybe_archive_l2(
                agent_id=agent_id, conversation_id=conversation_id,
                memory=memory, provider=provider, force=force,
            )
            result["archive"] = None
            result["facts"] = int((result.get("l2") or {}).get("facts_written") or 0)
            result["events"] = int((result.get("l2") or {}).get("events_written") or 0)
        result["llm_calls"] = self._counters["llm_calls"] - before_calls
        try:
            counts = await memory.chat_counts(
                agent_id=agent_id, conversation_id=conversation_id
            )
            event_cursor = await memory.chat_cursor_get(
                agent_id=agent_id, conversation_id=conversation_id, kind="event"
            )
            result["last_seq"] = int(counts["last_seq"])
            # 后台只按 L2 交互条数调度；没有需要按 seq 追赶的独立任务。
            result["processed_seq"] = int(counts["last_seq"])
            _ = event_cursor
        except Exception:  # noqa: BLE001
            pass
        return result

    # ------------------------------------------------------------------ #
    # 归档
    # ------------------------------------------------------------------ #

    async def maybe_archive(self, *, agent_id: str, conversation_id: str,
                            memory, provider, force: bool=False) -> Optional[Dict[str, Any]]:
        memory_settings = self._settings.memory
        if not self._settings.archive.enabled:
            return None
        lock = self._lock(f"archive:{agent_id}:{conversation_id}")
        if lock.locked():
            return None
        async with lock:
            plan = await memory.chat_archive_plan(
                agent_id=agent_id, conversation_id=conversation_id,
                trigger_messages=int(memory_settings.chat_archive_trigger),
                batch_messages=int(memory_settings.chat_archive_batch),
                window_messages=int(memory_settings.chat_archive_window),
            )
            if plan is None:
                return None
            if plan.get("existing"):
                state = str(plan.get("state") or "")
                if state == "completed":
                    await memory.chat_archive_advance(
                        agent_id=agent_id, conversation_id=conversation_id,
                        to_seq=int(plan["to_seq"]),
                    )
                    self._counters["archive_cursor_repaired"] += 1
                    plan = await memory.chat_archive_plan(
                        agent_id=agent_id, conversation_id=conversation_id,
                        trigger_messages=int(memory_settings.chat_archive_trigger),
                        batch_messages=int(memory_settings.chat_archive_batch),
                        window_messages=int(memory_settings.chat_archive_window),
                    )
                    if plan is None or plan.get("existing"):
                        return plan
                else:
                    return plan
            archive_id = str(plan["archive_id"])
            if not await memory.chat_archive_claim(
                archive_id=archive_id, agent_id=agent_id
            ):
                return None
            self._counters["archive_runs"] += 1
            raw_items = list(plan.get("items") or [])[:_ARCHIVE_MAX_ITEMS]
            items: List[Dict[str, Any]] = []
            lines: List[str] = []
            used = 0
            for item in raw_items:
                who = "大肥鱼" if item.get("role") == "assistant" else (
                    item.get("display_name") or item.get("sender_ref") or "成员"
                )
                source_label=(f" [source_event_id={item.get('event_id')}]" if self._settings.memory.l3_enabled and item.get('role')=='member' else '')
                line = f"[{str(item.get('occurred_at') or '')[:16]}] {who}{source_label}：{item.get('text') or ''}"
                if items and used + len(line) + 1 > _ARCHIVE_INPUT_CHARS:
                    break
                items.append(item)
                lines.append(line)
                used += len(line) + 1
            if not items:
                await self._settle_safely(
                    memory, archive_id, agent_id, "failed", "empty_input"
                )
                return None
            to_seq = int(items[-1].get("seq") or plan["to_seq"])
            try:
                if provider is None:
                    raise RuntimeError("provider_unavailable")
                summary_text = await self._summarise(provider, "\n".join(lines))
            except asyncio.CancelledError:
                await self._settle_safely(
                    memory, archive_id, agent_id, "interrupted", "cancelled"
                )
                raise
            except Exception as exc:  # noqa: BLE001
                self._counters["archive_failed"] += 1
                await self._settle_safely(
                    memory, archive_id, agent_id, "failed", type(exc).__name__
                )
                logger.warning("chat archive failed cid=%s error=%s",
                               conversation_id, type(exc).__name__)
                return None
            parts = conversation_id.split(":")
            try:
                committed = await memory.chat_archive_commit(
                    archive_id=archive_id, agent_id=agent_id,
                    conversation_id=conversation_id, to_seq=to_seq,
                    memory_id="lm-" + uuid.uuid4().hex,
                    summary_chars=len(summary_text),
                    channel_id=parts[0] if parts else "",
                    account_id=parts[1] if len(parts) > 1 else "",
                    content=summary_text,
                    source_seqs=[int(item.get("seq") or 0) for item in items],
                    occurred_at=str(items[-1].get("occurred_at") or ""),
                    message_count=len(items),
                )
            except asyncio.CancelledError:
                await self._settle_safely(
                    memory, archive_id, agent_id, "interrupted", "cancelled_at_commit"
                )
                raise
            except Exception as exc:  # noqa: BLE001
                committed = False
                logger.warning("chat archive commit failed: %s", type(exc).__name__)
            if not committed:
                self._counters["archive_failed"] += 1
                await self._settle_safely(
                    memory, archive_id, agent_id, "failed", "commit_rejected"
                )
                return None
            self._counters["archive_completed"] += 1
            return {"archive_id": archive_id, "from_seq": plan["from_seq"],
                    "to_seq": to_seq, "messages": len(items)}

    # ------------------------------------------------------------------ #
    # L2 独立交互归档（M03）
    # ------------------------------------------------------------------ #
    async def maybe_archive_l2(self, *, agent_id: str, conversation_id: str,
                               memory, provider, force: bool = False
                               ) -> Optional[Dict[str, Any]]:
        """未归档交互达到阈值时归档最旧的一批，并累积整理"此前已确认前情"。

        与原始聊天归档（`maybe_archive`）独立：条数按**交互**计，边界按轮次对齐；
        批次、摘要与覆盖标记在**同一事务**提交，失败/中断不推进覆盖。
        """

        if provider is None:
            return None
        lock = self._lock(f"l2:{agent_id}:{conversation_id}")
        if lock.locked():
            return None
        config = await memory.l2_config_get(
            agent_id=agent_id, conversation_id=conversation_id
        )
        if config.get("enabled") is False:
            return None
        trigger = int(config.get("trigger_count") or self._settings.memory.chat_l2_trigger)
        retain = int(config.get("retain_count") or self._settings.memory.chat_l2_retain)
        if not 1 <= retain < trigger:
            logger.warning("L2 配置非法 cid=%s trigger=%s retain=%s",
                           conversation_id, trigger, retain)
            return None
        batch = max(1, trigger - retain)
        async with lock:
            try:
                # ① 先读"此前已确认前情"（真实读入口，可被注入替换）：
                # 读取失败 = 归档中止（不调模型、不建批次）。
                previous = await memory.l2_latest_summary(
                    agent_id=agent_id, conversation_id=conversation_id
                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                self._counters["l2_failed"] += 1
                logger.warning(
                    "L2 读取前情失败 cid=%s error=%s",
                    conversation_id, type(exc).__name__,
                )
                return None
            try:
                # ② 计划批次，并在同一事务快照里复核前情版本未被并发推进。
                plan = await memory.l2_archive_plan(
                    agent_id=agent_id, conversation_id=conversation_id,
                    trigger=trigger, batch=batch, retain=retain,
                    char_trigger=int(self._settings.memory.chat_l2_char_budget),
                    expected_previous_archive_id=str(
                        (previous or {}).get("archive_id") or ""
                    ),
                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                self._counters["l2_failed"] += 1
                logger.warning(
                    "L2 归档计划失败 cid=%s error=%s", conversation_id, type(exc).__name__
                )
                return None
            if plan is None:
                return None
            if plan.get("existing"):
                # 已完成的批次不再重复摘要；失败/中断且仍在退避内则保持原样。
                if str(plan.get("state")) != "queued":
                    return plan
            archive_id = str(plan["archive_id"])
            if not await memory.l2_archive_claim(
                archive_id=archive_id, agent_id=agent_id
            ):
                return None
            self._counters["l2_runs"] += 1
            kept: List[Dict[str, Any]] = []
            lines: List[str] = []
            used = 0
            for item in list(plan.get("items") or []):
                who = "大肥鱼" if item.get("role") == "assistant" else (
                    str(item.get("display_name") or item.get("sender_ref") or "成员")
                )
                line = f"[{str(item.get('occurred_at') or '')[:16]}] {who}：{item.get('text') or ''}"
                if kept and used + len(line) + 1 > _L2_ARCHIVE_INPUT_CHARS:
                    # 字符预算超限时只允许在**轮次边界**截短区间：同一条交互链
                    # （用户 + 对应助手回复，同一 turn_id）要么全部进入摘要，
                    # 要么全部留在 L2 —— 覆盖永远对应完整轮次。
                    if str(item.get("turn_id") or "") != str(
                        kept[-1].get("turn_id") or ""
                    ):
                        break
                kept.append(item)
                lines.append(line)
                used += len(line) + 1
            if not kept:
                await self._settle_l2_safely(
                    memory, archive_id, agent_id, "failed", "empty_input"
                )
                return None
            previous = plan.get("previous_summary") or {}
            payload_parts = []
            if previous and previous.get("content"):
                payload_parts.append("此前已确认前情：\n" + str(previous["content"]))
            payload_parts.append("本批新增交互原文：\n" + "\n".join(lines))
            archive_facts=[]
            archive_events=[]
            # 事实与事件是两个独立开关：只关 L3 时仍然提取事件（不额外请求模型）。
            want_facts = bool(self._settings.memory.l3_enabled)
            want_events = bool(
                getattr(self._settings.memory, "events_enabled", True)
            )
            try:
                if want_facts or want_events:
                    summary, archive_facts, archive_events = (
                        await self._summarise_l2_with_facts(
                            provider, "\n\n".join(payload_parts), kept,
                            want_facts=want_facts, want_events=want_events,
                        )
                    )
                else:
                    summary = await self._summarise_l2(provider, "\n\n".join(payload_parts))
            except asyncio.CancelledError:
                await self._settle_l2_safely(
                    memory, archive_id, agent_id, "interrupted", "cancelled"
                )
                raise
            except Exception as exc:  # noqa: BLE001
                self._counters["l2_failed"] += 1
                await self._settle_l2_safely(
                    memory, archive_id, agent_id, "failed", type(exc).__name__
                )
                logger.warning("L2 archive failed cid=%s error=%s",
                               conversation_id, type(exc).__name__)
                return None
            try:
                committed = await memory.l2_archive_commit(
                    archive_id=archive_id, agent_id=agent_id,
                    conversation_id=conversation_id,
                    memory_id="lm-" + uuid.uuid4().hex,
                    content=summary,
                    interaction_count=len(kept),
                    event_ids=[str(item.get("event_id") or "") for item in kept],
                    previous_archive_id=str(previous.get("archive_id") or ""),
                    # 派生输出与摘要同事务提交：任一步失败整体回滚，不会出现
                    # "摘要已归档、L3/事件写失败且无法恢复"的窗口。
                    facts=archive_facts,
                    events=archive_events,
                )
            except asyncio.CancelledError:
                await self._settle_l2_safely(
                    memory, archive_id, agent_id, "interrupted", "cancelled_at_commit"
                )
                raise
            except ValueError as exc:
                # 计划依赖的前情版本已被并发归档推进：本次不覆盖新前情。
                self._counters["l2_failed"] += 1
                await self._settle_l2_safely(
                    memory, archive_id, agent_id, "failed", str(exc)
                )
                logger.warning("L2 归档提交被拒绝 cid=%s error=%s",
                               conversation_id, exc)
                return None
            except Exception as exc:  # noqa: BLE001
                committed = None
                logger.warning("L2 archive commit failed cid=%s error=%s",
                               conversation_id, type(exc).__name__)
            ok = bool(committed.get("ok")) if isinstance(committed, dict) else False
            if not ok:
                self._counters["l2_failed"] += 1
                await self._settle_l2_safely(
                    memory, archive_id, agent_id, "failed", "commit_rejected"
                )
                return None
            self._counters["l2_completed"] += 1
            self._counters["l2_interactions_archived"] += len(kept)
            facts_written = int(committed.get("facts_written") or 0)
            events_written = int(committed.get("events_written") or 0)
            self._counters['facts_written'] += facts_written
            self._counters['events_written'] += events_written
            self._counters['events_blocked'] += max(0, len(archive_events) - events_written)
            return {
                "facts_written":facts_written,
                "events_written":events_written,
                "archive_id": archive_id,
                "interactions": len(kept),
                "planned": len(plan.get("items") or []),
                "from_seq": int(kept[0].get("seq") or 0),
                "to_seq": int(kept[-1].get("seq") or 0),
                "retained": max(0, int(plan.get("active_before") or 0) - len(kept)),
                "summary_chars": len(summary),
            }

    async def _summarise_l2(self, provider, payload: str) -> str:
        request = ModelRequest(
            purpose=ModelPurpose.CONVERSATION_SUMMARY,
            messages=(
                ModelMessage(role=MessageRole.SYSTEM, content=L2_ARCHIVE_SYSTEM),
                ModelMessage(role=MessageRole.USER, content=payload),
            ),
            tools=(),
        )
        result = await asyncio.wait_for(
            provider.complete(request),
            timeout=float(self._settings.archive.timeout_seconds),
        )
        self._counters["llm_calls"] += 1
        parsed = json.loads(str(getattr(result, "text", "") or "{}"))
        text = str(parsed.get("summary") or "").strip()[:_L2_SUMMARY_CHARS]
        if not text:
            raise ValueError("empty_summary")
        return text

    async def _summarise_l2_with_facts(self,provider,payload,items,
                                       want_facts: bool = True,
                                       want_events: bool = True):
        """One paid request per archive; member evidence and summary stay separate.

        `want_facts` / `want_events` 由两个独立配置决定：关闭 L3 不影响事件，
        关闭事件也不影响事实；被关闭的那类输出直接丢弃，不落库。
        """
        sources={str(item.get('event_id')):item for item in items
                 if item.get('role')=='member' and item.get('kind','text')=='text'
                 and str(item.get('text') or '').strip() and not str(item.get('text')).lstrip().startswith('[')}
        policy=(L2_ARCHIVE_SYSTEM+'\n同时只从本批成员原话中筛选最多3条值得长期记住的人物事实。'
            '只保留明确的称呼、稳定偏好、个人背景、长期习惯，空数组完全正常。'
            '不保存临时动作、睡觉/熬夜/情绪/当前状态、发送图片文件、单次游戏、寒暄、玩笑、第三人转述或助手说的话。'
            '不得把前情摘要当作新证据；每条只能归属于该条原文说话者本人。'
            '输出JSON：{"summary":"...","facts":[{"source_event_id":"本批原话事件ID",'
            '"category":"name|stable_preference|personal_background|long_term_habit","content":"清楚独立的长期事实短句",'
            '"quote":"原话逐字片段"}],'
            '"events":[{"source_event_id":"本批原话事件ID","title":"20字内",'
            '"detail":"100字内","status":"planned|ongoing|done",'
            '"quote":"该条原话逐字片段","occurred_at":"原文明确日期或空"}]}。'
            '事件最多2条，可为空数组；宁缺不凑。只登记可复述的约定、安排、截止、活动；'
            '不把寒暄、玩笑、偏好、发送图片文件、单次游戏、投递状态或助手自述当事件。'
            '没有逐字原话证据就不要输出事件；日期不确定就留空，不得编造日期。')
        request=ModelRequest(purpose=ModelPurpose.CONVERSATION_SUMMARY,messages=(
            ModelMessage(role=MessageRole.SYSTEM,content=policy),
            ModelMessage(role=MessageRole.USER,content=json.dumps({'context':payload,'eligible_member_source_ids':list(sources)},ensure_ascii=False))),tools=())
        self._counters['llm_calls']+=1
        result=await asyncio.wait_for(provider.complete(request),timeout=float(self._settings.archive.timeout_seconds))
        if getattr(result,'finish_reason','stop')!='stop' or getattr(result,'tool_calls',()):raise ValueError('archive_response_incomplete')
        data=json.loads(result.text or '{}');summary=str(data.get('summary') or '').strip()[:_L2_SUMMARY_CHARS]
        if not summary:raise ValueError('empty_summary')
        rows=data.get('facts',[]);facts=[]
        transient=re.compile(r'发送.{0,8}(图片|照片|文件|表情)|发了.{0,6}(图|文件|表情)|睡了|睡觉|熬夜|晚安|早安|现在|刚刚|今天|今晚|昨天|心情|困了|饿了|\[.*(?:图片|表情|文件)')
        categories={'name','stable_preference','personal_background','long_term_habit'}
        if isinstance(rows,list) and want_facts:
            for row in rows[:3]:
                if not isinstance(row,dict):continue
                source=sources.get(str(row.get('source_event_id') or ''))
                content=str(row.get('content') or '').strip();quote=str(row.get('quote') or '').strip()
                if not source or row.get('category') not in categories or not 4<=len(content)<=160 or not 4<=len(quote)<=160:continue
                if quote not in source['text'] or transient.search(content+' '+quote):continue
                if any(item['content']==content for item in facts):continue
                facts.append({'source_event_id':str(source['event_id']),'content':content,'quote':quote,'category':row['category']})
        events=[]
        raw_events=data.get('events',[])
        if isinstance(raw_events,list) and want_events:
            for row in raw_events[:2]:
                if not isinstance(row,dict):continue
                source=sources.get(str(row.get('source_event_id') or ''))
                if not source:continue
                title=str(row.get('title') or '').strip()
                detail=str(row.get('detail') or '').strip()
                quote=str(row.get('quote') or '').strip()
                status=str(row.get('status') or 'ongoing').lower()
                if not 4<=len(title)<=40 or not 4<=len(detail)<=200:continue
                # 证据：逐字 quote 必须出现在本条来源原话里。来源 ID 合法只证明引用
                # 存在，不能证明 detail 为真；没有 quote 的事件一律不接受。
                if not 4<=len(quote)<=160 or quote not in str(source.get('text') or ''):
                    self._counters['events_blocked']+=1
                    continue
                # 类别边界：偏好、寒暄、媒体行为不是事件；只有可复述的安排/约定/活动。
                if _is_non_event_wording(title+' '+detail):
                    self._counters['events_blocked']+=1
                    continue
                if not _has_event_cue(title+' '+detail+' '+quote):
                    self._counters['events_blocked']+=1
                    continue
                # 标题与证据至少要有实际重合，防止"引用一句无关原话 + 另编一件事"。
                if sum(1 for char in set(title) if char.isalnum() and char in quote)<2:
                    self._counters['events_blocked']+=1
                    continue
                source_text = str(source.get('text') or '')
                evidence = quote + source_text
                # 标题不能新增证据里没有的字（地点/人物/人数被换掉就回退到原话片段）。
                if not _title_supported(title, evidence):
                    title = (quote or title)[:20]
                    self._counters['events_title_reduced'] += 1
                # detail 必须是证据能逐字复述的内容；任何新增的关键内容都不放行，
                # 直接回退到逐字 quote，而不是"大部分相似就算通过"。
                if not _detail_supported(detail, evidence):
                    detail = (quote or detail)[:200]
                    self._counters['events_detail_reduced'] += 1
                if status not in {'planned','ongoing','done'}:status='ongoing'
                # 时间以**原话**为准；模型给的日期只有在引用/原文里有字面依据时才采信。
                occurred_at, planned_at = _normalise_event_time(
                    quote or detail, str(source.get('occurred_at') or ''),
                )
                if not (occurred_at or planned_at):
                    model_time = str(row.get('occurred_at') or '') or str(
                        row.get('planned_at') or ''
                    )
                    if model_time and _date_literal_supported(
                        model_time, quote + source_text
                    ):
                        occurred_at, planned_at = _normalise_event_time(
                            model_time, str(source.get('occurred_at') or ''),
                        )
                if any(item['title']==title for item in events):continue
                events.append({'source_event_id':str(source['event_id']),'title':title,
                               'detail':detail,'status':status,'quote':quote,
                               'occurred_at':occurred_at,'planned_at':planned_at})
        return summary,facts,events

    async def _settle_l2_safely(self, memory, archive_id: str, agent_id: str,
                                state: str, error_code: str) -> None:
        try:
            await asyncio.shield(memory.l2_archive_settle(
                archive_id=archive_id, agent_id=agent_id,
                state=state, error_code=error_code,
            ))
        except Exception:  # noqa: BLE001
            logger.warning("L2 archive settle failed archive=%s", archive_id)

    async def _summarise(self, provider, payload: str) -> str:
        request = ModelRequest(
            purpose=ModelPurpose.CONVERSATION_SUMMARY,
            messages=(
                ModelMessage(role=MessageRole.SYSTEM, content=ARCHIVE_SYSTEM),
                ModelMessage(role=MessageRole.USER, content=payload),
            ),
            tools=(),
        )
        result = await asyncio.wait_for(
            provider.complete(request),
            timeout=float(self._settings.archive.timeout_seconds),
        )
        self._counters["llm_calls"] += 1
        parsed = json.loads(str(getattr(result, "text", "") or "{}"))
        text = str(parsed.get("summary") or "").strip()[:800]
        if not text:
            raise ValueError("empty_summary")
        return text

    async def _settle_safely(self, memory, archive_id: str, agent_id: str,
                             state: str, error_code: str) -> None:
        try:
            await asyncio.shield(memory.chat_archive_settle(
                archive_id=archive_id, agent_id=agent_id,
                state=state, error_code=error_code,
            ))
        except Exception:  # noqa: BLE001
            logger.warning("chat archive settle failed archive=%s", archive_id)

    # ------------------------------------------------------------------ #
    # 事实（L3）
    # ------------------------------------------------------------------ #

    async def scan_facts(self, **kwargs) -> int:
        """Compatibility entry: independent L3 scans are disabled; archive only."""
        return 0

    # ------------------------------------------------------------------ #
    # 事件（独立游标，不依赖事实是否写入）
    # ------------------------------------------------------------------ #

    async def scan_events(self, *, agent_id: str, conversation_id: str, memory,
                          provider, force: bool = False) -> int:
        memory_settings = self._settings.memory
        # 事件跟随 L3 开关（避免关闭后台时仍产生模型请求），但**独立游标**。
        if not memory_settings.l3_enabled or provider is None:
            return 0
        lock = self._lock(f"event:{agent_id}:{conversation_id}")
        if lock.locked():
            return 0
        async with lock:
            cursor = await memory.chat_cursor_get(
                agent_id=agent_id, conversation_id=conversation_id, kind="event"
            )
            counts = await memory.chat_counts(
                agent_id=agent_id, conversation_id=conversation_id
            )
            pending = int(counts["last_seq"]) - cursor
            if pending <= 0:
                return 0
            if not force and pending < max(1, int(memory_settings.l3_scan_every_messages)):
                return 0
            items = await memory.chat_after(
                agent_id=agent_id, conversation_id=conversation_id,
                after_seq=cursor, limit=min(_EVENT_MAX_ITEMS, pending),
            )
            if not items:
                return 0
            self._counters["events_scans"] += 1
            lines = [
                f"[{str(item.get('occurred_at') or '')[:16]}] "
                f"{item.get('display_name') or item.get('sender_ref') or '成员'}：{item.get('text') or ''}"
                for item in items[:_EVENT_MAX_ITEMS]
            ]
            try:
                parsed = await self._event_complete(provider, "\n".join(lines))
                if not isinstance(parsed,dict) or not isinstance(parsed.get('events'),list):raise ValueError('invalid_events_response')
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                self._counters["events_failed"] += 1
                self._last_error["events"] = type(exc).__name__
                logger.warning("chat events failed: %s", type(exc).__name__)
                # 失败不推进：下一轮重试同一段，不会被事实游标越过。
                return 0
            events = parsed.get("events") if isinstance(parsed, dict) else []
            events = events if isinstance(events, list) else []
            batch_names = {
                str(item.get("display_name") or "") for item in items
                if item.get("display_name")
            }
            batch_dates = {
                str(item.get("occurred_at") or "")[:10] for item in items
            }
            # 参与者按稳定身份保存（显示名只用于本批匹配，不靠同名合并）。
            identity_by_name: Dict[str, str] = {}
            for item in items[:_EVENT_MAX_ITEMS]:
                name = str(item.get("display_name") or "")
                if not name:
                    continue
                record_id = str(item.get("source_record_id") or "")
                identity = (
                    await memory.chat_identity_for_record(
                        agent_id=agent_id, conversation_id=conversation_id,
                        record_id=record_id,
                    ) if record_id else ""
                )
                if name in identity_by_name and identity_by_name[name]!=identity:identity_by_name[name]=''
                elif name not in identity_by_name:identity_by_name[name]=identity
            seqs = [int(item.get("seq") or 0) for item in items[:_EVENT_MAX_ITEMS]]
            written = 0
            for event in events[:_EVENT_MAX_EVENTS]:
                if not isinstance(event, dict):
                    continue
                title = str(event.get("title") or "").strip()
                detail = str(event.get("detail") or "").strip()
                if not title or not detail:
                    continue
                status = str(event.get("status") or "ongoing").lower()
                if status not in {"ongoing", "done", "planned", "cancelled", "expired"}:
                    status = "ongoing"
                participants = event.get("participants") or []
                participants = participants if isinstance(participants, list) else []
                # 只保留本批真实出现过的显示名，不接受模型编造参与者。
                participants = [
                    str(item)[:120] for item in participants
                    if str(item) in batch_names
                ][:10]
                occurred = str(event.get("occurred_at") or "").strip()
                if occurred and occurred[:10] not in batch_dates:
                    occurred = ""
                identities = [
                    identity_by_name[name] for name in participants if identity_by_name.get(name)
                ]
                # 稳定实体：标题 + 事件日期 + 参与者身份；不同时期同名事件不合并。
                normalized = "".join(
                    char for char in title if char.isalnum()
                ).lower()[:80]
                date_bucket = occurred[:10] or min(batch_dates or {""})
                dedupe = hashlib.sha256(
                    f"{conversation_id}\x1fevent\x1f{normalized}\x1f{date_bucket}\x1f"
                    f"{','.join(sorted(identities))}".encode()
                ).hexdigest()[:32]
                await memory.memory_event_upsert(
                    agent_id=agent_id, conversation_id=conversation_id,
                    kind="event", status=status, title=title, detail=detail,
                    participants=identities, source_seqs=seqs,
                    occurred_at=occurred,
                    planned_at=str(event.get("planned_at") or ""),
                    dedupe_key=dedupe,
                )
                written += 1
            await memory.chat_cursor_set(
                agent_id=agent_id, conversation_id=conversation_id, kind="event",
                last_seq=max(int(item.get("seq") or 0) for item in items),
            )
            self._counters["events_written"] += written
            return written

    async def _event_complete(self, provider, payload: str) -> Dict[str, Any]:
        request = ModelRequest(
            purpose=ModelPurpose.CONVERSATION_SUMMARY,
            messages=(
                ModelMessage(role=MessageRole.SYSTEM, content=EVENT_SYSTEM),
                ModelMessage(role=MessageRole.USER, content=payload),
            ),
            tools=(),
        )
        result = await asyncio.wait_for(
            provider.complete(request),
            timeout=float(self._settings.archive.timeout_seconds),
        )
        self._counters["llm_calls"] += 1
        data = json.loads(str(getattr(result, "text", "") or "{}"))
        return data if isinstance(data, dict) else {}
