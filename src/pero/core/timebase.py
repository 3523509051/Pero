"""统一时间语义：所有业务时间以**北京时间**（Asia/Shanghai，UTC+08:00）为准。

边界：
- 新生成的绝对业务时间一律是 aware datetime，ISO8601 明确带 `+08:00`；
- Unix 时间戳仍表示同一绝对时刻，**不加**八小时；计时/耗时/超时/TTL 继续用
  monotonic 或秒数，不改成北京时间日历钟；
- 外部来源（API/微信/插件）带 `Z` 或其它 offset 的时间按原 offset 解析，
  再转北京时间用于比较与展示，不改写来源原文或实际事件时刻；
- 只有日期、视频相对秒数、时长这类"不是绝对时间"的字段保留原类型；
- Windows 缺 tzdata 时显式回退固定 UTC+8（中国无夏令时），不依赖机器本地时区。

数值时间的**约定**：后端只把 int/float 当 **Unix 秒**；毫秒级数值（如
`1795449600000`）超出可表示范围，一律返回 None，不做"猜单位"，调用方要
毫秒请先自己除以 1000。前端 `fmtTime` 另按 1e12 阈值区分秒/毫秒（见
`frontend/src/time.ts`），两侧不要互相假设。
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Any, Optional

#: 北京时间固定偏移（无夏令时）。
BEIJING_OFFSET = timedelta(hours=8)
#: 展示用的时区名。
BEIJING_NAME = "Asia/Shanghai"


#: 无时区（naive）历史值的**来源证据**（逐字段，不做"全库统一加 8 小时"）。
#: 依据是写入代码本身：R02 之前 `chat_stream` 用
#: `datetime.now(_user_tz()).replace(tzinfo=None)` 写这些列，`USER_TZ` 就是
#: Asia/Shanghai，所以这些列的无时区值确实是北京时间；其它来源不明的字段
#: 只按北京时间解释**用于查询**，不改写原值、不做批量迁移。
NAIVE_PROVENANCE = {
    "chat_event.occurred_at": "北京时间（R02 前由 _user_tz() 写入）",
    "l2_turn.created_at": "北京时间（同上）",
    "chat_task.next_allowed_at": "北京时间（同上）",
    "l2_turn.finished_at": "北京时间（同上）",
    "其它未识别列": "未知：按北京时间解释用于查询，原文保留不改写",
}


def _fallback() -> timezone:
    return timezone(BEIJING_OFFSET, BEIJING_NAME)


def beijing_tz():
    """返回北京时间 tzinfo；缺 tzdata 时用固定 +08:00。"""

    try:
        from zoneinfo import ZoneInfo

        return ZoneInfo(BEIJING_NAME)
    except Exception:  # noqa: BLE001 - 老 Windows 没有 tzdata，不能因此崩溃
        return _fallback()


TZ = beijing_tz()


def now() -> datetime:
    """当前北京时间（aware）。"""

    return datetime.now(TZ)


def now_iso(*, timespec: str = "seconds") -> str:
    """当前北京时间的 ISO8601 字符串（明确带 +08:00）。"""

    return now().isoformat(timespec=timespec)


def today() -> date:
    """北京时间的"今天"（跨日按北京时区，不按服务器时区）。"""

    return now().date()


def parse(value: Any) -> Optional[datetime]:
    """解析成 aware datetime；无时区的旧值按北京时间解释。

    - 带 `Z` / offset：按原 offset 解析；
    - 无时区字符串：视为北京时间（历史数据是本地写入的日历时间）；
    - Unix 时间戳（int/float）：按绝对时刻换算，不做偏移；
    - 解析失败返回 None（调用方必须显式处理，不能当"现在"）。
    """

    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=TZ)
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(float(value), TZ)
        except (OverflowError, OSError, ValueError):
            return None
    text = str(value).strip()
    if not text:
        return None
    if text.isdigit() and len(text) >= 10:
        try:
            return datetime.fromtimestamp(float(text), TZ)
        except (OverflowError, OSError, ValueError):
            return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=TZ)
    return parsed


def to_beijing(value: Any) -> Optional[datetime]:
    """把任意时间表示转成北京时间；解析失败返回 None（不猜）。"""

    parsed = parse(value)
    return parsed.astimezone(TZ) if parsed is not None else None


def format_iso(value: Any, *, timespec: str = "seconds") -> str:
    """把时间格式化成北京时间 ISO；无法解析返回空串（调用方显示占位）。"""

    converted = to_beijing(value)
    return converted.isoformat(timespec=timespec) if converted is not None else ""


def display(value: Any, *, with_seconds: bool = True) -> str:
    """人读展示：`2026-11-23 15:00:00`（北京时间）；未知返回"未知时间"。"""

    converted = to_beijing(value)
    if converted is None:
        return "未知时间"
    pattern = "%Y-%m-%d %H:%M:%S" if with_seconds else "%Y-%m-%d %H:%M"
    return converted.strftime(pattern)


def epoch(value: Any) -> Optional[float]:
    """绝对时刻（Unix 秒，可含小数）；无法解析返回 None。

    与 `sort_key` 的区别：None 就是"未知"，不用 `-inf` 代替，便于 SQL 里
    当 NULL 处理（NULL 在 `ORDER BY ... DESC` 排最后，不会被当成最早）。
    """

    converted = to_beijing(value)
    return converted.timestamp() if converted is not None else None


def require_epoch(value: Any, label: str = "") -> float:
    """把查询边界转成绝对秒；解析不出来就报错，不静默放开或收紧范围。"""

    moment = epoch(value)
    if moment is None:
        raise ValueError(
            f"时间范围无法解析（{label}）" if label else "时间范围无法解析"
        )
    return moment


def sql_epoch(value: Any) -> Optional[float]:
    """SQLite 自定义函数 `bj_epoch(ts)` 的实现：ISO 字符串 → 绝对秒。

    旧库混用 `Z` / `+00:00` / `+08:00` / 无时区，按字符串比大小会把
    "北京 11-24 06:00"（存的 `2026-11-23T22:00:00+00:00`）排到 11-23，
    跨日筛选直接漏召回。改用绝对时刻后，日期范围与排序都不再依赖文本形态。
    """

    if value is None or value == "":
        return None
    return epoch(value)


def register_sql(connection, name: str = "bj_epoch") -> None:
    """在 SQLite 连接上注册 `bj_epoch`（deterministic，可用于表达式索引）。"""

    connection.create_function(name, 1, sql_epoch, deterministic=True)


def sort_key(value: Any) -> float:
    """排序/过期比较用的绝对时间戳；无法解析给 `-inf`（排最前，不冒充现在）。"""

    converted = to_beijing(value)
    if converted is None:
        return float("-inf")
    return converted.timestamp()


def later_than(left: Any, right: Any) -> bool:
    """`left` 是否晚于 `right`（按绝对时刻，不做字符串字典序比较）。"""

    return sort_key(left) > sort_key(right)


def day_bounds(value: Any = None) -> tuple[datetime, datetime]:
    """北京时间某一天的 [00:00, 次日 00:00)。"""

    base = to_beijing(value) or now()
    start = base.replace(hour=0, minute=0, second=0, microsecond=0)
    return start, start + timedelta(days=1)


def date_only(value: Any) -> str:
    """只保留北京时间的日期部分；解析失败返回空串。"""

    converted = to_beijing(value)
    return converted.date().isoformat() if converted is not None else ""


__all__ = [
    "BEIJING_NAME",
    "BEIJING_OFFSET",
    "NAIVE_PROVENANCE",
    "TZ",
    "beijing_tz",
    "date_only",
    "day_bounds",
    "display",
    "epoch",
    "format_iso",
    "later_than",
    "now",
    "now_iso",
    "parse",
    "register_sql",
    "require_epoch",
    "sort_key",
    "sql_epoch",
    "to_beijing",
    "today",
]
