"""首批真实工具：只有本地无副作用的 `get_current_time`。

时间语义：**默认与唯一主结果都是北京时间**（Asia/Shanghai，UTC+08:00）。
它不是天气服务，也不代表用户日程；只是当前时刻，不推测用户所在地。
旧字段 `utc_iso` / `utc_date` 保留真实 UTC 值并标记为兼容字段，
不把 `+08:00` 塞进名为 utc 的字段。
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Mapping

from pero.core.contracts import ToolSpec

#: 周几的中文写法（北京时间日历）。
_WEEKDAYS = ("周一", "周二", "周三", "周四", "周五", "周六", "周日")


class GetCurrentTimeTool:
    """返回当前北京时间（ISO 8601，带 +08:00）。无参数、无副作用。"""

    tool_id = "get_current_time"

    def spec(self) -> ToolSpec:
        return ToolSpec(
            name=self.tool_id,
            description=(
                "返回当前**北京时间**（Asia/Shanghai，UTC+08:00）的 ISO 8601 时间、"
                "日期与星期；同时给出真实 UTC 作为兼容字段。无参数、只读，"
                "不查询用户本地时区、天气或日程。"
            ),
            parameters={
                "type": "object",
                "properties": {},
                "additionalProperties": False,
            },
        )

    async def run(self, arguments: Mapping[str, Any]) -> Mapping[str, Any]:
        from pero.core.timebase import now as bj_now

        local = bj_now()
        utc = local.astimezone(timezone.utc)
        return {
            # 主结果：北京时间
            "beijing_iso": local.isoformat(timespec="seconds"),
            "beijing_date": local.strftime("%Y-%m-%d"),
            "weekday": _WEEKDAYS[local.weekday()],
            "timezone": "Asia/Shanghai",
            "utc_offset": "+08:00",
            "clock": "beijing",
            # 兼容字段：真实 UTC（不是北京时间的冒充）
            "utc_iso": utc.isoformat(timespec="seconds"),
            "utc_date": utc.strftime("%Y-%m-%d"),
            "utc_compat": True,
        }
