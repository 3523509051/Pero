"""首批真实工具：只有本地无副作用的 `get_current_time`。

它不是天气服务，也不代表用户日程/时区换算；只返回服务进程的 UTC 时间。
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Mapping

from pero.core.contracts import ToolSpec


class GetCurrentTimeTool:
    """返回当前 UTC 时间（ISO 8601）。无参数、无副作用。"""

    tool_id = "get_current_time"

    def spec(self) -> ToolSpec:
        return ToolSpec(
            name=self.tool_id,
            description=(
                "返回当前 UTC 时间，ISO 8601 格式。无参数、只读，不查询用户本地时区、"
                "天气或日程。"
            ),
            parameters={
                "type": "object",
                "properties": {},
                "additionalProperties": False,
            },
        )

    async def run(self, arguments: Mapping[str, Any]) -> Mapping[str, Any]:
        now = datetime.now(timezone.utc)
        return {
            "utc_iso": now.isoformat(),
            "utc_date": now.strftime("%Y-%m-%d"),
            "clock": "server_utc",
        }
