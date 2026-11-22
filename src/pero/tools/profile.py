"""`look_profile`（P15）：查询当前作用域内已建立的 L3 人物事实。

- 只读：读真实 L3 事实（来源记录 ID/证据类型/状态/时间），**不临时拼接
  原文冒充已建 L3**；没有事实就明确说没有；
- 只能在绑定的 Agent + 会话范围内工作（`tools.context`），不接受跨 Agent
  参数；对象按别名解析，歧义不选首项；
- identity/alias 不是人物事实：没有匹配身份或该身份无事实时如实说明。
"""

from __future__ import annotations

from typing import Any, List, Mapping

from pero.core.contracts import ToolSpec
from pero.memory.p15_store import FACT_STATUSES
from pero.tools.context import current_tool_context


class LookProfileTool:
    """查询当前会话可见的 L3 人物事实（只读真实存储）。"""

    ephemeral_result = True

    tool_id = "look_profile"

    def __init__(self, repository) -> None:
        self._repository = repository

    def spec(self) -> ToolSpec:
        return ToolSpec(
            name=self.tool_id,
            description=(
                "查询当前会话内已记录的人物事实（L3）：只有来源可核对的明确"
                "陈述/偏好才会在这里。没有记录时明确说明，不要推测或编造人物"
                "特点。可以用成员名字查询，也可以不带名字查看最近事实。"
            ),
            parameters={
                "type": "object",
                "properties": {
                    "name": {
                        "type": "string",
                        "description": "可选：成员名字（当前会话内的别名）。缺省时返回最近事实。",
                    },
                    "limit": {
                        "type": "integer",
                        "description": "最多返回几条（1–10，默认 5）。",
                        "minimum": 1,
                        "maximum": 10,
                    },
                },
                "additionalProperties": False,
            },
        )

    async def run(self, arguments: Mapping[str, Any]) -> Mapping[str, Any]:
        context = current_tool_context()
        if context is None or context.memory is None:
            return {
                "ok": False,
                "state": "unavailable",
                "detail": "当前轮次没有绑定记忆存储，未查询人物事实。",
                "facts": [],
            }
        if not getattr(self._repository, "available", False):
            return {
                "ok": False,
                "state": "unavailable",
                "detail": "记忆存储不可用，未查询人物事实。",
                "facts": [],
            }
        limit = min(max(int(arguments.get("limit") or 5), 1), 10)
        name = str(arguments.get("name") or "").strip()
        identity_id = ""
        ambiguous: List[str] = []
        if name:
            try:
                matches = await self._repository.match_alias_tokens(
                    agent_id=context.agent_id,
                    account_id=context.account_id,
                    conversation_id=context.conversation_id,
                    tokens=[name],
                )
                distinct = sorted({str(item["identity_id"]) for item in matches})
                if len(distinct) == 1:
                    identity_id = distinct[0]
                elif len(distinct) > 1:
                    ambiguous = distinct
            except Exception as exc:  # noqa: BLE001
                return {
                    "ok": False,
                    "state": "failed",
                    "detail": f"身份解析失败（{type(exc).__name__}）。",
                    "facts": [],
                }
            if not identity_id and not ambiguous:
                return {
                    "ok": False,
                    "state": "unknown_person",
                    "detail": "当前会话没有这个名字的已知身份（不猜测对应关系）。",
                    "facts": [],
                }
            if ambiguous:
                return {
                    "ok": False,
                    "state": "identity_ambiguous",
                    "detail": "这个名字对应多个成员，未按单人过滤；请明确对象。",
                    "identity_ids": ambiguous,
                    "facts": [],
                }
        try:
            result = await self._repository.l3_list(
                agent_id=context.agent_id,
                identity_id=identity_id,
                limit=limit,
            )
        except Exception as exc:  # noqa: BLE001
            return {
                "ok": False,
                "state": "failed",
                "detail": f"人物事实查询失败（{type(exc).__name__}）。",
                "facts": [],
            }
        facts = [
            {
                "fact_id": item["fact_id"],
                "identity_id": item["identity_id"],
                "display_name": item["display_name"],
                "content": item["content"],
                "status": item["status"],
                "evidence_type": item["evidence_type"],
                "source_record_ids": item["source_record_ids"],
                "updated_at": item["updated_at"],
            }
            for item in result.get("items", [])
        ]
        return {
            "ok": True,
            "state": "ok" if facts else "no_facts",
            "detail": (
                "" if facts
                else "该对象没有已记录的人物事实；只有来源可核对的明确陈述/偏好会进入 L3。"
            ),
            "facts": facts,
        }


__all__ = ["LookProfileTool"]
