"""记忆工具：`remember`（明确保存授权确认）与 `recall`（回答前/临时检索）。

约束：
- 两个工具都**只能**在绑定的 Agent + 会话范围内工作，靠 `tools.context` 的
  `ContextVar` 取运行期上下文，不读全局库、不接受跨 Agent 参数；
- `remember` 只消费编排识别出的**可信保存授权**（`ToolContext.save_grants`）：
  必须给出本轮真实原文的 `record_id`；无授权直接拒绝，**不把模型参数当用户请求**。
  授权在生成回答之前已由系统保存，因此本工具**不再写入**新事实：
  已保存 → 幂等返回同一真实记录 ID（改写 content 不生效）；被阻断（需要补充/对象
  歧义/超长/失败）→ 原样返回真实状态，不绕过任何一道关；
- `recall` 返回带层、记录 ID、时间与来源的命中，区分没命中/身份歧义/不可用/失败；
- 工具结果只含受控摘要与引用，结果正文由执行器按白名单摘要，不整段落库。
"""

from __future__ import annotations

from typing import Any, List, Mapping

from pero.core.contracts import ToolSpec
from pero.memory.models import (
    LAYER_JOURNAL,
    LAYER_L4,
    LAYER_RAW,
    SAVE_DUPLICATE,
    SAVE_FAILED,
    SAVE_SAVED,
    SAVE_UNAUTHORIZED,
)
from pero.tools.context import SaveGrant, current_tool_context


class RememberTool:
    """确认/复述用户明确要求保存的事实（授权来自真实原文，不由模型参数发起）。"""

    tool_id = "remember"

    def spec(self) -> ToolSpec:
        return ToolSpec(
            name=self.tool_id,
            description=(
                "确认用户明确要求记住的事实已保存。只能对系统注记[保存凭据]里给出的"
                "「来源原文 ID」调用：系统已在生成回答前保存该请求，此工具返回其"
                "真实记录 ID 与状态。没有对应来源 ID 时不要调用；它不能用来保存"
                "用户没有明确要求记住的内容。"
            ),
            parameters={
                "type": "object",
                "properties": {
                    "record_id": {
                        "type": "string",
                        "description": "保存凭据里的来源原文 ID（以 m- 开头）。",
                    },
                    "content": {
                        "type": "string",
                        "description": (
                            "可选：对要保存事实的复述，仅用于比对，"
                            "不会作为新的保存内容写入。"
                        ),
                    },
                },
                "required": ["record_id"],
                "additionalProperties": False,
            },
        )

    async def run(self, arguments: Mapping[str, Any]) -> Mapping[str, Any]:
        context = current_tool_context()
        if context is None or context.memory is None:
            return {
                "ok": False,
                "status": SAVE_FAILED,
                "detail": "当前轮次没有绑定记忆存储，未保存任何内容。",
            }
        memory = context.memory
        if not getattr(memory, "available", False):
            return {
                "ok": False,
                "status": SAVE_FAILED,
                "detail": "记忆存储不可用，未保存任何内容。",
            }

        record_id = str(arguments.get("record_id") or "").strip()
        grants = context.save_grants or {}
        if not record_id or record_id not in grants:
            return {
                "ok": False,
                "status": SAVE_UNAUTHORIZED,
                "detail": (
                    "本轮没有该来源的保存授权：只有用户明确要求保存的原文才能写入长期记忆，"
                    "模型参数不能替代用户授权。"
                ),
                "authorized_records": sorted(grants),
            }
        grant: SaveGrant = grants[record_id]

        # 已保存的授权：幂等返回同一真实记录 ID；模型改写的 content 不生效。
        if grant.status in {SAVE_SAVED, SAVE_DUPLICATE} and grant.memory_id:
            claimed = " ".join(str(arguments.get("content") or "").split())
            content_ignored = bool(claimed) and claimed != grant.content
            return {
                "ok": True,
                "status": SAVE_DUPLICATE,
                "memory_id": grant.memory_id,
                "content": grant.content,
                "subject": grant.subject_label,
                "subject_identity_id": grant.subject_identity_id,
                "sources": [grant.record_id],
                "turn_id": context.turn_id,
                "content_ignored": content_ignored,
                "detail": (
                    "该请求已由系统在生成回答前保存，返回原记录 ID；"
                    + ("模型复述与原内容不一致，已忽略，未写入新内容。"
                       if content_ignored else "未写入新内容。")
                ),
            }

        # 被阻断/失败的授权：原样返回真实状态，不绕过对象歧义等任何一道关。
        return {
            "ok": False,
            "status": grant.status,
            "memory_id": None,
            "subject": grant.subject_label,
            "subject_identity_id": grant.subject_identity_id,
            "sources": [grant.record_id],
            "turn_id": context.turn_id,
            "detail": grant.detail or "该保存请求未被接受，本次没有产生长期记录。",
        }


class RecallTool:
    """在当前 Agent/会话范围内检索 L4、事件日记与原文来源。"""

    ephemeral_result = True

    tool_id = "recall"

    def spec(self) -> ToolSpec:
        return ToolSpec(
            name=self.tool_id,
            description=(
                "检索本会话可见的长期记忆：用户明确保存的记录、事件日记与原文来源。"
                "返回带层、记录 ID、时间与来源的命中；没命中会明确说明，不要当成"
                "「用户从未说过」。"
            ),
            parameters={
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "关键词或短句（中文按二元组切分，英文按词切分）。",
                    },
                    "limit": {
                        "type": "integer",
                        "description": "最多返回几条（1–10，默认 5）。",
                        "minimum": 1,
                        "maximum": 10,
                    },
                },
                "required": ["query"],
                "additionalProperties": False,
            },
        )

    async def run(self, arguments: Mapping[str, Any]) -> Mapping[str, Any]:
        context = current_tool_context()
        if context is None or context.memory is None:
            return {
                "ok": False,
                "state": "unavailable",
                "detail": "当前轮次没有绑定记忆存储，未执行检索。",
                "hits": [],
            }
        query = " ".join(str(arguments.get("query") or "").split())
        if not query:
            return {
                "ok": False,
                "state": "no_hits",
                "detail": "没有关键词，未执行检索。",
                "hits": [],
            }
        limit = min(max(int(arguments.get("limit") or 5), 1), 10)

        retriever = context.retriever
        try:
            if retriever is not None:
                result = await retriever.retrieve(
                    agent_id=context.agent_id,
                    conversation_id=context.conversation_id,
                    account_id=context.account_id,
                    query=query,
                    limit=limit,
                )
                state = result.state
                detail = result.detail
                hits: List[Mapping[str, Any]] = [
                    {
                        "layer": item.layer,
                        "record_id": item.record_id,
                        "occurred_at": item.occurred_at,
                        "preview": item.preview,
                        "source_refs": list(item.source_refs),
                    }
                    for item in result.hits
                ]
            else:
                items, detail = await context.memory.search(
                    agent_id=context.agent_id,
                    conversation_id=context.conversation_id,
                    query=query,
                    layers=(LAYER_L4, LAYER_JOURNAL, LAYER_RAW),
                    limit=limit,
                )
                state = "ok" if items else "no_hits"
                hits = [
                    {
                        "layer": item.layer,
                        "record_id": item.record_id,
                        "occurred_at": item.occurred_at,
                        "preview": item.preview,
                        "source_refs": list(item.source_refs),
                    }
                    for item in items
                ]
        except Exception as exc:  # noqa: BLE001 - 检索失败不能伪装成没命中
            return {
                "ok": False,
                "state": "failed",
                "detail": f"检索失败（{type(exc).__name__}），本轮没有可用结果。",
                "hits": [],
            }

        return {
            "ok": state in ("ok", "identity_ambiguous"),
            "state": state,
            "query": query,
            "detail": detail,
            "hits": hits,
        }


__all__ = ["RecallTool", "RememberTool"]
