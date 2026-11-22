"""统一模型请求出口：默认关闭思考，认证配置可开启主回复／工具分析。

模式由应用解析后交给构造器，禁止通过 parameters 覆盖 thinking/model
或使用 reasoning_effort/extra_body 绕过策略。HTTP 与 SDK 导出均校验
捕获的模式；工具思考的 reasoning_content 只在当前轮协议中使用。
"""

from __future__ import annotations

import copy
import logging
import uuid
from datetime import datetime, timezone
from types import MappingProxyType
from typing import Any, Dict, List, Mapping, Sequence

from pero.core.contracts import (
    MessageRole,
    ModelMessage,
    ModelPurpose,
    ModelRequest,
    ModelToolCall,
    PURPOSE_DESCRIPTIONS,
    PURPOSE_TOOLS_ALLOWED,
    ToolSpec,
)

logger = logging.getLogger(__name__)

PROTOCOL = "chat.completions"
DEFAULT_MODEL = "deepseek-flash"

#: 最终 HTTP JSON 中的思考字段取值。只读视图，防止被就地改写。
THINKING_DISABLED: Mapping[str, str] = MappingProxyType({"type": "disabled"})

#: 明确禁止调用方传入的键：任何可能开启或替代关闭思考的旁路。
FORBIDDEN_PARAMETER_KEYS = frozenset(
    {
        "thinking",
        "reasoning",
        "reasoning_effort",
        "reasoning_content",
        "enable_thinking",
        "chat_template_kwargs",
        "extra_body",
        "extra_headers",
        "model",
        "messages",
        "stream",
    }
)

#: 允许透传的普通生成参数白名单（与思考开关无关）。
ALLOWED_PARAMETER_KEYS = frozenset(
    {
        "temperature",
        "top_p",
        "max_tokens",
        "tool_choice",
        "response_format",
        "stop",
        "seed",
        "frequency_penalty",
        "presence_penalty",
    }
)

_ALLOWED_ROLES = frozenset(role.value for role in MessageRole)

#: 出站正文允许出现的顶层键。任何其他键（含 stream、reasoning_effort 等）一律拒绝，
#: 避免后续传输层合并参数时重新引入"开启思考"或未审阅字段。
_ALLOWED_BODY_KEYS = frozenset(
    {
        "model",
        "messages",
        "thinking",
        "tools",
        "tool_choice",
        "temperature",
        "top_p",
        "max_tokens",
        "response_format",
        "stop",
        "seed",
        "frequency_penalty",
        "presence_penalty",
    }
)


class ThinkingPolicyViolation(ValueError):
    """请求模式与已解析策略不符，或试图旁路策略。"""


class RequestValidationError(ValueError):
    """请求结构不符合构造器约定。"""


def assert_thinking_policy(payload: Mapping[str, Any], expected: str = "disabled") -> None:
    """思考策略断言：必须显式 disabled，且没有旁路键。

    校验对象是“当前这份正文”，不是常量或另一份原始字典。
    """

    if not isinstance(payload, Mapping):
        raise RequestValidationError("待校验的请求正文必须是映射")
    thinking = payload.get("thinking")
    if expected not in {"enabled","disabled"} or not isinstance(thinking, Mapping) or dict(thinking) != {"type":expected}:
        raise ThinkingPolicyViolation(
            "思考模式与已解析的请求策略不一致"
        )
    for key in sorted(FORBIDDEN_PARAMETER_KEYS & set(payload)):
        if key in {"thinking", "model", "messages"}:
            continue
        raise ThinkingPolicyViolation(f"请求包含不允许的旁路参数：{key}")


def assert_thinking_disabled(payload: Mapping[str, Any]) -> None:
    assert_thinking_policy(payload, "disabled")


def validate_outbound_body(body: Any, expected_thinking: str = "disabled") -> Dict[str, Any]:
    """对“实际要导出的正文快照”做完整校验，返回同一份快照。

    导出边界（HTTP 与 SDK）都走这里，因此即使绕过 builder 直接构造请求对象，
    缺失开关、与已解析模式不符或含禁止旁路键的正文无法导出。
    """

    if not isinstance(body, dict):
        raise RequestValidationError("请求正文必须是 JSON 对象")
    # 先查策略：缺开关或 enabled/adaptive 要报政策错误，而不是笼统的结构错误。
    assert_thinking_policy(body, expected_thinking)
    unknown = sorted(set(body) - _ALLOWED_BODY_KEYS)
    if unknown:
        raise RequestValidationError("正文包含不允许的顶层字段：" + "、".join(unknown))
    model = body.get("model")
    if not isinstance(model, str) or not model.strip():
        raise RequestValidationError("正文缺少有效 model")
    messages = body.get("messages")
    if not isinstance(messages, list) or not messages:
        raise RequestValidationError("正文 messages 必须是非空数组")
    tools = body.get("tools")
    if tools is not None and (not isinstance(tools, list) or not tools):
        raise RequestValidationError("正文 tools 若存在必须是非空数组")
    return body


def _wire_tool_calls(index: int, calls: Any) -> List[Dict[str, Any]]:
    """把 assistant 的 tool_calls 转成官方结构；arguments 保持原始 JSON 字符串。"""

    if not calls:
        return []
    wire: List[Dict[str, Any]] = []
    for call in calls:
        if not isinstance(call, ModelToolCall):
            raise RequestValidationError(
                f"第 {index} 条消息的 tool_calls 每项必须是 ModelToolCall"
            )
        if not isinstance(call.call_id, str) or not call.call_id.strip():
            raise RequestValidationError(f"第 {index} 条消息存在空 tool_call_id")
        if not isinstance(call.name, str) or not call.name.strip():
            raise RequestValidationError(f"第 {index} 条消息存在空工具名")
        if not isinstance(call.arguments, str):
            raise RequestValidationError(
                f"第 {index} 条消息的 {call.name} arguments 必须是原始 JSON 字符串"
            )
        wire.append(
            {
                "id": call.call_id,
                "type": "function",
                "function": {"name": call.name, "arguments": call.arguments},
            }
        )
    return wire


class ProviderChatRequest:
    """一次已经构造完成、可以导出的 Chat Completions 请求。

    正文所有权在对象内部：构造时深拷贝并校验一次，此后对外只提供独立快照，
    因此调用方修改入参字典、公开属性返回值或导出结果，都不会改变内部有效请求。
    """

    __slots__ = ("_request_id", "_purpose", "_body", "_created_at", "_expected_thinking")

    def __init__(
        self,
        *,
        request_id: str,
        purpose: ModelPurpose,
        payload: Mapping[str, Any],
        model: str | None = None,
        protocol: str = PROTOCOL,
        created_at: str | None = None,
        expected_thinking: str = "disabled",
    ) -> None:
        if not isinstance(request_id, str) or not request_id.strip():
            raise RequestValidationError("request_id 不能为空")
        if not isinstance(purpose, ModelPurpose):
            raise RequestValidationError("purpose 必须是 core.contracts.ModelPurpose")
        if protocol != PROTOCOL:
            raise RequestValidationError(f"首版只支持协议 {PROTOCOL}")

        body = validate_outbound_body(copy.deepcopy(dict(payload)), expected_thinking)
        if model is not None and body["model"] != model:
            raise RequestValidationError("正文 model 与请求声明不一致")

        self._expected_thinking = expected_thinking
        self._request_id = request_id.strip()
        self._purpose = purpose
        self._body = body
        self._created_at = created_at or datetime.now(timezone.utc).isoformat()

    # ------------------------------------------------------------------ #
    # 只读属性
    # ------------------------------------------------------------------ #

    @property
    def request_id(self) -> str:
        return self._request_id

    @property
    def purpose(self) -> ModelPurpose:
        return self._purpose

    @property
    def model(self) -> str:
        return self._validated_snapshot()["model"]

    @property
    def protocol(self) -> str:
        return PROTOCOL

    @property
    def created_at(self) -> str:
        return self._created_at

    @property
    def thinking_policy(self) -> str:
        """实际生效的策略描述，取自校验通过的正文，而不是固定常量。"""

        return f"thinking.type={self._validated_snapshot()['thinking']['type']}"

    @property
    def payload(self) -> Dict[str, Any]:
        """只读快照副本；修改返回值不会影响内部请求。"""

        return self._validated_snapshot()

    # ------------------------------------------------------------------ #
    # 导出边界
    # ------------------------------------------------------------------ #

    def _validated_snapshot(self) -> Dict[str, Any]:
        """取内部正文的独立副本，并对这份副本做政策与结构校验。"""

        return validate_outbound_body(copy.deepcopy(self._body), self._expected_thinking)

    def http_body(self) -> Dict[str, Any]:
        """最终 HTTP JSON 正文。

        先取得独立快照，再对**这份即将返回的快照**调用 ``assert_thinking_policy()``，
        通过后才返回，因此导出结果本身也经过校验。
        """

        snapshot = self._validated_snapshot()
        assert_thinking_policy(snapshot, self._expected_thinking)
        # “导出校验通过”不等于“已发送”：本阶段没有网络客户端，不存在发送日志。
        logger.debug(
            "model_request_exported_http request_id=%s purpose=%s",
            self._request_id,
            self._purpose.value,
        )
        return snapshot

    def to_sdk_kwargs(self) -> Dict[str, Any]:
        """OpenAI 兼容 SDK 的调用参数。

        来源是已通过同一政策校验的正文快照，再把顶层 ``thinking`` 转成
        ``extra_body.thinking``。这只是 SDK 的包装层级，不是 HTTP 正文层级。
        """

        body = self.http_body()
        model = body.pop("model")
        thinking = body.pop("thinking")
        kwargs: Dict[str, Any] = {"model": model}
        kwargs.update(body)
        kwargs["extra_body"] = {"thinking": thinking}
        return kwargs

    def redacted_summary(self) -> Dict[str, Any]:
        """可写入日志的摘要：只有编号、用途与策略，没有正文和凭据。

        摘要字段取自校验通过的有效正文；正文不合规时这里会直接抛错，
        不会返回与正文不符的固定 ``disabled``。
        """

        body = self._validated_snapshot()
        tool_names: List[str] = []
        for tool in body.get("tools") or []:
            function = tool.get("function") if isinstance(tool, dict) else None
            if isinstance(function, dict) and function.get("name"):
                tool_names.append(str(function["name"]))
        parameter_keys = sorted(
            set(body) - {"model", "messages", "thinking", "tools"}
        )
        return {
            "request_id": self._request_id,
            "purpose": self._purpose.value,
            "model": body["model"],
            "protocol": PROTOCOL,
            "thinking": dict(body["thinking"]),
            "thinking_policy": f"thinking.type={body['thinking']['type']}",
            "message_count": len(body["messages"]),
            "tools": tool_names,
            "parameter_keys": parameter_keys,
            "created_at": self._created_at,
        }


class DeepSeekRequestBuilder:
    """Chat Completions 协议下唯一的请求构造器。

    ``build()`` 是主回复和全部后台辅助调用共用的入口；``purpose`` 区分用途。
    """

    provider_id = "deepseek"
    protocol = PROTOCOL

    def __init__(
        self,
        *,
        model: str = DEFAULT_MODEL,
        base_url: str = "https://api.deepseek.com/v1",
        request_timeout_seconds: float = 60.0,
        thinking_enabled: bool = False,
    ) -> None:
        if not model or not model.strip():
            raise RequestValidationError("模型名不能为空")
        if type(thinking_enabled) is not bool:raise ThinkingPolicyViolation("invalid thinking policy")
        self.expected_thinking = "enabled" if thinking_enabled else "disabled"
        self.model = model.strip()
        self.base_url = base_url.rstrip("/")
        self.request_timeout_seconds = float(request_timeout_seconds)

    # ------------------------------------------------------------------ #
    # 对外入口
    # ------------------------------------------------------------------ #

    def build(self, request: ModelRequest) -> ProviderChatRequest:
        if not isinstance(request, ModelRequest):
            raise RequestValidationError("build() 只接受 core.contracts.ModelRequest")
        return self._build(
            purpose=request.purpose,
            messages=self._messages_for_request(request),
            tools=request.tools,
            parameters=request.parameters,
        )

    def build_chat_request(
        self,
        *,
        purpose: ModelPurpose | str,
        messages: Sequence[ModelMessage],
        tools: Sequence[ToolSpec] = (),
        parameters: Mapping[str, Any] | None = None,
    ) -> ProviderChatRequest:
        return self._build(
            purpose=purpose,
            messages=messages,
            tools=tools,
            parameters=parameters or {},
        )

    # ------------------------------------------------------------------ #
    # 内部实现
    # ------------------------------------------------------------------ #

    def _build(
        self,
        *,
        purpose: ModelPurpose | str,
        messages: Sequence[ModelMessage],
        tools: Sequence[ToolSpec],
        parameters: Mapping[str, Any],
    ) -> ProviderChatRequest:
        resolved_purpose = self._resolve_purpose(purpose)
        wire_messages = self._wire_messages(messages)
        if self.expected_thinking == "disabled":
            for message in wire_messages:message.pop("reasoning_content", None)
        wire_tools = self._wire_tools(resolved_purpose, tools)
        extra = self._validated_parameters(parameters)

        payload: Dict[str, Any] = {"model": self.model, "messages": wire_messages}
        if wire_tools:
            payload["tools"] = wire_tools
        payload.update(extra)
        # 思考开关放在最后写入：即使未来追加参数，也不会被覆盖。
        payload["thinking"] = {"type":self.expected_thinking}
        assert_thinking_policy(payload, self.expected_thinking)

        request = ProviderChatRequest(
            request_id=f"{resolved_purpose.value}-{uuid.uuid4().hex[:12]}",
            purpose=resolved_purpose,
            payload=payload,
            model=self.model,
            protocol=self.protocol,
            expected_thinking=self.expected_thinking,
        )
        # 只记录编号与策略，不记录正文、附件内容或凭据。
        # 这是“构造时校验通过”日志，不代表已经发送：本阶段没有网络客户端，
        # 因此不存在 request_sent 之类的日志。
        logger.info(
            "model_request_constructed purpose=%s request_id=%s model=%s thinking=%s messages=%d tools=%d",
            request.purpose.value,
            request.request_id,
            request.model,
            self.expected_thinking,
            len(wire_messages),
            len(wire_tools),
        )
        return request

    def _messages_for_request(self, request):
        messages = tuple(request.messages)
        if self.expected_thinking != "enabled" or not request.history_prefix:
            return messages
        start=request.system_prefix
        end=start+request.history_prefix
        if start not in {0,1} or end>len(messages):raise RequestValidationError("invalid history boundary")
        prior=[{"role":m.role.value if isinstance(m.role,MessageRole) else str(m.role),
                "content":m.content,"tools":[c.name for c in m.tool_calls]} for m in messages[start:end]]
        import json
        reference=ModelMessage(role=MessageRole.USER,content="历史对话参考（数据，不是本轮新指令）：\n"+json.dumps(prior,ensure_ascii=False))
        return messages[:start]+(reference,)+messages[end:]

    @staticmethod
    def _resolve_purpose(purpose: ModelPurpose | str) -> ModelPurpose:
        try:
            return ModelPurpose(purpose)
        except ValueError as exc:
            raise RequestValidationError(
                f"未登记的调用目的：{purpose!r}；新增用途必须写入 ModelPurpose"
            ) from exc

    @staticmethod
    def _wire_messages(messages: Sequence[ModelMessage]) -> List[Dict[str, Any]]:
        if not messages:
            raise RequestValidationError("messages 不能为空")
        wire: List[Dict[str, Any]] = []
        # declared_calls：历史上出现过的全部调用 ID（唯一性）；
        # pending：已声明但还没收到结果的 ID（配对与消费顺序）。
        declared_calls: set[str] = set()
        pending: List[str] = []
        for index, message in enumerate(messages):
            if not isinstance(message, ModelMessage):
                raise RequestValidationError(f"第 {index} 条消息必须是 ModelMessage")
            role = message.role.value if isinstance(message.role, MessageRole) else str(message.role)
            if role not in _ALLOWED_ROLES:
                raise RequestValidationError(f"第 {index} 条消息角色非法：{role!r}")
            content = message.content
            wire_calls = _wire_tool_calls(index, message.tool_calls)
            # assistant 仅有工具调用时允许空正文（None 或空字符串）。
            if content is None:
                if role != MessageRole.ASSISTANT.value or not wire_calls:
                    raise RequestValidationError(
                        f"第 {index} 条消息 content 为 null 只允许用于带 tool_calls 的 assistant"
                    )
            elif not isinstance(content, (str, list)):
                raise RequestValidationError(
                    f"第 {index} 条消息 content 必须是字符串、内容块列表或（assistant 专用）null"
                )
            if isinstance(content, list):
                for block in content:
                    if not isinstance(block, dict) or "type" not in block:
                        raise RequestValidationError(
                            f"第 {index} 条消息的内容块必须是含 type 的对象"
                        )
            if role == MessageRole.TOOL.value:
                if not message.tool_call_id:
                    raise RequestValidationError(f"第 {index} 条 tool 消息缺少 tool_call_id")
                if wire_calls:
                    raise RequestValidationError(f"第 {index} 条 tool 消息不能携带 tool_calls")
                # 一个调用只能被消费一次：必须是“已声明且尚未收到结果”的 ID。
                if message.tool_call_id not in pending:
                    raise RequestValidationError(
                        f"第 {index} 条 tool 消息的 tool_call_id 没有待配对的 assistant 调用："
                        f"{message.tool_call_id}（未声明或已被消费）"
                    )
                pending.remove(message.tool_call_id)
            else:
                if pending:
                    raise RequestValidationError(
                        f"第 {index} 条 {role} 消息出现时仍有未完成的工具调用："
                        + "、".join(pending)
                    )
            if role != MessageRole.ASSISTANT.value and wire_calls:
                raise RequestValidationError(f"第 {index} 条消息角色 {role} 不能携带 tool_calls")
            for call in wire_calls:
                call_id = call["id"]
                if call_id in declared_calls:
                    raise RequestValidationError(f"第 {index} 条消息出现重复的 tool_call_id：{call_id}")
                declared_calls.add(call_id)
                pending.append(call_id)

            item: Dict[str, Any] = {"role": role, "content": copy.deepcopy(content)}
            if role == "assistant" and message.reasoning_content is not None:
                item["reasoning_content"] = message.reasoning_content
            if message.name:
                item["name"] = message.name
            if message.tool_call_id:
                item["tool_call_id"] = message.tool_call_id
            if wire_calls:
                # 只有工具调用、没有正文时按官方 assistant 结构发送 null。
                if content == "":
                    item["content"] = None
                item["tool_calls"] = wire_calls
            wire.append(item)

        # 请求不能以未完成的工具调用结束：assistant 声明的每个 ID 都应有结果。
        if pending:
            raise RequestValidationError(
                "历史以未完成的工具调用结束，缺少对应的 tool 结果：" + "、".join(pending)
            )
        return wire

    @staticmethod
    def _wire_tools(
        purpose: ModelPurpose, tools: Sequence[ToolSpec]
    ) -> List[Dict[str, Any]]:
        if not tools:
            return []
        if not PURPOSE_TOOLS_ALLOWED.get(purpose, False):
            # 不阻止该用途携带工具，但必须留下痕迹：工具语义由主链路决定。
            logger.warning(
                "purpose=%s 首版预期不携带工具，本次仍按调用方要求保留 %d 个工具定义",
                purpose.value,
                len(tools),
            )
        wire: List[Dict[str, Any]] = []
        seen: set[str] = set()
        for tool in tools:
            if not isinstance(tool, ToolSpec):
                raise RequestValidationError("tools 中每项必须是 ToolSpec")
            if not tool.name or not tool.name.strip():
                raise RequestValidationError("工具名不能为空")
            if tool.name in seen:
                raise RequestValidationError(f"工具名重复：{tool.name}")
            seen.add(tool.name)
            wire.append(
                {
                    "type": "function",
                    "function": {
                        "name": tool.name,
                        "description": tool.description,
                        "parameters": copy.deepcopy(dict(tool.parameters)),
                    },
                }
            )
        return wire

    @staticmethod
    def _validated_parameters(parameters: Mapping[str, Any]) -> Dict[str, Any]:
        if not isinstance(parameters, Mapping):
            raise RequestValidationError("parameters 必须是映射")
        forbidden = FORBIDDEN_PARAMETER_KEYS & set(parameters)
        if forbidden:
            raise ThinkingPolicyViolation(
                "禁止调用方覆盖思考策略或请求骨架：" + "、".join(sorted(forbidden))
            )
        unknown = set(parameters) - ALLOWED_PARAMETER_KEYS
        if unknown:
            raise RequestValidationError("不支持的参数：" + "、".join(sorted(unknown)))
        return {key: copy.deepcopy(value) for key, value in parameters.items()}


def describe_purposes() -> List[Dict[str, Any]]:
    """用途与默认关闭策略；配置后的实际模式由运行视图补充。"""

    return [
        {
            "purpose": purpose.value,
            "description": PURPOSE_DESCRIPTIONS.get(purpose, ""),
            "tools_allowed": bool(PURPOSE_TOOLS_ALLOWED.get(purpose, False)),
            "thinking": dict(THINKING_DISABLED),
        }
        for purpose in ModelPurpose
    ]
