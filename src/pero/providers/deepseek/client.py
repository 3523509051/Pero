"""DeepSeek Chat Completions 异步客户端（实现 ModelProvider 契约）。

约束：
1. 请求只能经 `DeepSeekRequestBuilder` 构造，本模块不拼业务请求、不合并额外参数。
2. 真正发送前，对序列化后、即将交给 HTTP 层的最终 JSON 再做一次政策校验；
   此后不再向该 JSON 追加任何字段（含 extra_body 之类的私货）。
3. 凭据只从配置指定的环境变量读取；缺失时抛 `api_key_missing`，不返回假回复。
4. 首版非流式、不自动重试、不调用付费接口来"验证"连通性。
5. 日志只记录请求编号、purpose、状态码、调用数、耗时；不记录 Authorization、
   正文、完整远端错误体或附件内容。
6. 响应中的 `reasoning_content` 不进入 `ModelResult`、历史或日志。
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from dataclasses import dataclass, replace
from typing import Any, Dict, List, Mapping, Protocol, runtime_checkable

from pero.core.contracts import (
    ModelRequest,
    ModelResult,
    ModelToolCall,
)
from pero.core.settings import ProviderSettings
from pero.providers.deepseek.request_builder import (
    DeepSeekRequestBuilder,
    RequestValidationError,
    ThinkingPolicyViolation,
    assert_thinking_disabled,
    assert_thinking_policy,
    validate_outbound_body,
)

logger = logging.getLogger(__name__)

#: usage 中允许保留的数值字段（不含明细嵌套，避免把审核/推理细节带出去）。
_USAGE_KEYS = (
    "prompt_tokens",
    "completion_tokens",
    "total_tokens",
    "prompt_cache_hit_tokens",
    "prompt_cache_miss_tokens",
)

#: 非 2xx 只给本地固定说明，绝不转发上游 error.message / error.type / 原始响应体。
#: 上游错误体可能回显无效 Key、请求内容或地址，截断与去换行不构成脱敏。
_LOCAL_HTTP_REASONS = {
    400: "请求不符合服务端要求",
    401: "凭据无效或未获授权",
    403: "访问被拒绝",
    404: "接口路径不存在",
    408: "请求超时",
    413: "请求体过大",
    422: "参数未被接受",
    429: "请求过于频繁",
    500: "服务端内部错误",
    502: "上游网关错误",
    503: "服务暂不可用",
    504: "上游网关超时",
}


class ModelCallError(Exception):
    """模型调用失败。``code`` 用于接口与报告，``message`` 不含凭据。"""

    code = "model_call_failed"

    def __init__(self, message: str, *, http_status: int | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.http_status = http_status


class ModelNotConfiguredError(ModelCallError):
    """未配置 API Key（或空值），调用被明确拒绝。"""

    code = "api_key_missing"


class ModelCallTimeout(ModelCallError):
    code = "timeout"


class ModelTransportError(ModelCallError):
    """连接失败/传输异常；不含凭据与完整 URL 查询串。"""

    code = "transport_error"


class ModelHTTPStatusError(ModelCallError):
    """非 2xx 响应：只暴露状态码与本地固定说明，不带上游错误正文。"""

    def __init__(self, status: int) -> None:
        reason = _LOCAL_HTTP_REASONS.get(status, "服务端返回了非成功状态")
        super().__init__(f"模型服务返回 HTTP {status}（{reason}）", http_status=status)
        self.code = f"http_{status}"


class ModelProtocolError(ModelCallError):
    """响应 JSON 非法、缺 choices 或结构不符。"""

    code = "protocol_error"

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


class ModelRequestError(ModelCallError):
    """待发请求未通过构造/政策校验（例如历史里 tool 消息未配对）。"""

    code = "request_invalid"


@dataclass(frozen=True)
class HttpResult:
    status: int
    text: str


@runtime_checkable
class AsyncHttpTransport(Protocol):
    """可注入的 HTTP 传输层。

    只接收已经完成序列化与校验的 JSON 字符串，便于以后接入实机检查用的
    模拟传输或带监控的传输实现，无需改动客户端逻辑。
    """

    async def post_json(
        self,
        *,
        url: str,
        headers: Mapping[str, str],
        body_json: str,
        timeout: float,
    ) -> HttpResult: ...

    async def aclose(self) -> None: ...


class HttpxTransport:
    """基于 httpx 的默认传输实现；连接池懒创建，可显式关闭。"""

    def __init__(self, *, verify: bool = True) -> None:
        self._verify = verify
        self._client: Any = None

    def _ensure_client(self) -> Any:
        if self._client is None:
            import httpx

            self._client = httpx.AsyncClient(
                verify=self._verify,
                follow_redirects=False,
                limits=httpx.Limits(max_connections=8, max_keepalive_connections=4),
            )
        return self._client

    async def post_json(
        self,
        *,
        url: str,
        headers: Mapping[str, str],
        body_json: str,
        timeout: float,
    ) -> HttpResult:
        client = self._ensure_client()
        response = await client.post(
            url,
            content=body_json.encode("utf-8"),
            headers=dict(headers),
            timeout=timeout,
        )
        return HttpResult(status=response.status_code, text=response.text)

    async def aclose(self) -> None:
        if self._client is not None:
            client = self._client
            self._client = None
            await client.aclose()


class DeepSeekModelProvider:
    """ModelProvider 契约的真实实现（首版非流式）。"""

    provider_id = "deepseek"

    def __init__(
        self,
        *,
        settings: ProviderSettings,
        model: str,
        request_builder: DeepSeekRequestBuilder | None = None,
        transport: AsyncHttpTransport | None = None,
    ) -> None:
        self._settings = settings
        self._model = model
        self._builder = request_builder or DeepSeekRequestBuilder(
            model=model,
            base_url=settings.base_url,
            request_timeout_seconds=settings.request_timeout_seconds,
        )
        self._transport: AsyncHttpTransport = transport or HttpxTransport()
        self._timeout = float(settings.request_timeout_seconds)

    # ------------------------------------------------------------------ #
    # 状态
    # ------------------------------------------------------------------ #

    @property
    def model_name(self) -> str:
        resolver=getattr(self,"credential_resolver",None)
        return resolver.profile()[1]['model'] if resolver else self._model

    @property
    def api_key_env(self) -> str:
        return self._settings.api_key_env

    @property
    def api_key_configured(self) -> bool:
        """只判断环境变量是否存在且非空，不读取、不打印其值。"""

        resolver=getattr(self,"credential_resolver",None)
        return resolver.configured(self._settings.api_key_env) if resolver else bool(os.environ.get(self._settings.api_key_env, "").strip())

    @property
    def endpoint_url(self) -> str:
        return f"{self._settings.base_url.rstrip('/')}/chat/completions"

    async def aclose(self) -> None:
        await self._transport.aclose()

    # ------------------------------------------------------------------ #
    # 调用
    # ------------------------------------------------------------------ #

    async def complete(self, request: ModelRequest) -> ModelResult:
        from pero.configuration.credentials import CredentialUnavailable
        resolver=getattr(self,"credential_resolver",None)
        thinking=False
        endpoint=self.endpoint_url
        try:
            if resolver:
                _profile,profile,thinking=resolver.profile(request)
                builder=DeepSeekRequestBuilder(model=profile['model'],base_url=profile['base_url'],thinking_enabled=thinking,
                                              request_timeout_seconds=self._timeout)
                endpoint=profile['base_url'].rstrip('/')+'/chat/completions'
            else:builder=self._builder
            prepared = builder.build(request)
        except CredentialUnavailable:
            raise ModelNotConfiguredError("模型配置引用不可用") from None
        except (ThinkingPolicyViolation, RequestValidationError) as exc:
            # 历史结构或策略问题属于“请求不可发出”，转成明确的调用错误，不外抛成 500。
            raise ModelRequestError(f"请求未通过构造校验：{exc}") from exc
        body = prepared.http_body()  # 导出边界已校验一次

        # 序列化后再解析回来：真正发给 HTTP 层的就是这份内容，
        # 对它做最后一次校验，之后不再合并任何参数。
        body_json = json.dumps(body, ensure_ascii=False, separators=(",", ":"))
        wire_body = json.loads(body_json)
        expected = "enabled" if thinking else "disabled"
        validate_outbound_body(wire_body,expected)
        assert_thinking_policy(wire_body,expected)

        try:
            api_key = (await resolver.resolve(profile['credential_ref']) if resolver and profile.get('credential_ref')
                       else os.environ.get(self._settings.api_key_env, "").strip())
        except CredentialUnavailable:
            raise ModelNotConfiguredError("模型凭据引用不可用") from None
        if not api_key:
            raise ModelNotConfiguredError(
                f"未配置模型凭据：环境变量 {self._settings.api_key_env} 为空"
            )

        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

        try:
            result = await asyncio.wait_for(
                self._transport.post_json(
                    url=endpoint,
                    headers=headers,
                    body_json=body_json,
                    timeout=self._timeout,
                ),
                timeout=self._timeout,
            )
        except asyncio.CancelledError:
            # 取消由调用方（HTTP 请求中断/超时）触发，原样向上传播，状态由循环层记录。
            logger.info(
                "model_call_cancelled request_id=%s purpose=%s",
                prepared.request_id,
                prepared.purpose.value,
            )
            raise
        except asyncio.TimeoutError as exc:
            logger.warning(
                "model_call_timeout request_id=%s purpose=%s timeout=%ss",
                prepared.request_id,
                prepared.purpose.value,
                self._timeout,
            )
            raise ModelCallTimeout("模型调用超时") from exc
        except Exception as exc:  # 传输层异常：只记录类型，不打印请求头
            logger.warning(
                "model_call_transport_error request_id=%s purpose=%s error=%s",
                prepared.request_id,
                prepared.purpose.value,
                type(exc).__name__,
            )
            raise ModelTransportError(f"模型服务连接失败（{type(exc).__name__}）") from exc

        if not 200 <= result.status < 300:
            # 不解析、不记录、不转发上游错误体；只保留可判断的状态码。
            logger.warning(
                "model_call_http_error request_id=%s purpose=%s status=%s code=http_%s",
                prepared.request_id,
                prepared.purpose.value,
                result.status,
                result.status,
            )
            raise ModelHTTPStatusError(result.status)

        parsed = _parse_success(result.text, request_id=prepared.request_id)
        if thinking and request.tools and parsed.tool_calls and parsed.reasoning_content is None:
            raise ModelResponseError("服务未返回思考工具调用所需的临时协议内容")
        logger.info(
            "model_call_completed request_id=%s purpose=%s status=%s tool_calls=%d finish=%s",
            prepared.request_id,
            prepared.purpose.value,
            result.status,
            len(parsed.tool_calls),
            parsed.finish_reason,
        )
        return parsed if thinking else replace(parsed,reasoning_content=None)


def _parse_success(text: str, *, request_id: str) -> ModelResult:
    try:
        data = json.loads(text)
    except (json.JSONDecodeError, TypeError) as exc:
        raise ModelProtocolError("invalid_json", "模型响应不是合法 JSON") from exc
    if not isinstance(data, dict):
        raise ModelProtocolError("invalid_response", "模型响应顶层不是对象")

    choices = data.get("choices")
    if not isinstance(choices, list) or not choices:
        raise ModelProtocolError("missing_choices", "模型响应缺少 choices")
    choice = choices[0]
    if not isinstance(choice, dict):
        raise ModelProtocolError("invalid_choice", "choices 首项不是对象")
    message = choice.get("message")
    if not isinstance(message, dict):
        raise ModelProtocolError("missing_message", "响应缺少 message")

    content = message.get("content")
    if content is None:
        content_text = ""
    elif isinstance(content, str):
        content_text = content
    else:
        raise ModelProtocolError("invalid_content", "message.content 不是字符串")

    raw_calls = message.get("tool_calls") or []
    if not isinstance(raw_calls, list):
        raise ModelProtocolError("invalid_tool_calls", "message.tool_calls 不是数组")
    calls: List[ModelToolCall] = []
    for item in raw_calls:
        if not isinstance(item, dict):
            raise ModelProtocolError("invalid_tool_call", "tool_calls 存在非对象项")
        call_id = item.get("id")
        function = item.get("function")
        if not isinstance(call_id, str) or not call_id:
            raise ModelProtocolError("invalid_tool_call", "tool_calls 缺少调用 ID")
        if not isinstance(function, dict):
            raise ModelProtocolError("invalid_tool_call", "tool_calls 缺少 function")
        name = function.get("name")
        arguments = function.get("arguments")
        if not isinstance(name, str) or not name:
            raise ModelProtocolError("invalid_tool_call", "tool_calls 缺少工具名")
        if not isinstance(arguments, str):
            # 保留原始字符串；非字符串视为协议异常，不尝试修复。
            raise ModelProtocolError("invalid_tool_call", "tool_calls arguments 不是字符串")
        calls.append(ModelToolCall(call_id=call_id, name=name, arguments=arguments))

    usage_raw = data.get("usage") if isinstance(data.get("usage"), dict) else {}
    usage: Dict[str, int] = {}
    for key in _USAGE_KEYS:
        value = usage_raw.get(key) if isinstance(usage_raw, Mapping) else None
        if isinstance(value, int) and not isinstance(value, bool):
            usage[key] = value

    finish_reason = choice.get("finish_reason")
    if finish_reason is not None and not isinstance(finish_reason, str):
        finish_reason = None

    # 明确不读取 message.reasoning_content：即使思考被误开启，也不进入历史/日志。
    return ModelResult(
        text=content_text,
        tool_calls=tuple(calls),
        finish_reason=finish_reason,
        usage=usage,
        request_id=request_id,
        reasoning_content=message.get("reasoning_content") if isinstance(message.get("reasoning_content"),str) else None,
    )


__all__ = [
    "AsyncHttpTransport",
    "DeepSeekModelProvider",
    "HttpxTransport",
    "HttpResult",
    "ModelCallError",
    "ModelCallTimeout",
    "ModelHTTPStatusError",
    "ModelNotConfiguredError",
    "ModelProtocolError",
    "ModelRequestError",
    "ModelTransportError",
    "RequestValidationError",
]
