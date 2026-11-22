"""工具执行器：串行、有界、失败也留结构化结果。

规则：
- 每个被模型请求的调用都产出一条结果（成功或失败），保证 assistant.tool_calls
  与 tool 消息一一配对，不留下孤立消息。
- 同一 run 内重复的 call ID 拒绝执行，避免副作用重复。
- 超出本轮工具预算的调用不执行，返回 `call_limit`。
- 未知工具、参数不是 JSON 对象、参数不符合 Schema、超时、异常都转成
  有界失败结果，模型续答时可见，不假装成功。
- 结果过大会被替换为 `result_too_large`，避免把整份内容塞回上下文。
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Mapping, Sequence, Set

from pero.core.contracts import ModelToolCall
from pero.tools.registry import ToolRegistry, UnknownToolError
from pero.tools.validation import validate_arguments

logger = logging.getLogger(__name__)

DEFAULT_MAX_RESULT_CHARS = 4000


class _ResultNotEncodable(Exception):
    """工具返回值无法安全转成 JSON 文本（非对象、含不可序列化值等）。"""


def _encode_success(name: str, result: Any) -> str:
    """把工具结果编码为回填用的 JSON 文本；失败抛 `_ResultNotEncodable`。"""

    if not isinstance(result, Mapping):
        raise _ResultNotEncodable("工具返回值不是对象")
    try:
        payload = dict(result)
        return json.dumps(
            {"ok": True, "tool": name, "result": payload},
            ensure_ascii=False,
        )
    except Exception as exc:  # 含不可序列化对象、异常 Mapping 实现等
        raise _ResultNotEncodable(
            f"工具结果无法编码为 JSON（{type(exc).__name__}）"
        ) from exc


@dataclass(frozen=True)
class ToolOutcome:
    """一次工具调用的结果，同时携带要回填给模型的 JSON 文本。"""

    call_id: str
    name: str
    ok: bool
    duration_ms: int
    content: str
    error_code: str | None = None
    error_message: str | None = None
    ephemeral: bool = False
    analysis: Mapping[str, Any] | None = None

    def summary(self) -> Dict[str, Any]:
        """脱敏摘要，用于接口返回与报告，不含结果正文。"""

        return {
            "call_id": self.call_id,
            "name": self.name,
            "status": "ok" if self.ok else "error",
            "error_code": self.error_code,
            "duration_ms": self.duration_ms,
            "analysis": dict(self.analysis) if self.analysis else None,
        }


class ToolExecutor:
    """串行执行模型请求的工具调用。"""

    def __init__(
        self,
        registry: ToolRegistry,
        *,
        timeout_seconds: float,
        max_calls_per_run: int,
        max_result_chars: int = DEFAULT_MAX_RESULT_CHARS,
    ) -> None:
        if timeout_seconds <= 0:
            raise ValueError("tool timeout 必须是正数")
        if max_calls_per_run <= 0:
            raise ValueError("tool max_calls_per_run 必须是正数")
        self._registry = registry
        self._timeout = float(timeout_seconds)
        self._max_calls = int(max_calls_per_run)
        self._max_result_chars = int(max_result_chars)

    @property
    def timeout_seconds(self) -> float:
        return self._timeout

    @property
    def max_calls_per_run(self) -> int:
        return self._max_calls

    async def execute(
        self,
        calls: Sequence[ModelToolCall],
        *,
        used_calls: int,
        seen_call_ids: Set[str],
    ) -> List[ToolOutcome]:
        """按顺序执行调用；`seen_call_ids` 由调用方持有并跨轮复用。"""

        from pero.tools.context import current_tool_context
        context = current_tool_context()
        configuration = getattr(context, 'configuration', None)
        outcomes: List[ToolOutcome] = []
        executed = 0
        for call in calls:
            started = time.monotonic()

            if call.call_id in seen_call_ids:
                outcomes.append(
                    self._failure(call, started, "duplicate_call_id", "该调用 ID 已执行过，拒绝重复执行")
                )
                continue
            if used_calls + executed >= self._max_calls:
                outcomes.append(
                    self._failure(call, started, "call_limit", "超过本轮工具调用上限，未执行")
                )
                continue

            seen_call_ids.add(call.call_id)
            executed += 1
            tool_config = configuration.tool(call.name) if configuration else {}
            if tool_config.get('enabled') is False:
                outcomes.append(self._failure(call, started, "tool_disabled", "该工具在本轮配置中已停用"))
                continue

            try:
                tool = self._registry.get(call.name)
            except UnknownToolError:
                outcomes.append(
                    self._failure(call, started, "unknown_tool", f"未登记的工具：{call.name}")
                )
                continue

            try:
                arguments = json.loads(call.arguments) if call.arguments.strip() else {}
            except json.JSONDecodeError:
                outcomes.append(
                    self._failure(call, started, "invalid_arguments", "工具参数不是合法 JSON")
                )
                continue
            if not isinstance(arguments, dict):
                outcomes.append(
                    self._failure(call, started, "invalid_arguments", "工具参数必须是 JSON 对象")
                )
                continue

            arguments = {**(tool_config.get('defaults') or {}), **arguments}
            try:
                spec = tool.spec()
                errors = validate_arguments(arguments, spec.parameters)
            except Exception as exc:  # 工具声明/校验异常也不能外抛
                outcomes.append(
                    self._failure(
                        call, started, "tool_error", f"工具声明校验异常（{type(exc).__name__}）"
                    )
                )
                continue
            if errors:
                outcomes.append(
                    self._failure(call, started, "invalid_arguments", "；".join(errors))
                )
                continue

            # 执行、取值、转 dict、JSON 编码都在这一个错误边界内：
            # 任何一步失败都产出有界 tool_error，不会留下半条工具历史。
            try:
                timeout = min(float(tool_config.get('timeout_seconds') or getattr(tool, "timeout_seconds", self._timeout)), 240.0)
                result = await asyncio.wait_for(tool.run(arguments), timeout=timeout)
                content = _encode_success(call.name, result)
            except asyncio.TimeoutError:
                outcomes.append(
                    self._failure(call, started, "timeout", f"工具执行超时（>{timeout:g}s）")
                )
                continue
            except asyncio.CancelledError:
                raise
            except _ResultNotEncodable as exc:
                outcomes.append(
                    self._failure(call, started, "tool_error", str(exc))
                )
                continue
            except Exception as exc:  # 工具内部异常不外抛，转为结构化失败
                outcomes.append(
                    self._failure(call, started, "tool_error", f"工具执行异常（{type(exc).__name__}）")
                )
                continue

            if isinstance(result, Mapping) and result.get("ok") is False:
                code = str(result.get("error_code") or result.get("status") or "tool_rejected")[:80]
                outcomes.append(self._failure(call, started, code, "工具明确报告未成功完成"))
                continue

            result_cap=max(self._max_result_chars,48000) if call.name in {'list_stickers','list_images'} else self._max_result_chars
            if len(content) > result_cap and not getattr(tool, "unlimited_result", False):
                outcomes.append(
                    self._failure(
                        call,
                        started,
                        "result_too_large",
                        f"工具结果超过 {result_cap} 字符上限",
                    )
                )
                continue

            duration_ms = int((time.monotonic() - started) * 1000)
            logger.info("tool_call name=%s ok=true duration_ms=%d", call.name, duration_ms)
            outcomes.append(
                ToolOutcome(
                    call_id=call.call_id,
                    name=call.name,
                    ok=True,
                    duration_ms=duration_ms,
                    content=content,
                    ephemeral=bool(getattr(tool, "ephemeral_result", False)),
                )
            )

        return outcomes

    def _failure(
        self,
        call: ModelToolCall,
        started: float,
        code: str,
        message: str,
    ) -> ToolOutcome:
        duration_ms = int((time.monotonic() - started) * 1000)
        logger.info(
            "tool_call name=%s ok=false code=%s duration_ms=%d",
            call.name or "(empty)",
            code,
            duration_ms,
        )
        content = json.dumps(
            {"ok": False, "tool": call.name, "error_code": code, "message": message},
            ensure_ascii=False,
        )
        return ToolOutcome(
            call_id=call.call_id,
            name=call.name,
            ok=False,
            duration_ms=duration_ms,
            content=content,
            error_code=code,
            error_message=message,
        )
