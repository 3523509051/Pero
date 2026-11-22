"""有界 Agent 循环：输入 → 统一请求 → 工具执行 → 历史回填 → 续答。

约束：
- 每次请求都经统一构造器并强制 `thinking=disabled`；首轮 purpose 为 `main_reply`，
  续答为 `tool_continuation`，业务层不拼 HTTP。
- 有模型调用上限、工具调用总数上限和工具超时；达到上限返回明确终止状态，
  不再额外调用模型"解释一下"。
- 状态区分 completed / failed / limit_reached / cancelled，不用空文本或未知
  finish_reason 冒充完成。
- 会话历史只在内存，超限明确拒绝；取消时回滚本轮，避免留下未配对的 tool 消息。
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from enum import Enum
from typing import Dict, List, Mapping, Optional, Sequence, Set, Tuple

from pero.agent.sessions import AgentSession, RunSummary
from pero.core.contracts import (
    MessageRole,
    ModelMessage,
    ModelPurpose,
    ModelRequest,
)
from pero.core.settings import RuntimeSettings
from pero.providers.deepseek.client import ModelCallError
from pero.tools.executor import ToolExecutor, ToolOutcome
from pero.tools.registry import ToolRegistry

logger = logging.getLogger(__name__)


class RunStatus(str, Enum):
    COMPLETED = "completed"
    FAILED = "failed"
    LIMIT_REACHED = "limit_reached"
    CANCELLED = "cancelled"


class SessionBusyError(RuntimeError):
    """同一会话已有运行在进行。"""

    code = "session_busy"


class HistoryLimitError(RuntimeError):
    """会话历史已达上限，明确拒绝而不是静默丢弃旧消息。"""

    code = "history_limit"


class UserInputTooLongError(RuntimeError):
    """输入文本超过配置上限。"""

    code = "input_too_long"


def _utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def _history_is_paired(history: Sequence[ModelMessage]) -> bool:
    """历史里每个 assistant.tool_calls 是否都有对应的 tool 结果。"""

    pending: Set[str] = set()
    for message in history:
        if message.role == MessageRole.ASSISTANT and message.tool_calls:
            pending.update(call.call_id for call in message.tool_calls)
        elif message.role == MessageRole.TOOL and message.tool_call_id:
            pending.discard(message.tool_call_id)
    return not pending


@dataclass(frozen=True)
class RunResult:
    run_id: str
    status: RunStatus
    text: str = ""
    history_text: Optional[str] = None
    image_evidence: Sequence[dict] = field(default_factory=tuple)
    error_code: Optional[str] = None
    error_message: Optional[str] = None
    finish_reason: Optional[str] = None
    tool_outcomes: Sequence[ToolOutcome] = field(default_factory=tuple)
    model_calls: int = 0
    tool_calls: int = 0
    started_at: str = ""
    finished_at: str = ""
    #: 本次运行各次模型调用 usage 的**累计真实数值**（供应商未返回则保持空）。
    usage: Dict[str, int] = field(default_factory=dict)


@dataclass
class _RunState:
    """跨异常处理共享的真实运行计数与最近一份完整合法历史快照。

    `snapshot` 在每次完整写入一组消息后更新，因此异常/取消时回滚的是
    "最近一次完整合法历史"，而不是无条件丢弃本轮早前完成的工具组。
    """

    snapshot: List[ModelMessage]
    model_calls: int = 0
    tool_calls: int = 0
    outcomes: List[ToolOutcome] = field(default_factory=list)


class AgentLoop:
    """首版单 Agent 循环实现，按会话串行处理。"""

    def __init__(
        self,
        *,
        provider,
        executor: ToolExecutor,
        tool_registry: ToolRegistry,
        limits: RuntimeSettings,
        event_bus=None,
        background_jobs=None,
    ) -> None:
        self._provider = provider
        self._executor = executor
        self._registry = tool_registry
        self._limits = limits
        #: P15：可选 SSE 事件总线（tool.started/finished 在真实执行点发出）。
        self._event_bus = event_bus
        self._background_jobs = background_jobs
        #: run_id → 累计 usage（真实返回值，只记数值）；完成时随 RunResult 交回。
        self._run_usage: Dict[str, Dict[str, int]] = {}

    @property
    def limits(self) -> RuntimeSettings:
        return self._limits

    async def run(
        self,
        *,
        session: AgentSession,
        user_text: str,
        request_note: str = "",
        system_prompt: str = "",
        provider=None,
    ) -> RunResult:
        """执行一轮。`user_text` 是**要提交进历史**的用户事实原文；
        `request_note` 是本轮**临时注记**（保存凭据/检索参考）：
        只会出现在发给模型的请求里（含工具续答），**绝不**写进会话历史，
        也不会随轮次落库——下一轮与 SQLite 里都只有用户原文。
        `system_prompt` 是**稳定人格上下文**（per-Agent 配置）：每条请求都作为
        独立 SYSTEM 消息置于最前，同样不入历史与 SQLite。
        """

        text = (user_text or "").strip()
        if not text:
            raise ValueError("消息正文不能为空")
        if session.busy:
            raise SessionBusyError("该会话正在处理上一条消息")
        if len(text) > self._limits.max_user_chars:
            raise UserInputTooLongError(
                f"输入超过 {self._limits.max_user_chars} 字符上限"
            )
        if request_note and len(text) + len(request_note) + 2 > self._limits.max_user_chars:
            raise UserInputTooLongError(
                "用户正文加临时注记超过输入上限（注记不应被截断，应整体拒绝）"
            )
        if len(session.history) + 2 > self._limits.max_history_messages:
            raise HistoryLimitError(
                "会话历史已达上限，无法再接受新一轮对话；本阶段会话不落盘，可新建会话"
            )

        run_id = f"run-{uuid.uuid4().hex[:12]}"
        started_at = _utc()
        state = _RunState(snapshot=list(session.history))
        turn_start = len(session.history)
        session.busy = True
        try:
            return await self._run(
                session=session,
                text=text,
                request_note=request_note or "",
                system_prompt=system_prompt,
                run_id=run_id,
                started_at=started_at,
                provider=provider or self._provider,
                state=state,
            )
        except asyncio.CancelledError as exc:
            from pero.tools.context import current_tool_context
            images=getattr(current_tool_context(),"images",None)
            exc.image_evidence=tuple(images.evidence()) if images is not None else ()
            exc.completed_tool_outcomes=tuple(replace(o,analysis={**o.analysis,'state':'cancelled'})
                if o.analysis and o.analysis.get('state')=='pending' else o for o in state.outcomes)
            exc.completed_tools_started_at=started_at
            cancelled_context=current_tool_context()
            if cancelled_context is not None:
                cancelled_context.execution_evidence.update(started_at=started_at,outcomes=exc.completed_tool_outcomes)
            # 只在存在未配对组时回滚到最近一份完整合法历史；
            # 已完整记录的工具组保留，回滚不撤销已执行的工具副作用。
            restored = False
            if not _history_is_paired(session.history):
                session.history[:] = state.snapshot
                restored = True
            session.run_count += 1
            session.last_run = RunSummary(
                run_id=run_id,
                status=RunStatus.CANCELLED.value,
                error_code="cancelled",
                started_at=started_at,
                finished_at=_utc(),
                model_calls=state.model_calls,
                tool_calls=state.tool_calls,
            )
            logger.info(
                "agent_run_cancelled run_id=%s session_id=%s model_calls=%d tool_calls=%d restored=%s",
                run_id,
                session.session_id,
                state.model_calls,
                state.tool_calls,
                restored,
            )
            raise
        except Exception as exc:  # 非取消的意外异常：给出明确 failed 终态，不留半条配对
            restored = False
            if not _history_is_paired(session.history):
                session.history[:] = state.snapshot
                restored = True
            logger.error(
                "agent_run_internal_error run_id=%s session_id=%s error_type=%s history_restored=%s model_calls=%d tool_calls=%d",
                run_id,
                session.session_id,
                type(exc).__name__,
                restored,
                state.model_calls,
                state.tool_calls,
            )
            return self._finish(
                session,
                run_id=run_id,
                started_at=started_at,
                status=RunStatus.FAILED,
                error_code="internal_error",
                error_message="运行发生内部错误，本轮已终止",
                outcomes_all=tuple(state.outcomes),
                model_calls=state.model_calls,
                tool_calls=state.tool_calls,
            )
        finally:
            # Protocol-only reasoning must never survive this turn or reach persistence.
            session.history[:] = [replace(m,reasoning_content=None) if m.reasoning_content is not None else m for m in session.history]
            self._compact_temporary_results(session, turn_start, state.outcomes)
            from pero.tools.context import current_tool_context
            files = getattr(current_tool_context(), "files", None)
            if files is not None:
                for index in range(turn_start,len(session.history)):
                    message=session.history[index]
                    if message.role == MessageRole.ASSISTANT and not message.tool_calls and isinstance(message.content,str):
                        session.history[index]=replace(message,content=files.history_view(message.content))
                files.clear()
            images=getattr(current_tool_context(),"images",None)
            if images is not None:
                evidence=images.evidence()
                if evidence and len(session.history)<self._limits.max_history_messages:
                    lines=["[图片输入记录｜非用户发言]"]
                    for image in evidence:
                        state="已提供给模型" if image.get("provided_to_model") else "已挂载，未确认提供给模型"
                        lines.append(f"{image['name']} / {image['resource_id']}：{state}")
                    session.history.append(ModelMessage(role=MessageRole.SYSTEM,content="\n".join(lines)))
                images.clear()
            session.busy = False
            session.updated_at = _utc()

    # ------------------------------------------------------------------ #
    # 内部
    # ------------------------------------------------------------------ #

    async def _run(
        self,
        *,
        session: AgentSession,
        text: str,
        request_note: str,
        system_prompt: str = "",
        run_id: str,
        started_at: str,
        provider,
        state: _RunState,
    ) -> RunResult:
        # 提交进历史的只有用户原文；注记只在构造请求时合并（见 _request_messages）。
        turn_user_index = len(session.history)
        session.history.append(ModelMessage(role=MessageRole.USER, content=text))
        outcomes_all: List[ToolOutcome] = []
        seen_call_ids: Set[str] = set()
        model_calls = 0
        tool_calls_used = 0

        while True:
            from pero.tools.context import current_tool_context
            files=getattr(current_tool_context(),"files",None)
            images=getattr(current_tool_context(),"images",None)
            extra=(1 if files is not None and files.mounts else 0)+(len(images.mounts) if images is not None else 0)
            if extra and len(session.history)+extra>self._limits.max_history_messages:
                return self._finish(session,run_id=run_id,started_at=started_at,
                    status=RunStatus.LIMIT_REACHED,error_code="history_limit",
                    error_message="本轮文件/图片挂载超出消息预算，未继续调用模型",
                    outcomes_all=outcomes_all,model_calls=model_calls,tool_calls=tool_calls_used)
            if model_calls >= self._limits.max_model_calls:
                return self._finish(
                    session,
                    run_id=run_id,
                    started_at=started_at,
                    status=RunStatus.LIMIT_REACHED,
                    error_code="model_call_limit",
                    error_message=f"达到单轮模型调用上限（{self._limits.max_model_calls}）",
                    outcomes_all=outcomes_all,
                    model_calls=model_calls,
                    tool_calls=tool_calls_used,
                )
            remaining_tools = self._limits.max_tool_calls - tool_calls_used
            config_context = current_tool_context()
            configuration = getattr(config_context, 'configuration', None)
            specs = tuple(self._registry.specs(configuration)) if remaining_tools > 0 else ()
            purpose = (
                ModelPurpose.MAIN_REPLY if model_calls == 0 else ModelPurpose.TOOL_CONTINUATION
            )
            model_calls += 1
            state.model_calls = model_calls

            current_note=request_note
            if self._background_jobs is not None and config_context is not None:
                task_note=self._background_jobs.model_note(config_context.agent_id,config_context.conversation_id)
                current_note='\n\n'.join(part for part in (request_note,task_note) if part)
            base_messages = self._file_request_messages(self._request_messages(
                session.history, turn_user_index, current_note
            ))
            # 稳定人格：per-Agent SYSTEM 消息每条请求都置于最前（主回复与续答一致），
            # 不写入会话历史，也不落库。
            execution_policy=('工具执行真实性：不得把助手历史中的“已提交、在看、看完了”当作真实执行证据。'
                '用户要求观看、读取或分析外部资料时，如果缺少实际工具结果，应调用本轮可用的相应工具；链接可来自当前消息或近期上下文。'
                '不要用“正在看、已经处理、稍后通知”等文字代替工具调用。只有工具返回真实 queued/job_id 后才能承诺后台处理和完成回报；'
                '未调用、调用失败或结果不可用时如实说明，不编造内容。后台任务状态以宿主本轮状态为准，完成状态本身不等于拿到了资料正文。')
            effective_system='\n\n'.join(part for part in (system_prompt,execution_policy) if part)
            base_messages = (ModelMessage(role=MessageRole.SYSTEM, content=effective_system),) + base_messages
            request = ModelRequest(
                purpose=purpose,
                messages=base_messages,
                tools=specs,
                history_prefix=turn_user_index,
                system_prefix=1,
            )
            try:
                result = await provider.complete(request)
                self._collect_usage(run_id, getattr(result, "usage", None))
                self._confirm_file_results(outcomes_all)
            except ModelCallError as exc:
                from pero.tools.context import current_tool_context
                files=getattr(current_tool_context(),"files",None)
                message=exc.message
                if files is not None and files.mounts and exc.http_status==400:
                    message="文件引用请求被拒绝，可能格式不支持或上下文过长；请拆分文件后重试。原件与映射保留，不自动重试或开启思考。"
                return self._finish(
                    session,
                    run_id=run_id,
                    started_at=started_at,
                    status=RunStatus.FAILED,
                    error_code=exc.code,
                    error_message=message,
                    outcomes_all=outcomes_all,
                    model_calls=model_calls,
                    tool_calls=tool_calls_used,
                )

            if result.tool_calls:
                call_ids = [call.call_id for call in result.tool_calls]
                duplicate = len(set(call_ids)) != len(call_ids)
                # 与已有历史（前几轮 assistant 声明过）的 ID 冲突同样不可写入，
                # 否则这条历史无法再构造合法请求，会永久破坏会话。
                if not duplicate:
                    existing_ids: Set[str] = set()
                    for message in session.history:
                        if message.role == MessageRole.ASSISTANT:
                            existing_ids.update(call.call_id for call in message.tool_calls)
                    duplicate = bool(existing_ids & set(call_ids))
                if duplicate:
                    return self._finish(
                        session,
                        run_id=run_id,
                        started_at=started_at,
                        status=RunStatus.FAILED,
                        error_code="duplicate_tool_call",
                        error_message="模型返回了重复的工具调用 ID，已拒绝执行",
                        finish_reason=result.finish_reason,
                        outcomes_all=outcomes_all,
                        model_calls=model_calls,
                        tool_calls=tool_calls_used,
                    )

                # 批量容量预检：assistant + 全部工具结果都要能放下，之后才执行任何工具。
                needed = 1 + len(result.tool_calls)
                if len(session.history) + needed > self._limits.max_history_messages:
                    return self._finish(
                        session,
                        run_id=run_id,
                        started_at=started_at,
                        status=RunStatus.LIMIT_REACHED,
                        error_code="history_limit",
                        error_message=(
                            f"本轮需要追加 {needed} 条消息，超过会话历史上限"
                            f"（{self._limits.max_history_messages}），未执行工具"
                        ),
                        finish_reason=result.finish_reason,
                        outcomes_all=outcomes_all,
                        model_calls=model_calls,
                        tool_calls=tool_calls_used,
                    )

                session.history.append(
                    ModelMessage(
                        role=MessageRole.ASSISTANT,
                        content=result.text,
                        tool_calls=result.tool_calls,
                        reasoning_content=result.reasoning_content,
                    )
                )
                if self._event_bus is not None:
                    for call in result.tool_calls:
                        self._event_bus.publish(
                            "tool.started",
                            agent_id=getattr(session, "agent_id", ""),
                            session_id=getattr(session, "session_id", ""),
                            data={"call_id": call.call_id, "tool": call.name},
                        )
                try:
                    outcomes = await self._executor.execute(
                        result.tool_calls,
                        used_calls=tool_calls_used,
                        seen_call_ids=seen_call_ids,
                    )
                except asyncio.CancelledError:
                    # P15-R01：取消也必须给 started 的调用发受控终态，不留
                    # 永久"处理中"；然后原样抛出（取消不属于本层吞掉的异常）。
                    if self._event_bus is not None:
                        for call in result.tool_calls:
                            self._event_bus.publish(
                                "tool.finished",
                                agent_id=getattr(session, "agent_id", ""),
                                session_id=getattr(session, "session_id", ""),
                                data={"call_id": call.call_id, "tool": call.name,
                                      "status": "cancelled"},
                            )
                    raise
                except BaseException:
                    if self._event_bus is not None:
                        for call in result.tool_calls:
                            self._event_bus.publish(
                                "tool.finished",
                                agent_id=getattr(session, "agent_id", ""),
                                session_id=getattr(session, "session_id", ""),
                                data={"call_id": call.call_id, "tool": call.name,
                                      "status": "error"},
                            )
                    raise
                if self._event_bus is not None:
                    for outcome in outcomes:
                        self._event_bus.publish(
                            "tool.finished",
                            agent_id=getattr(session, "agent_id", ""),
                            session_id=getattr(session, "session_id", ""),
                            data={
                                "call_id": outcome.call_id,
                                "tool": outcome.name,
                                "status": "ok" if outcome.ok else "failed",
                                "error_code": outcome.error_code,
                            },
                        )
                from pero.agent.tool_analysis import analyze_outcomes
                # Preserve already executed effects if optional analysis is cancelled.
                state.outcomes=outcomes_all+[replace(o,analysis={'state':'pending'})
                    if configuration and configuration.tool(o.name).get('analysis_enabled',False) else o for o in outcomes]
                state.tool_calls=tool_calls_used+sum(o.error_code not in {'duplicate_call_id','call_limit'} for o in outcomes)
                if config_context is not None:
                    config_context.execution_evidence.update(started_at=started_at,outcomes=state.outcomes)
                outcomes, analysis_calls = await analyze_outcomes(outcomes,provider,self._limits.max_model_calls-model_calls,
                    on_model_call=lambda:setattr(state,'model_calls',state.model_calls+1))
                model_calls += analysis_calls
                state.model_calls = model_calls
                for outcome in outcomes:
                    session.history.append(
                        ModelMessage(
                            role=MessageRole.TOOL,
                            content=outcome.content,
                            tool_call_id=outcome.call_id,
                        )
                    )
                outcomes_all.extend(outcomes)
                tool_calls_used += sum(
                    1
                    for outcome in outcomes
                    if outcome.error_code not in {"duplicate_call_id", "call_limit"}
                )
                # 这一组（assistant + 全部 tool 结果）已完整写入：更新计数与最近合法快照。
                state.tool_calls = tool_calls_used
                state.outcomes = outcomes_all
                state.snapshot = list(session.history)
                if any(outcome.error_code == "call_limit" for outcome in outcomes):
                    return self._finish(
                        session,
                        run_id=run_id,
                        started_at=started_at,
                        status=RunStatus.LIMIT_REACHED,
                        error_code="tool_call_limit",
                        error_message=(
                            f"达到单轮工具调用上限（{self._limits.max_tool_calls}）"
                        ),
                        outcomes_all=outcomes_all,
                        model_calls=model_calls,
                        tool_calls=tool_calls_used,
                    )
                continue

            return self._finalize_text(
                session,
                result=result,
                run_id=run_id,
                started_at=started_at,
                outcomes_all=outcomes_all,
                model_calls=model_calls,
                tool_calls=tool_calls_used,
                state=state,
            )

    @staticmethod
    def _file_request_messages(messages):
        from pero.tools.context import current_tool_context
        files=getattr(current_tool_context(),"files",None)
        images=getattr(current_tool_context(),"images",None)
        return tuple(messages)+(files.messages() if files is not None else ())+(images.messages() if images is not None else ())

    @staticmethod
    def _confirm_file_results(outcomes):
        import json
        from pero.tools.context import current_tool_context
        files=getattr(current_tool_context(),"files",None)
        images=getattr(current_tool_context(),"images",None)
        if files is not None:
            files.confirmed()
        if images is not None:
            images.confirmed()
        for index,item in enumerate(outcomes):
            if item.name in {"read_file","look_image"} and item.ok:
                payload=json.loads(item.content)
                payload["result"]["provided_to_model"]=True
                payload["result"]["status"]="provided_to_model"
                outcomes[index]=replace(item,content=json.dumps(payload,ensure_ascii=False))

    @staticmethod
    def _compact_temporary_results(session, start, outcomes):
        """Current-run tool bodies survive continuations, never the next conversation turn.

        An entire assistant-call/tool-result group is replaced together, so no orphan
        tool messages are left when one group contains both temporary and normal tools.
        """
        from pero.memory.summaries import summarize_tool_outcome
        temporary = {item.call_id for item in outcomes if item.ephemeral}
        if not temporary:
            return
        by_id = {item.call_id: item for item in outcomes}
        history = session.history
        compacted = list(history[:start])
        index = start
        while index < len(history):
            message = history[index]
            if message.role == MessageRole.ASSISTANT and message.tool_calls:
                end = index + 1
                while end < len(history) and history[end].role == MessageRole.TOOL:
                    end += 1
                if any(call.call_id in temporary for call in message.tool_calls):
                    lines = ["[工具使用记录｜非用户发言｜正文仅用于已结束的任务]"]
                    for call in message.tool_calls:
                        item = by_id.get(call.call_id)
                        if item is not None:
                            summary, refs, _truncated = summarize_tool_outcome(item)
                            lines.append(f"{call.name} / {call.call_id}: {summary}")
                            lines.extend("来源：" + str(ref)[:500] for ref in refs[:3])
                    compacted.append(ModelMessage(role=MessageRole.SYSTEM, content="\n".join(lines)))
                    index = end
                    continue
            compacted.append(message)
            index += 1
        session.history[:] = compacted

    @staticmethod
    def _request_messages(
        history: List[ModelMessage], user_index: int, note: str
    ) -> Tuple[ModelMessage, ...]:
        """构造发往模型的消息序列：本轮用户消息**临时**带上注记副本。

        历史里的原件保持用户原文；这里返回的是带注记的**副本**，只存在于请求对象中。
        工具续答（同一次运行内的后续请求）同样能看见注记，但任何历史提交路径
        （user/assistant 回填、取消回滚、SQLite 落库）拿到的都是不带注记的原件，
        不需要也不做任何"猜标题/字符串替换"式的事后清理。
        """

        if not note:
            return tuple(history)
        base = history[user_index]
        merged = ModelMessage(role=MessageRole.USER, content=f"{base.content}\n\n{note}")
        return tuple(history[:user_index]) + (merged,) + tuple(history[user_index + 1 :])

    def _finalize_text(
        self,
        session: AgentSession,
        *,
        result,
        run_id: str,
        started_at: str,
        outcomes_all: List[ToolOutcome],
        model_calls: int,
        tool_calls: int,
        state: _RunState,
    ) -> RunResult:
        finish = result.finish_reason
        text = result.text

        if finish == "stop":
            if not text.strip():
                return self._finish(
                    session,
                    run_id=run_id,
                    started_at=started_at,
                    status=RunStatus.FAILED,
                    error_code="empty_response",
                    error_message="模型返回空文本且没有工具调用",
                    finish_reason=finish,
                    outcomes_all=outcomes_all,
                    model_calls=model_calls,
                    tool_calls=tool_calls,
                )
            if len(session.history) + 1 > self._limits.max_history_messages:
                return self._finish(
                    session,
                    run_id=run_id,
                    started_at=started_at,
                    status=RunStatus.LIMIT_REACHED,
                    text=text,
                    error_code="history_limit",
                    error_message="最终回答未写入会话历史（已达上限），历史未被截断",
                    finish_reason=finish,
                    outcomes_all=outcomes_all,
                    model_calls=model_calls,
                    tool_calls=tool_calls,
                )
            session.history.append(
                ModelMessage(role=MessageRole.ASSISTANT, content=text)
            )
            state.snapshot = list(session.history)
            return self._finish(
                session,
                run_id=run_id,
                started_at=started_at,
                status=RunStatus.COMPLETED,
                text=text,
                finish_reason=finish,
                outcomes_all=outcomes_all,
                model_calls=model_calls,
                tool_calls=tool_calls,
            )

        # 非 stop 一律按真实终止原因处理，不冒充完成（即使带了部分文本）。
        if not text.strip() and finish not in {
            "length",
            "content_filter",
            "insufficient_system_resource",
            "aborted",
            "tool_calls",
        }:
            return self._finish(
                session,
                run_id=run_id,
                started_at=started_at,
                status=RunStatus.FAILED,
                error_code="empty_response",
                error_message="模型返回空文本且没有工具调用",
                finish_reason=finish,
                outcomes_all=outcomes_all,
                model_calls=model_calls,
                tool_calls=tool_calls,
            )

        known_failure = {
            "length": "finish_length",
            "content_filter": "finish_content_filter",
            "insufficient_system_resource": "finish_insufficient_resource",
            "aborted": "finish_aborted",
            "tool_calls": "inconsistent_tool_calls",
        }
        code = known_failure.get(finish or "")
        if code is None:
            code = f"finish_{finish}" if finish else "unknown_finish_reason"
        return self._finish(
            session,
            run_id=run_id,
            started_at=started_at,
            status=RunStatus.FAILED,
            text=text,
            error_code=code,
            error_message=f"模型未正常结束（finish_reason={finish}）",
            finish_reason=finish,
            outcomes_all=outcomes_all,
            model_calls=model_calls,
            tool_calls=tool_calls,
        )

    def _finish(
        self,
        session: AgentSession,
        *,
        run_id: str,
        started_at: str,
        status: RunStatus,
        text: str = "",
        error_code: Optional[str] = None,
        error_message: Optional[str] = None,
        finish_reason: Optional[str] = None,
        outcomes_all: Sequence[ToolOutcome],
        model_calls: int,
        tool_calls: int,
    ) -> RunResult:
        finished_at = _utc()
        from pero.tools.context import current_tool_context
        files=getattr(current_tool_context(),"files",None)
        images=getattr(current_tool_context(),"images",None)
        history_text=files.history_view(text) if files is not None else text
        session.run_count += 1
        session.last_run = RunSummary(
            run_id=run_id,
            status=status.value,
            error_code=error_code,
            started_at=started_at,
            finished_at=finished_at,
            model_calls=model_calls,
            tool_calls=tool_calls,
        )
        logger.info(
            "agent_run_finished run_id=%s session_id=%s status=%s error_code=%s model_calls=%d tool_calls=%d",
            run_id,
            session.session_id,
            status.value,
            error_code or "-",
            model_calls,
            tool_calls,
        )
        return RunResult(
            run_id=run_id,
            status=status,
            text=text,
            history_text=history_text,
            image_evidence=tuple(images.evidence()) if images is not None else (),
            error_code=error_code,
            error_message=error_message,
            finish_reason=finish_reason,
            tool_outcomes=tuple(outcomes_all),
            model_calls=model_calls,
            tool_calls=tool_calls,
            usage=dict(self._run_usage.pop(run_id, {}) or {}),
            started_at=started_at,
            finished_at=finished_at,
        )

    def _collect_usage(self, run_id: str, usage) -> None:
        """累计本次运行的真实 usage 数值；供应商未返回就不记录（不估算）。"""

        if not isinstance(usage, Mapping):
            return
        samples = self._run_usage.setdefault(run_id, {})
        for key in ("prompt_tokens", "prompt_cache_hit_tokens",
                    "prompt_cache_miss_tokens"):
            value = usage.get(key)
            if isinstance(value, int) and not isinstance(value, bool):
                samples[key] = samples.get(key, 0) + int(value)
