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


def _now_local() -> str:
    from datetime import datetime

    try:
        from zoneinfo import ZoneInfo

        tz = ZoneInfo("Asia/Shanghai")
    except Exception:  # noqa: BLE001
        from datetime import timedelta, timezone

        tz = timezone(timedelta(hours=8))
    return datetime.now(tz).replace(tzinfo=None).isoformat(timespec="seconds")


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
            },
            "events": {"enabled": bool(memory.l3_enabled), "independent_cursor": True},
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
        if next_at and next_at > _now_local():
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
            # L2 独立交互归档（条数触发）；与原始聊天归档彼此独立。
            result["l2"] = await self.maybe_archive_l2(
                agent_id=agent_id, conversation_id=conversation_id,
                memory=memory, provider=provider, force=force,
            )
            result["archive"] = await self._guarded(memory=memory,agent_id=agent_id,conversation_id=conversation_id,kind='archive',provider=provider,force=force,scanner=self.maybe_archive)
            result["facts"] = int((result.get("l2") or {}).get("facts_written") or 0)
            result["events"] = await self._guarded(
                memory=memory, agent_id=agent_id, conversation_id=conversation_id,
                kind="events", provider=provider, force=force,
                scanner=self.scan_events,
            )
        result["llm_calls"] = self._counters["llm_calls"] - before_calls
        try:
            counts = await memory.chat_counts(
                agent_id=agent_id, conversation_id=conversation_id
            )
            event_cursor = await memory.chat_cursor_get(
                agent_id=agent_id, conversation_id=conversation_id, kind="event"
            )
            result["last_seq"] = int(counts["last_seq"])
            if self._settings.memory.l3_enabled:
                # L3 no longer has an independent cursor to wait on.
                result["processed_seq"] = event_cursor
            else:
                result["processed_seq"] = int(counts["last_seq"])
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
            try:
                if self._settings.memory.l3_enabled:
                    summary,archive_facts=await self._summarise_l2_with_facts(provider,"\n\n".join(payload_parts),kept)
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
                committed = False
                logger.warning("L2 archive commit failed cid=%s error=%s",
                               conversation_id, type(exc).__name__)
            if not committed:
                self._counters["l2_failed"] += 1
                await self._settle_l2_safely(
                    memory, archive_id, agent_id, "failed", "commit_rejected"
                )
                return None
            self._counters["l2_completed"] += 1
            self._counters["l2_interactions_archived"] += len(kept)
            facts_written=0
            if archive_facts:
                try:
                    facts_written=await memory.l3_from_archive(agent_id=agent_id,conversation_id=conversation_id,
                        archive_id=archive_id,facts=archive_facts)
                    self._counters['facts_written']+=facts_written
                except asyncio.CancelledError:raise
                except Exception as exc:
                    self._counters['facts_failed']+=1
                    logger.warning('archive_l3_write_failed type=%s',type(exc).__name__)
            return {
                "facts_written":facts_written,
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

    async def _summarise_l2_with_facts(self,provider,payload,items):
        """One paid request per archive; member evidence and summary stay separate."""
        sources={str(item.get('event_id')):item for item in items
                 if item.get('role')=='member' and item.get('kind','text')=='text'
                 and str(item.get('text') or '').strip() and not str(item.get('text')).lstrip().startswith('[')}
        policy=(L2_ARCHIVE_SYSTEM+'\n同时只从本批成员原话中筛选最多3条值得长期记住的人物事实。'
            '只保留明确的称呼、稳定偏好、个人背景、长期习惯，空数组完全正常。'
            '不保存临时动作、睡觉/熬夜/情绪/当前状态、发送图片文件、单次游戏、寒暄、玩笑、第三人转述或助手说的话。'
            '不得把前情摘要当作新证据；每条只能归属于该条原文说话者本人。'
            '输出JSON：{"summary":"...","facts":[{"source_event_id":"本批原话事件ID",'
            '"category":"name|stable_preference|personal_background|long_term_habit","content":"清楚独立的长期事实短句",'
            '"quote":"原话逐字片段"}]}。')
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
        if isinstance(rows,list):
            for row in rows[:3]:
                if not isinstance(row,dict):continue
                source=sources.get(str(row.get('source_event_id') or ''))
                content=str(row.get('content') or '').strip();quote=str(row.get('quote') or '').strip()
                if not source or row.get('category') not in categories or not 4<=len(content)<=160 or not 4<=len(quote)<=160:continue
                if quote not in source['text'] or transient.search(content+' '+quote):continue
                if any(item['content']==content for item in facts):continue
                facts.append({'source_event_id':str(source['event_id']),'content':content,'quote':quote,'category':row['category']})
        return summary,facts

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
