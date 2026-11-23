"""OneBot v11 反向 WebSocket 接收入口（默认关闭）。

Pero 扮演 OneBot 服务端：本地桥主动连入 `ws://127.0.0.1:<port><path>`。
本阶段只做解析、身份归一化、会话路由、短期去重与元数据统计：
不触发模型、不自动发送回复、不保存消息正文。

账号隔离（P03-R01）：
- 每个连接必须带合法的 `X-Self-ID` 头，连接身份来自该头，不从帧内容或"最后连接者"猜账号；
- 同账号第二连接按显式策略**拒绝**（不替换、不接管）；
- 入站帧的 `self_id` 必须与连接账号一致，不一致即拒绝并计数；
- 出站动作必须带显式账号上下文，按该账号找连接；echo 只认同一条连接上的 action_id；
- 单连接断开只结束它自己的待确认动作，其他账号不受影响。
"""

from __future__ import annotations

import asyncio
import dataclasses
import json
import re
import logging
import os
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple

from pero.channels.dedupe import BoundedDedupeCache
from pero.channels.onebot11.mapping import (
    CHANNEL_ID,
    ChannelActionError,
    assert_action_scope,
    build_media_action_payload,
    build_media_batch_payload,
    encode_action,
    media_batch_receipt_from_echo,
    media_receipt_from_echo,
    normalize_payload,
    receipt_from_echo,
    valid_account_id,
)
from pero.core.contracts import (
    ChannelEvent,
    ChannelEventKind,
    OutboundAction,
    QueryResult,
    QueryStatus,
    SendReceipt,
    SendStatus,
)
from pero.core.settings import OneBotChannelSettings

logger = logging.getLogger(__name__)

EventHandler = Callable[[ChannelEvent], Awaitable[None]]

#: 同账号重复连接的策略：拒绝新连接，保留已有连接（不替换、不轮流接管）。
DUPLICATE_CONNECTION_POLICY = "reject_new"

#: P13-R01：固定允许的**只读查询** action 白名单。配置里的 action 字符串
#: 必须命中白名单才发帧；不在白名单一律 query_rejected——不把配置字符串
#: 当成任意桥 RPC 入口。
QUERY_ACTIONS = frozenset(
    {"pero_file_bytes_v1", "pero_file_chunk_v1", "pero_quote_image_v1"}
)


def _utc() -> str:
    from pero.core.timebase import now_iso

    return now_iso()


@dataclass
class _Connection:
    """一个已绑定账号的入站连接。"""

    conn_id: str
    connection: Any
    account_id: str
    role: str
    remote: str = ""
    connected_at: str = field(default_factory=_utc)
    last_event_at: Optional[str] = None
    last_heartbeat_at: Optional[str] = None


@dataclass
class _PendingAction:
    """一次等待 echo 的出站动作；归属某个账号、某条连接与一次唯一发送。

    - `action_id`：稳定业务标识，用于"同连接未完成的同业务动作"重复拒绝；
    - `wire_echo`：本次发送唯一的传输关联标识，echo 只按它匹配；
    - future 完成后登记仍由发送协程自己持有，直到它按**对象身份**清理。
    """

    wire_echo: str
    action_id: str
    account_id: str
    conn_id: str
    future: asyncio.Future


class OneBotChannelServer:
    """OneBot v11 接收适配器（按账号路由连接与回执）。"""

    channel_id = CHANNEL_ID

    def __init__(
        self,
        settings: OneBotChannelSettings,
        *,
        on_event: Optional[EventHandler] = None,
        dedupe: Optional[BoundedDedupeCache] = None,
    ) -> None:
        self._settings = settings
        self._on_event = on_event
        self._dedupe = dedupe or BoundedDedupeCache(
            max_entries=settings.dedupe_max_entries,
            ttl_seconds=settings.dedupe_ttl_seconds,
        )
        self._server: Any = None
        self._listening = False
        self._bound_port: Optional[int] = None
        self._connections: Dict[str, _Connection] = {}
        self._account_connections: Dict[str, str] = {}
        #: 键为 (conn_id, wire_echo)：一次实际发送一条登记
        self._pending: Dict[Tuple[str, str], _PendingAction] = {}
        #: 键为 (conn_id, action_id)：业务层重复拒绝用，未完成期间占位
        self._business_index: Dict[Tuple[str, str], _PendingAction] = {}
        self._token: Optional[str] = None
        #: P13-R01：接收侧文件 offer 处理器（同步有界入队；未装配为 None）。
        self._file_offer_handler = None

        self._counters: Dict[str, int] = {
            "frames": 0,
            "messages": 0,
            "meta_events": 0,
            "heartbeats": 0,
            "action_echoes": 0,
            "duplicates_dropped": 0,
            "rejected_events": 0,
            "protocol_errors": 0,
            "auth_rejected": 0,
            "handler_errors": 0,
            "rejected_connections": 0,
            "duplicate_connections": 0,
            "account_mismatch": 0,
            "unknown_echoes": 0,
            "duplicate_echoes": 0,
            "duplicate_action_ids": 0,
            #: WebSocket 帧已写出（不等于发送成功）
            "frames_sent": 0,
            #: 动作在编码/校验阶段被拒绝（尚未发出帧）
            "action_rejected": 0,
            #: 按桥回执判定的投递结果（sent/failed/unknown）——只统计发送动作
            "receipt_sent": 0,
            "receipt_failed": 0,
            "receipt_unknown": 0,
            #: P13-R01：只读查询（QueryResult 语义，独立于发送回执计数）
            "query_ok": 0,
            "query_failed": 0,
            "query_unknown": 0,
            "query_rejected": 0,
        }
        self._last_error_code: Optional[str] = None
        self._last_connected_at: Optional[str] = None
        self._last_disconnected_at: Optional[str] = None
        self._last_event_at: Optional[str] = None
        self._last_heartbeat_at: Optional[str] = None
        self._disabled_reason: str = "config_disabled"

    def set_event_handler(self, handler: Optional[EventHandler]) -> None:
        """装配后注入事件订阅者（编排器）。回调必须是同步、非阻塞的入队操作。"""

        self._on_event = handler

    def set_media_handler(self, handler) -> None:
        """Synchronous bounded enqueue; raw inline bytes never enter the public event contract."""
        self._media_handler = handler

    def set_status_emitter(self, emitter) -> None:
        """P15-R01：注入组件状态回调（真实连接生命周期变化）。

        回调签名 `(state: str, *, conn_id, account, error="")`；由装配层接到
        SSE 总线发 `component.status`。通道自身不做网络/发布逻辑。
        """
        self._status_emitter = emitter

    def _emit_component_status(
        self, state: str, *, conn_id: str = "", account: str = "", error: str = ""
    ) -> None:
        emitter = getattr(self, "_status_emitter", None)
        if emitter is None:
            return
        try:
            emitter(state, conn_id=conn_id, account=account, error=error)
        except Exception:  # noqa: BLE001 - 状态发布失败不影响通道
            logger.debug("component.status 发布失败", exc_info=True)

    def set_file_offer_handler(self, handler) -> None:
        """P13-R01：注入桥文件 offer 接收侧处理器（同步、有界入队，不做网络 IO）。

        与媒体处理器并列：在**接收阶段**（账号归属校验 + 去重之后）同步调用，
        不进模型批次——文件单独到达/无模型凭据/自动处理关闭时，只要补传
        开启仍会保存原件。处理器异常只计数，不影响消息正常投递。
        """

        self._file_offer_handler = handler

    # ------------------------------------------------------------------ #
    # 生命周期
    # ------------------------------------------------------------------ #

    @property
    def enabled(self) -> bool:
        return bool(self._settings.enabled)

    @property
    def listening(self) -> bool:
        return self._listening

    @property
    def connected(self) -> bool:
        """是否有**任意**账号在线；不代表所有账号都在线。"""

        return bool(self._connections)

    @property
    def online_accounts(self) -> List[str]:
        return sorted(self._account_connections)

    async def start(self) -> bool:
        """按配置启动接收；默认配置下什么都不监听。"""

        if not self.enabled:
            self._disabled_reason = "config_disabled"
            logger.info("onebot11 通道未启用（channels.onebot11.enabled=false），不监听端口")
            return False
        if self._server is not None:
            return True

        self._token = os.environ.get(self._settings.token_env, "").strip() or None
        try:
            from websockets.asyncio.server import serve
        except Exception as exc:  # noqa: BLE001 - 依赖缺失要如实报告
            self._last_error_code = "dependency_missing"
            logger.error("onebot11 通道启动失败：websockets 不可用（%s）", type(exc).__name__)
            return False

        try:
            self._server = await serve(
                self._handle_connection,
                self._settings.host,
                self._settings.port,
                max_size=self._settings.max_frame_bytes,
                ping_interval=20,
                ping_timeout=20,
                process_request=self._process_request,
            )
        except OSError as exc:
            self._last_error_code = "bind_failed"
            self._server = None
            logger.error(
                "onebot11 通道无法监听 %s:%s（%s）",
                self._settings.host,
                self._settings.port,
                type(exc).__name__,
            )
            return False
        except Exception as exc:  # noqa: BLE001
            self._last_error_code = "start_failed"
            self._server = None
            logger.error("onebot11 通道启动异常（%s）", type(exc).__name__)
            return False

        self._listening = True
        self._disabled_reason = ""
        self._bound_port = self._settings.port
        logger.info(
            "onebot11 通道已监听 ws://%s:%s%s（仅本机，未连接任何桥）",
            self._settings.host,
            self._bound_port,
            self._settings.path,
        )
        return True

    async def stop(self) -> None:
        server = self._server
        self._server = None
        self._listening = False
        for conn in list(self._connections.values()):
            await self._safe_close(conn.connection)
        self._connections.clear()
        self._account_connections.clear()
        self._resolve_all_pending(None)
        if server is not None:
            try:
                server.close()
                wait_closed = getattr(server, "wait_closed", None)
                if wait_closed is not None:
                    await wait_closed()
            except Exception as exc:  # noqa: BLE001
                logger.warning("onebot11 通道关闭异常（%s）", type(exc).__name__)
        logger.info("onebot11 通道已停止")

    # ------------------------------------------------------------------ #
    # 连接与帧处理
    # ------------------------------------------------------------------ #

    def _process_request(self, connection: Any, request: Any) -> Any:
        """只接受配置的路径；其它路径返回 404，不暴露实现细节。"""

        try:
            path = getattr(request, "path", "") or ""
            base = path.split("?", 1)[0]
            if self._settings.path and base != self._settings.path:
                respond = getattr(connection, "respond", None)
                if respond is not None:
                    return respond(404, "not found\n")
        except Exception:  # noqa: BLE001 - 路径判定失败不应阻断接收
            return None
        return None

    async def _handle_connection(self, connection: Any) -> None:
        conn_id = uuid.uuid4().hex[:8]
        headers = self._headers_of(connection)

        if not self._token_ok(headers):
            self._counters["auth_rejected"] += 1
            self._last_error_code = "auth_rejected"
            logger.warning("onebot11 连接被拒：令牌不匹配（conn=%s）", conn_id)
            await self._safe_close(connection, code=1008, reason="unauthorized")
            return

        # 连接身份只来自握手头；不从帧内容或"最后一个连接"推断。
        account_id = valid_account_id(headers.get("x-self-id"))
        if account_id is None:
            self._counters["rejected_connections"] += 1
            self._last_error_code = "missing_self_id"
            logger.warning("onebot11 连接被拒：缺少合法 X-Self-ID（conn=%s）", conn_id)
            await self._safe_close(connection, code=1008, reason="missing self id")
            return

        existing_conn_id = self._account_connections.get(account_id)
        if existing_conn_id is not None:
            # 同账号重复连接：拒绝新连接，保留已有连接，不做静默替换。
            self._counters["duplicate_connections"] += 1
            self._last_error_code = "duplicate_account_connection"
            logger.warning(
                "onebot11 同账号重复连接被拒 account=%s，保留 conn=%s",
                account_id,
                existing_conn_id,
            )
            await self._safe_close(connection, code=1008, reason="account already connected")
            return

        remote = ""
        try:
            remote = str(getattr(connection, "remote_address", "") or "")
        except Exception:  # noqa: BLE001
            remote = ""
        conn = _Connection(
            conn_id=conn_id,
            connection=connection,
            account_id=account_id,
            role=str(headers.get("x-client-role") or ""),
            remote=remote,
        )
        self._connections[conn_id] = conn
        self._account_connections[account_id] = conn_id
        self._last_connected_at = _utc()
        self._last_error_code = None
        self._emit_component_status("connected", conn_id=conn_id, account=account_id)
        logger.info(
            "onebot11 桥已连接 conn=%s account=%s role=%s remote=%s",
            conn_id,
            account_id,
            conn.role or "-",
            remote or "-",
        )
        try:
            async for raw in connection:
                await self._on_frame(raw, conn)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - 断开是常态，只记类型
            self._last_error_code = "connection_error"
            logger.warning("onebot11 连接异常 conn=%s error=%s", conn_id, type(exc).__name__)
        finally:
            self._connections.pop(conn_id, None)
            if self._account_connections.get(conn.account_id) == conn_id:
                self._account_connections.pop(conn.account_id, None)
            self._emit_component_status(
                "disconnected", conn_id=conn_id, account=conn.account_id,
                error=self._last_error_code or "",
            )
            # 只结束这条连接自己的待确认动作，其他账号继续等待。
            self._resolve_pending_for_connection(conn_id, None)
            self._last_disconnected_at = _utc()
            logger.info("onebot11 连接结束 conn=%s account=%s", conn_id, conn.account_id)

    @staticmethod
    def _headers_of(connection: Any) -> Dict[str, str]:
        try:
            request = getattr(connection, "request", None)
            headers = getattr(request, "headers", None) or {}
            return {str(key).lower(): str(value) for key, value in headers.items()}
        except Exception:  # noqa: BLE001
            return {}

    def _token_ok(self, headers: Dict[str, str]) -> bool:
        if not self._token:
            return True
        provided = headers.get("authorization", "")
        if provided.startswith("Bearer "):
            provided = provided[len("Bearer ") :]
        return provided.strip() == self._token

    async def _on_frame(self, raw: Any, conn: _Connection) -> None:
        self._counters["frames"] += 1
        if isinstance(raw, (bytes, bytearray)):
            try:
                text = bytes(raw).decode("utf-8")
            except UnicodeDecodeError:
                self._counters["protocol_errors"] += 1
                self._last_error_code = "invalid_encoding"
                return
        elif isinstance(raw, str):
            text = raw
        else:
            self._counters["protocol_errors"] += 1
            self._last_error_code = "invalid_frame"
            return

        try:
            payload = json.loads(text)
        except (json.JSONDecodeError, ValueError):
            # 不打印原帧内容，避免把群聊正文写进日志。
            self._counters["protocol_errors"] += 1
            self._last_error_code = "invalid_json"
            return

        event = normalize_payload(payload)

        if event.kind is ChannelEventKind.ACTION_ECHO:
            self._counters["action_echoes"] += 1
            self._resolve_pending(conn.conn_id, event.event_id, payload)
            return

        if event.kind is ChannelEventKind.UNKNOWN:
            self._counters["rejected_events"] += 1
            self._last_error_code = event.reject_code or "unknown_event"
            logger.info(
                "onebot11 事件被拒绝 conn=%s account=%s code=%s",
                conn.conn_id,
                conn.account_id,
                event.reject_code or "unknown",
            )
            return

        # 帧里的账号必须与连接账号一致：不允许帧自行冒充另一个账号。
        if event.account_id != conn.account_id:
            self._counters["account_mismatch"] += 1
            self._last_error_code = "account_mismatch"
            logger.warning(
                "onebot11 账号不匹配已拒绝 conn=%s conn_account=%s frame_account=%r",
                conn.conn_id,
                conn.account_id,
                event.account_id,
            )
            return

        if event.kind is ChannelEventKind.META:
            self._counters["meta_events"] += 1
            if event.meta_event_type == "heartbeat":
                self._counters["heartbeats"] += 1
                self._last_heartbeat_at = _utc()
                conn.last_heartbeat_at = self._last_heartbeat_at
            self._last_event_at = _utc()
            conn.last_event_at = self._last_event_at
            return

        message = event.message
        if message is None:  # 防御：MESSAGE 必须有正文对象
            self._counters["rejected_events"] += 1
            self._last_error_code = "missing_message"
            return

        accepted = self._dedupe.observe(message.account_id, message.event_id)
        self._last_event_at = _utc()
        conn.last_event_at = self._last_event_at
        if not accepted:
            self._counters["duplicates_dropped"] += 1
            logger.info(
                "onebot11 重复事件已丢弃 account=%s event=%s",
                message.account_id,
                message.event_id,
            )
            return

        self._counters["messages"] += 1
        offer_handler = getattr(self, "_file_offer_handler", None)
        if offer_handler is not None:
            try:
                offer_handler(event)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - 入队失败不影响消息投递
                self._counters["handler_errors"] += 1
                logger.warning(
                    "onebot11 文件 offer 入队异常 error=%s", type(exc).__name__
                )
        if self._on_event is None:
            # 没有订阅者：只统计，不触发模型、不回复。
            return
        try:
            import inspect
            media=getattr(self,"_media_handler",None)
            result=media(event,payload) if media is not None else self._on_event(event)
            if inspect.isawaitable(result):
                await result
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - 订阅者异常不影响接收循环
            self._counters["handler_errors"] += 1
            self._last_error_code = "handler_error"
            logger.warning("onebot11 事件处理异常 error=%s", type(exc).__name__)

    # ------------------------------------------------------------------ #
    # 出站动作（内部能力，本阶段不开放 HTTP 入口）
    # ------------------------------------------------------------------ #

    def _connection_for_account(self, account_id: str) -> Optional[_Connection]:
        conn_id = self._account_connections.get(account_id)
        if conn_id is None:
            return None
        return self._connections.get(conn_id)

    async def send_action(
        self,
        action: OutboundAction,
        *,
        account_id: str,
        conversation_type: str,
        target_ref: str,
        timeout: Optional[float] = None,
    ) -> SendReceipt:
        """按**显式账号**向该账号的活动连接发送动作，并按 echo 判定回执。

        无该账号连接 → `unknown`（不回退其他账号）；会话标识与账号/类型/目标不一致 →
        `failed`；同一连接上重复 action_id → 发送前 `failed`；超时/断连/无 echo → `unknown`。
        任何情况都不自动重发。
        """

        normalized_account = valid_account_id(account_id)
        if normalized_account is None or normalized_account != str(account_id):
            return self._action_failure(
                action.action_id, "invalid_account", "动作缺少合法账号上下文"
            )

        try:
            assert_action_scope(
                action,
                account_id=normalized_account,
                conversation_type=conversation_type,
                target_ref=target_ref,
            )
        except ChannelActionError as exc:
            return self._action_failure(action.action_id, exc.code, exc.message)

        conn = self._connection_for_account(normalized_account)
        if conn is None:
            self._counters["receipt_unknown"] += 1
            return SendReceipt(
                status=SendStatus.UNKNOWN,
                detail="该账号没有活动连接",
                action_id=action.action_id,
                evidence=f"账号 {normalized_account} 无在线连接，不回退其他账号",
            )

        action_id = action.action_id or f"act-{uuid.uuid4().hex[:12]}"
        business_key = (conn.conn_id, action_id)
        if business_key in self._business_index:
            # 同一连接上仍有未完成的同业务动作：发送前拒绝，绝不覆盖已有 future。
            self._counters["duplicate_action_ids"] += 1
            return self._action_failure(
                action_id,
                "duplicate_action_id",
                "该连接上已有同名待确认动作，拒绝覆盖",
            )

        # 每次实际发送用唯一 wire echo：复用的业务 action_id 不会接收旧动作迟到/重复的 echo。
        wire_echo = f"we-{uuid.uuid4().hex}"
        scoped_action = dataclasses.replace(action, action_id=action_id)
        try:
            payload = encode_action(
                scoped_action,
                account_id=normalized_account,
                conversation_type=conversation_type,
                target_ref=target_ref,
                wire_echo=wire_echo,
            )
        except ChannelActionError as exc:
            return self._action_failure(action_id, exc.code, exc.message)

        loop = asyncio.get_running_loop()
        future: asyncio.Future = loop.create_future()
        pending = _PendingAction(
            wire_echo=wire_echo,
            action_id=action_id,
            account_id=normalized_account,
            conn_id=conn.conn_id,
            future=future,
        )
        pending_key = (conn.conn_id, wire_echo)
        self._pending[pending_key] = pending
        self._business_index[business_key] = pending
        try:
            await conn.connection.send(json.dumps(payload, ensure_ascii=False))
            # 只代表帧已写出，不代表桥已执行或已送达。
            self._counters["frames_sent"] += 1
            echo_payload = await asyncio.wait_for(
                future, timeout=timeout or self._settings.action_timeout_seconds
            )
        except asyncio.TimeoutError:
            self._counters["receipt_unknown"] += 1
            return SendReceipt(
                status=SendStatus.UNKNOWN,
                detail="等待动作回执超时",
                action_id=action_id,
                evidence="超时未收到 echo",
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - 发送失败/断连
            self._counters["receipt_unknown"] += 1
            return SendReceipt(
                status=SendStatus.UNKNOWN,
                detail="动作发送过程中连接异常",
                action_id=action_id,
                evidence=f"发送异常（{type(exc).__name__}）",
            )
        finally:
            # 按对象身份清理：只删自己这一次登记。
            # 若该键已被断连/stop 清理、或已被别的对象占用，这里一律不动。
            self._release_pending(pending)

        if not isinstance(echo_payload, dict):
            self._counters["receipt_unknown"] += 1
            return SendReceipt(
                status=SendStatus.UNKNOWN,
                detail="连接已断开，未收到回执",
                action_id=action_id,
                evidence="echo 缺失（连接结束）",
            )
        receipt = receipt_from_echo(echo_payload, action_id=action_id)
        if receipt.status is SendStatus.SENT:
            self._counters["receipt_sent"] += 1
        elif receipt.status is SendStatus.FAILED:
            self._counters["receipt_failed"] += 1
        else:
            self._counters["receipt_unknown"] += 1
        return receipt

    async def send_media_action(
        self,
        *,
        action: str,
        action_id: str,
        account_id: str,
        conversation_type: str,
        target_ref: str,
        text: str,
        image_base64: str,
        image_name: str,
        image_sha256: str,
        timeout: Optional[float] = None,
    ) -> Tuple[SendReceipt, Dict[str, Any]]:
        """版本化媒体动作（P12 `pero_media_send_v1`）：文字+单图一次投递。

        与 `send_action` 同一套隔离规则：显式账号、目标数字 ID、唯一 wire_echo、
        同连接业务 action_id 去重、超时/断连一律 unknown、绝不自动重发。
        返回 `(聚合回执, 分项事实)`；分项事实（text_state/image_state…）由
        `mapping.media_receipt_from_echo` 解析，整体状态只作聚合参考。
        """

        normalized_account = valid_account_id(account_id)
        if normalized_account is None or normalized_account != str(account_id):
            return self._action_failure(action_id, "invalid_account", "动作缺少合法账号上下文"), {
                "text_state": "unknown",
                "image_state": "skipped",
                "evidence": "invalid_account",
            }
        conn = self._connection_for_account(normalized_account)
        if conn is None:
            self._counters["receipt_unknown"] += 1
            return (
                SendReceipt(
                    status=SendStatus.UNKNOWN,
                    detail="该账号没有活动连接",
                    action_id=action_id,
                    evidence=f"账号 {normalized_account} 无在线连接，不回退其他账号",
                ),
                # 未调用不算已发送：分项如实 unknown/skipped。
                {
                    "text_state": "unknown",
                    "image_state": "skipped",
                    "evidence": "no_connection",
                },
            )
        business_key = (conn.conn_id, action_id)
        if business_key in self._business_index:
            self._counters["duplicate_action_ids"] += 1
            return (
                self._action_failure(
                    action_id,
                    "duplicate_action_id",
                    "该连接上已有同名待确认动作，拒绝覆盖",
                ),
                {
                    "text_state": "unknown",
                    "image_state": "skipped",
                    "evidence": "duplicate_action_id",
                },
            )
        wire_echo = f"we-{uuid.uuid4().hex}"
        try:
            payload = build_media_action_payload(
                action=action,
                action_id=action_id,
                conversation_type=conversation_type,
                target_ref=target_ref,
                text=text,
                image_base64=image_base64,
                image_name=image_name,
                image_sha256=image_sha256,
                wire_echo=wire_echo,
            )
        except ChannelActionError as exc:
            return self._action_failure(action_id, exc.code, exc.message), {
                "text_state": "unknown",
                "image_state": "skipped",
                "evidence": exc.code,
            }

        loop = asyncio.get_running_loop()
        future: asyncio.Future = loop.create_future()
        pending = _PendingAction(
            wire_echo=wire_echo,
            action_id=action_id,
            account_id=normalized_account,
            conn_id=conn.conn_id,
            future=future,
        )
        self._pending[(conn.conn_id, wire_echo)] = pending
        self._business_index[business_key] = pending
        try:
            await conn.connection.send(json.dumps(payload, ensure_ascii=False))
            self._counters["frames_sent"] += 1
            echo_payload = await asyncio.wait_for(
                future, timeout=timeout or self._settings.action_timeout_seconds
            )
        except asyncio.TimeoutError:
            self._counters["receipt_unknown"] += 1
            return (
                SendReceipt(
                    status=SendStatus.UNKNOWN,
                    detail="等待媒体动作回执超时",
                    action_id=action_id,
                    evidence="超时未收到 echo",
                ),
                # 超时/断连/异常不代表未发出：分项如实 unknown，不补造 sent。
                {"text_state": "unknown", "image_state": "unknown", "evidence": "media_timeout"},
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            self._counters["receipt_unknown"] += 1
            return (
                SendReceipt(
                    status=SendStatus.UNKNOWN,
                    detail="媒体动作发送过程中连接异常",
                    action_id=action_id,
                    evidence=f"发送异常（{type(exc).__name__}）",
                ),
                {"text_state": "unknown", "image_state": "unknown", "evidence": "media_send_exception"},
            )
        finally:
            self._release_pending(pending)

        if not isinstance(echo_payload, dict):
            self._counters["receipt_unknown"] += 1
            return (
                SendReceipt(
                    status=SendStatus.UNKNOWN,
                    detail="连接已断开，未收到回执",
                    action_id=action_id,
                    evidence="echo 缺失（连接结束）",
                ),
                {"text_state": "unknown", "image_state": "unknown", "evidence": "echo_missing"},
            )
        text_state, facts = media_receipt_from_echo(echo_payload, action_id=action_id)
        if text_state == "sent":
            receipt = SendReceipt(
                status=SendStatus.SENT,
                detail="桥已确认文字发送（媒体分项见 facts）",
                action_id=action_id,
                evidence=facts.get("evidence") or "text_state=sent",
            )
            self._counters["receipt_sent"] += 1
        elif text_state == "failed":
            receipt = SendReceipt(
                status=SendStatus.FAILED,
                detail="桥报告文字发送失败",
                action_id=action_id,
                evidence=facts.get("evidence") or "text_state=failed",
            )
            self._counters["receipt_failed"] += 1
        else:
            receipt = SendReceipt(
                status=SendStatus.UNKNOWN,
                detail="桥未能确认文字发送",
                action_id=action_id,
                evidence=facts.get("evidence") or "text_state=unknown",
            )
            self._counters["receipt_unknown"] += 1
        return receipt, facts

    async def send_media_batch_action(
        self,
        *,
        action: str,
        action_id: str,
        account_id: str,
        conversation_type: str,
        target_ref: str,
        text: str,
        images: List[Dict[str, Any]],
        timeout: Optional[float] = None,
    ) -> Tuple[SendReceipt, Dict[str, Any]]:
        """版本化批量媒体动作（P12 `pero_media_send_v2`）：文字+最多 6 图。

        与 v1 同一套隔离规则；返回值 `(聚合回执, 分项事实)`，分项事实包含
        逐图 `images[]`（index/state/error）与 `batch_state`。任何一项
        failed/unknown 只如实记录，绝不自动整批重发。
        """

        expected = len(images or [])
        blank_images = [
            {"index": index, "state": "skipped", "asset_id": "", "error": ""}
            for index in range(expected)
        ]

        def _fail_facts(text_state: str, evidence: str) -> Dict[str, Any]:
            return {
                "text_state": text_state,
                "images": [dict(item) for item in blank_images],
                "batch_state": "unknown",
                "evidence": evidence,
            }

        normalized_account = valid_account_id(account_id)
        if normalized_account is None or normalized_account != str(account_id):
            return self._action_failure(
                action_id, "invalid_account", "动作缺少合法账号上下文"
            ), _fail_facts("unknown", "invalid_account")
        if not images:
            return self._action_failure(
                action_id, "empty_media", "批量媒体动作缺少图片"
            ), _fail_facts("unknown", "empty_media")
        conn = self._connection_for_account(normalized_account)
        if conn is None:
            self._counters["receipt_unknown"] += 1
            return (
                SendReceipt(
                    status=SendStatus.UNKNOWN,
                    detail="该账号没有活动连接",
                    action_id=action_id,
                    evidence=f"账号 {normalized_account} 无在线连接，不回退其他账号",
                ),
                _fail_facts("unknown", "no_connection"),
            )
        business_key = (conn.conn_id, action_id)
        if business_key in self._business_index:
            self._counters["duplicate_action_ids"] += 1
            return (
                self._action_failure(
                    action_id,
                    "duplicate_action_id",
                    "该连接上已有同名待确认动作，拒绝覆盖",
                ),
                _fail_facts("unknown", "duplicate_action_id"),
            )
        wire_echo = f"we-{uuid.uuid4().hex}"
        try:
            payload = build_media_batch_payload(
                action=action,
                action_id=action_id,
                conversation_type=conversation_type,
                target_ref=target_ref,
                text=text,
                images=images,
                wire_echo=wire_echo,
                # 低于两端 WebSocket 帧上限（桥为 16 MiB），留 1 MiB 信封余量。
                max_payload_bytes=max(
                    0,
                    min(int(self._settings.max_frame_bytes), 16 * 1024 * 1024)
                    - 1024 * 1024,
                ),
            )
        except ChannelActionError as exc:
            return self._action_failure(action_id, exc.code, exc.message), _fail_facts(
                "unknown", exc.code
            )

        loop = asyncio.get_running_loop()
        future: asyncio.Future = loop.create_future()
        pending = _PendingAction(
            wire_echo=wire_echo,
            action_id=action_id,
            account_id=normalized_account,
            conn_id=conn.conn_id,
            future=future,
        )
        self._pending[(conn.conn_id, wire_echo)] = pending
        self._business_index[business_key] = pending
        try:
            await conn.connection.send(json.dumps(payload, ensure_ascii=False))
            self._counters["frames_sent"] += 1
            echo_payload = await asyncio.wait_for(
                future, timeout=timeout or self._settings.action_timeout_seconds
            )
        except asyncio.TimeoutError:
            self._counters["receipt_unknown"] += 1
            return (
                SendReceipt(
                    status=SendStatus.UNKNOWN,
                    detail="等待批量媒体动作回执超时",
                    action_id=action_id,
                    evidence="超时未收到 echo",
                ),
                {
                    "text_state": "unknown",
                    "images": [
                        {"index": index, "state": "unknown", "asset_id": "", "error": "timeout"}
                        for index in range(expected)
                    ],
                    "batch_state": "unknown",
                    "evidence": "media_batch_timeout",
                },
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            self._counters["receipt_unknown"] += 1
            return (
                SendReceipt(
                    status=SendStatus.UNKNOWN,
                    detail="批量媒体动作发送过程中连接异常",
                    action_id=action_id,
                    evidence=f"发送异常（{type(exc).__name__}）",
                ),
                {
                    "text_state": "unknown",
                    "images": [
                        {"index": index, "state": "unknown", "asset_id": "", "error": "exception"}
                        for index in range(expected)
                    ],
                    "batch_state": "unknown",
                    "evidence": "media_batch_exception",
                },
            )
        finally:
            self._release_pending(pending)

        if not isinstance(echo_payload, dict):
            self._counters["receipt_unknown"] += 1
            return (
                SendReceipt(
                    status=SendStatus.UNKNOWN,
                    detail="连接已断开，未收到回执",
                    action_id=action_id,
                    evidence="echo 缺失（连接结束）",
                ),
                {
                    "text_state": "unknown",
                    "images": [
                        {"index": index, "state": "unknown", "asset_id": "", "error": "echo_missing"}
                        for index in range(expected)
                    ],
                    "batch_state": "unknown",
                    "evidence": "echo_missing",
                },
            )
        text_state, facts = media_batch_receipt_from_echo(
            echo_payload,
            action_id=action_id,
            expected=expected,
            expected_asset_ids=[str(item.get("asset_id") or "") for item in images],
        )
        if text_state == "sent":
            receipt = SendReceipt(
                status=SendStatus.SENT,
                detail="桥已确认文字发送（逐图分项见 facts）",
                action_id=action_id,
                evidence=facts.get("evidence") or "text_state=sent",
            )
            self._counters["receipt_sent"] += 1
        elif text_state == "failed":
            receipt = SendReceipt(
                status=SendStatus.FAILED,
                detail="桥报告文字发送失败",
                action_id=action_id,
                evidence=facts.get("evidence") or "text_state=failed",
            )
            self._counters["receipt_failed"] += 1
        else:
            receipt = SendReceipt(
                status=SendStatus.UNKNOWN,
                detail="桥未能确认文字发送",
                action_id=action_id,
                evidence=facts.get("evidence") or "text_state=unknown",
            )
            self._counters["receipt_unknown"] += 1
        return receipt, facts

    async def _query_bridge(
        self,
        *,
        action: str,
        params: Dict[str, Any],
        query_id: str,
        account_id: str,
        timeout: Optional[float] = None,
        _control: bool = False,
    ) -> Tuple[QueryResult, Dict[str, Any]]:
        """P13-R01 统一只读查询信封（四层结构，字节对象固定在第 3→4 层）：

        1. 桥 action 返回 ``{"ok": bool, "error": ...}`` 或
           ``{"ok": True, "data": {...字节对象...}}``（bridge 层）；
        2. OneBot echo 又把这**整个桥返回值**放进 ``data``：
           ``{"status":"ok","retcode":0,"data":<桥返回>,"echo":...}``（echo 层）；
        3. 本方法解开 echo 层 → 校验桥层 ``ok`` → 返回**字节对象**
           （即桥返回里的 ``data["data"]``）：worker 拿到的 dict 顶层就是
           ``content_base64`` 等字段，不再需要二次解包；
        4. worker（独立任务）消费字节对象：校验、解码、入库。

        语义：返回 ``QueryResult``（ok/failed/unknown）并记独立 ``query_*``
        计数；**不**产生 SendReceipt、不进 receipt_sent/failed/unknown。
        """

        allowed = {"wake_screen","sleep_screen"} if _control else QUERY_ACTIONS
        if action not in allowed:
            self._counters["query_rejected"] += 1
            return (
                QueryResult(
                    status=QueryStatus.FAILED,
                    query_id=query_id,
                    detail="查询 action 不在只读白名单",
                    evidence="query_action_not_allowed",
                ),
                {},
            )
        normalized_account = valid_account_id(account_id)
        if normalized_account is None or normalized_account != str(account_id):
            self._counters["query_rejected"] += 1
            return (
                QueryResult(
                    status=QueryStatus.FAILED,
                    query_id=query_id,
                    detail="查询缺少合法账号上下文",
                    evidence="invalid_account",
                ),
                {},
            )
        conn = self._connection_for_account(normalized_account)
        if conn is None:
            self._counters["query_unknown"] += 1
            return (
                QueryResult(
                    status=QueryStatus.UNKNOWN,
                    query_id=query_id,
                    detail="该账号没有活动连接",
                    evidence="no_connection",
                ),
                {},
            )
        wire_echo = f"wq-{uuid.uuid4().hex}"
        payload = {"action": action, "params": params, "echo": wire_echo}
        loop = asyncio.get_running_loop()
        future: asyncio.Future = loop.create_future()
        pending = _PendingAction(
            wire_echo=wire_echo,
            action_id=query_id,
            account_id=normalized_account,
            conn_id=conn.conn_id,
            future=future,
        )
        self._pending[(conn.conn_id, wire_echo)] = pending
        try:
            await conn.connection.send(json.dumps(payload, ensure_ascii=False))
            self._counters["frames_sent"] += 1
            echo_payload = await asyncio.wait_for(
                future, timeout=timeout or self._settings.action_timeout_seconds
            )
        except asyncio.TimeoutError:
            self._counters["query_unknown"] += 1
            return (
                QueryResult(
                    status=QueryStatus.UNKNOWN,
                    query_id=query_id,
                    detail="查询超时",
                    evidence="query_timeout",
                ),
                {},
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            self._counters["query_unknown"] += 1
            return (
                QueryResult(
                    status=QueryStatus.UNKNOWN,
                    query_id=query_id,
                    detail="查询发送过程中连接异常",
                    evidence=f"query_send_exception:{type(exc).__name__}",
                ),
                {},
            )
        finally:
            self._release_pending(pending)

        if not isinstance(echo_payload, dict):
            self._counters["query_unknown"] += 1
            return (
                QueryResult(
                    status=QueryStatus.UNKNOWN,
                    query_id=query_id,
                    detail="连接已断开，未收到查询回执",
                    evidence="echo_missing",
                ),
                {},
            )
        envelope = echo_payload.get("data")
        envelope = envelope if isinstance(envelope, dict) else {}
        if echo_payload.get("status") != "ok" or envelope.get("ok") is not True:
            error = str(envelope.get("error") or "query_failed")[:60]
            self._counters["query_failed"] += 1
            return (
                QueryResult(
                    status=QueryStatus.FAILED,
                    query_id=query_id,
                    detail=f"桥报告查询失败：{error}",
                    evidence=f"query_error:{error}",
                ),
                {},
            )
        if _control:
            self._counters['control_accepted']=self._counters.get('control_accepted',0)+1
            return QueryResult(status=QueryStatus.OK,query_id=query_id,detail='屏幕动作已受理，完成状态需实机确认',evidence='control_accepted'), envelope
        byte_object = envelope.get("data")
        if not isinstance(byte_object, dict):
            # ok 但缺字节对象层：真实信封必须能被消费，形状不对按失败处理，
            # 不用自造的扁平结果顶替。
            self._counters["query_failed"] += 1
            return (
                QueryResult(
                    status=QueryStatus.FAILED,
                    query_id=query_id,
                    detail="查询信封缺少字节对象层",
                    evidence="query_envelope_invalid",
                ),
                {},
            )
        self._counters["query_ok"] += 1
        return (
            QueryResult(
                status=QueryStatus.OK,
                query_id=query_id,
                detail="查询成功（不代表任何消息 sent）",
                evidence="query_ok",
            ),
            byte_object,
        )


    async def control_screen(self, *, action: str, account_id: str):
        """Explicit host screen command; never exposed as a model tool or sent receipt."""
        return await self._query_bridge(action=action,params={},query_id='screen-'+uuid.uuid4().hex,
            account_id=account_id,_control=True)

    async def query_file_bytes(
        self,
        *,
        action: str,
        query_id: str,
        account_id: str,
        target_ref: str,
        file_id: str,
        timeout: Optional[float] = None,
        conversation_type: str = "group",
    ) -> Tuple[QueryResult, Dict[str, Any]]:
        """P13 只读文件字节查询（`pero_file_bytes_v1`）：**独立查询语义**。

        固定参数白名单（账号 + 群数字 ID + 桥文件句柄），不接受路径/URL/SQL；
        wire_echo/账号归属/超时/取消沿用同一套隔离规则；发送方在**独立
        worker 任务**里调用——接收协程绝不等待自己连接的 echo。
        """

        if not re.fullmatch(r"f_\d{4,8}", str(file_id or "")):
            self._counters["query_rejected"] += 1
            return (
                QueryResult(
                    status=QueryStatus.FAILED,
                    query_id=query_id,
                    detail="文件句柄格式不合法",
                    evidence="invalid_file_id",
                ),
                {},
            )
        if conversation_type not in {"group", "private"}:
            self._counters["query_rejected"] += 1
            return (QueryResult(status=QueryStatus.FAILED, query_id=query_id,
                                detail="不支持的会话类型", evidence="invalid_conversation_type"), {})
        try:
            group_id = int(target_ref)
        except (TypeError, ValueError):
            self._counters["query_rejected"] += 1
            return (
                QueryResult(
                    status=QueryStatus.FAILED,
                    query_id=query_id,
                    detail="目标群 ID 不合法",
                    evidence="invalid_target",
                ),
                {},
            )
        return await self._query_bridge(
            action=action,
            params={**({"user_id": group_id, "conversation_type": "private"}
                       if conversation_type == "private" else {"group_id": group_id}),
                    "file_id": str(file_id)[:40]},
            query_id=query_id,
            account_id=account_id,
            timeout=timeout,
        )

    async def query_file_chunk(
        self,
        *,
        action: str,
        query_id: str,
        account_id: str,
        target_ref: str,
        file_id: str,
        offset: int,
        length: int,
        timeout: Optional[float] = None,
        conversation_type: str = "group",
    ) -> Tuple[QueryResult, Dict[str, Any]]:
        """P13 分块只读文件查询（`pero_file_chunk_v1`）：大文件端到端传输。

        参数白名单：账号 + 群数字 ID + 桥文件句柄 + offset/length（原始字节）。
        `length=0` 只取元数据（size/sha256/eof），不返回内容；单块上限 8 MiB，
        与文件总大小限制分开。与 `query_file_bytes` 共用查询信封与隔离规则。
        """

        if not re.fullmatch(r"f_\d{4,8}", str(file_id or "")):
            self._counters["query_rejected"] += 1
            return (
                QueryResult(
                    status=QueryStatus.FAILED,
                    query_id=query_id,
                    detail="文件句柄格式不合法",
                    evidence="invalid_file_id",
                ),
                {},
            )
        if isinstance(offset, bool) or not isinstance(offset, int) or offset < 0:
            self._counters["query_rejected"] += 1
            return (
                QueryResult(
                    status=QueryStatus.FAILED,
                    query_id=query_id,
                    detail="分块 offset 不合法",
                    evidence="invalid_offset",
                ),
                {},
            )
        if (
            isinstance(length, bool)
            or not isinstance(length, int)
            or length < 0
            or length > 8_388_608
        ):
            self._counters["query_rejected"] += 1
            return (
                QueryResult(
                    status=QueryStatus.FAILED,
                    query_id=query_id,
                    detail="分块 length 不合法",
                    evidence="invalid_length",
                ),
                {},
            )
        if conversation_type not in {"group", "private"}:
            self._counters["query_rejected"] += 1
            return (QueryResult(status=QueryStatus.FAILED, query_id=query_id,
                                detail="不支持的会话类型", evidence="invalid_conversation_type"), {})
        try:
            group_id = int(target_ref)
        except (TypeError, ValueError):
            self._counters["query_rejected"] += 1
            return (
                QueryResult(
                    status=QueryStatus.FAILED,
                    query_id=query_id,
                    detail="目标群 ID 不合法",
                    evidence="invalid_target",
                ),
                {},
            )
        return await self._query_bridge(
            action=action,
            params={
                **({"user_id": group_id, "conversation_type": "private"}
                   if conversation_type == "private" else {"group_id": group_id}),
                "file_id": str(file_id)[:40],
                "offset": offset,
                "length": length,
            },
            query_id=query_id,
            account_id=account_id,
            timeout=timeout,
        )

    async def query_quote_image(
        self,
        *,
        action: str,
        query_id: str,
        account_id: str,
        target_ref: str,
        image_md5: str,
        refer_time: str = "",
        timeout: Optional[float] = None,
        conversation_type: str = "group",
    ) -> Tuple[QueryResult, Dict[str, Any]]:
        """P13-R01 只读引用原图查询（`pero_quote_image_v1`）：与文件字节同一信封。

        参数白名单：账号 + 群数字 ID + 32 位 md5 + 可选 refer_time；桥按
        当前群查找链解析本地副本，找不到即明确失败（不拿其他图顶替）。
        """

        normalized_md5 = str(image_md5 or "").strip().lower()
        if not re.fullmatch(r"[0-9a-f]{32}", normalized_md5):
            self._counters["query_rejected"] += 1
            return (
                QueryResult(
                    status=QueryStatus.FAILED,
                    query_id=query_id,
                    detail="引用图片 md5 格式不合法",
                    evidence="invalid_image_md5",
                ),
                {},
            )
        if conversation_type not in {"group", "private"}:
            self._counters["query_rejected"] += 1
            return (QueryResult(status=QueryStatus.FAILED, query_id=query_id,
                                detail="不支持的会话类型", evidence="invalid_conversation_type"), {})
        try:
            group_id = int(target_ref)
        except (TypeError, ValueError):
            self._counters["query_rejected"] += 1
            return (
                QueryResult(
                    status=QueryStatus.FAILED,
                    query_id=query_id,
                    detail="目标群 ID 不合法",
                    evidence="invalid_target",
                ),
                {},
            )
        return await self._query_bridge(
            action=action,
            params={
                **({"user_id": group_id, "conversation_type": "private"}
                   if conversation_type == "private" else {"group_id": group_id}),
                "image_md5": normalized_md5,
                "refer_time": str(refer_time or "")[:20],
            },
            query_id=query_id,
            account_id=account_id,
            timeout=timeout,
        )

    def _action_failure(
        self, action_id: Optional[str], code: str, message: str
    ) -> SendReceipt:
        self._counters["action_rejected"] += 1
        self._last_error_code = code
        return SendReceipt(
            status=SendStatus.FAILED,
            detail=message,
            action_id=action_id,
            evidence=f"动作被拒绝：{code}",
        )

    def _resolve_pending(
        self, conn_id: Optional[str], wire_echo: Optional[str], payload: Any
    ) -> None:
        """完成 echo 对应的 future，但**不**释放发送协程仍持有的登记。

        登记由拥有它的那个 `send_action` 在自己的 `finally` 里按对象身份释放，
        因此"完成 future → 新动作注册 → 旧协程 finally"这种交错不会误删新登记。
        """

        if not wire_echo or not conn_id:
            return
        pending = self._pending.get((conn_id, wire_echo))
        if pending is None:
            # 未知/过期/来自其他连接/已被释放的 echo：忽略并计数。
            self._counters["unknown_echoes"] += 1
            self._last_error_code = "unknown_echo"
            logger.info("onebot11 未匹配的 echo 已忽略 conn=%s", conn_id)
            return
        if pending.future.done():
            # 重复 echo：已完成的任务不能被反复确认，也不影响后续同名业务动作。
            self._counters["duplicate_echoes"] += 1
            self._last_error_code = "duplicate_echo"
            logger.info("onebot11 重复 echo 已忽略 conn=%s", conn_id)
            return
        pending.future.set_result(payload)

    def _release_pending(self, pending: _PendingAction) -> None:
        """按对象身份移除一次登记；键已被别的对象占用时一律不动。"""

        pending_key = (pending.conn_id, pending.wire_echo)
        if self._pending.get(pending_key) is pending:
            self._pending.pop(pending_key, None)
        business_key = (pending.conn_id, pending.action_id)
        if self._business_index.get(business_key) is pending:
            self._business_index.pop(business_key, None)

    def _resolve_pending_for_connection(self, conn_id: str, payload: Any) -> None:
        """只结束这条连接自己的待确认动作。"""

        for pending in [item for item in self._pending.values() if item.conn_id == conn_id]:
            if not pending.future.done():
                pending.future.set_result(payload)
            self._release_pending(pending)

    def _resolve_all_pending(self, payload: Any) -> None:
        for pending in list(self._pending.values()):
            if not pending.future.done():
                pending.future.set_result(payload)
        self._pending.clear()
        self._business_index.clear()

    async def _safe_close(
        self, connection: Any, *, code: int = 1000, reason: str = ""
    ) -> None:
        try:
            result = connection.close(code=code, reason=reason)
            if asyncio.iscoroutine(result):
                await result
        except Exception:  # noqa: BLE001
            return

    # ------------------------------------------------------------------ #
    # 状态（脱敏元数据）
    # ------------------------------------------------------------------ #

    def status(self) -> Dict[str, Any]:
        dedupe_stats = self._dedupe.stats()
        accounts = [
            {
                "account_id": conn.account_id,
                "conn_id": conn.conn_id,
                "role": conn.role,
                "connected_at": conn.connected_at,
                "last_event_at": conn.last_event_at,
                "last_heartbeat_at": conn.last_heartbeat_at,
                "pending_actions": sum(
                    1 for key in self._pending if key[0] == conn.conn_id
                ),
            }
            for conn in sorted(self._connections.values(), key=lambda item: item.account_id)
        ]
        return {
            "channel_id": CHANNEL_ID,
            "implementation_delivered": True,
            "enabled": self.enabled,
            "listening": self._listening,
            "connected": self.connected,
            "host": self._settings.host,
            "port": self._bound_port or self._settings.port,
            "path": self._settings.path,
            "disabled_reason": self._disabled_reason if not self.enabled else "",
            "connections": len(self._connections),
            "online_accounts": self.online_accounts,
            "accounts": accounts,
            "duplicate_connection_policy": DUPLICATE_CONNECTION_POLICY,
            "token_required": bool(self._token),
            "last_connected_at": self._last_connected_at,
            "last_disconnected_at": self._last_disconnected_at,
            "last_event_at": self._last_event_at,
            "last_heartbeat_at": self._last_heartbeat_at,
            "last_error_code": self._last_error_code,
            "counters": dict(self._counters),
            "dedupe": dedupe_stats,
            "pending_actions": len(self._pending),
            "notes": [
                "本阶段只解析与统计：不触发模型、不自动回复、不发送任何消息。",
                "frames_sent 只表示 WebSocket 帧已写出，不代表发送成功；投递结论看 receipt_*。",
                "connected 表示有任意账号在线，不代表所有账号在线；账号级状态见 accounts。",
                "同账号重复连接按 reject_new 处理：拒绝新连接，保留已有连接。",
                "message_id 为桥进程内序列号，去重范围为本桥进程；重启后不保证跨重启去重。",
                "原始群 wxid / 发送者 wxid / 引用消息不在事件字段内，统一标为未知。",
            ],
        }

    @property
    def settings(self) -> OneBotChannelSettings:
        return self._settings


__all__ = [
    "DUPLICATE_CONNECTION_POLICY",
    "EventHandler",
    "OneBotChannelServer",
]
