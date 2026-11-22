"""高德工具层：`amap_weather` / `amap_places` / `amap_route` 三个真实工具。

- 客户端与额度在 `tools/amap.py`；本文件只做工具规格、取消检查与受控输出；
- 凭据只读环境变量（`amap.api_key_env`）；缺 Key/禁用时明确返回不可用，底座照常启动；
- 结果只含裁剪后的业务字段 + 来源说明；每次实际 API 请求单独计次（持久化预留）；
- L20 只落受控摘要（由 `summaries.summarize_tool_outcome` 处理），不复制完整 API 返回。
"""

from __future__ import annotations

import asyncio
import os
import threading
from pathlib import Path
from typing import Any, Callable, Optional

from pero.core.contracts import ToolSpec
from pero.core.settings import AmapSettings
from pero.tools.amap import AMapClient, AMapQuota, AmapError

import logging

_logger = logging.getLogger(__name__)

CANCELLED_NOTE = "请求已取消。"
DISABLED_NOTE = "高德地图功能未启用（amap.enabled=false），如实告诉用户该功能当前关闭。"
UNKNOWN_NOTE = "高德请求未完成（内部错误）；不代表数据无效，请如实说明。"


def _task_cancelled() -> bool:
    task = asyncio.current_task()
    if task is None:
        return False
    cancelling = getattr(task, "cancelling", None)
    return bool(cancelling() if callable(cancelling) else False)


class AmapBackend:
    """进程级共享：持久额度 + 用量缓存 + 每次请求临时客户端（Key 每次从环境读取）。

    额度状态读取不在事件循环里直接 sqlite.connect：`usage()` 一律经
    `asyncio.to_thread` 执行，结果写入 `_usage_cache`；状态接口用
    `usage_snapshot()` 只读缓存快照。缓存由 `start()` 启动的常驻刷新任务与
    每次工具调用后的同步读共同维护，`aclose()` 负责取消任务，不留后台协程。
    """

    #: 用量缓存刷新间隔（秒）。
    USAGE_REFRESH_SECONDS = 30.0

    def __init__(self, settings: AmapSettings, data_root: Path) -> None:
        self.settings = settings
        self.quota = AMapQuota(data_root / "amap-quota.sqlite3")
        self._external_cancel: Optional[Callable[[], bool]] = None
        self._usage_lock = threading.Lock()
        self._usage_cache: dict = {"day": "", "total": 0, "kinds": {}}
        self._refresh_task: Optional[asyncio.Task] = None

    def usage_snapshot(self) -> dict:
        """状态接口只读快照：不触发磁盘 IO，不返回查询内容。"""

        with self._usage_lock:
            snapshot = dict(self._usage_cache)
            snapshot["kinds"] = dict(self._usage_cache.get("kinds") or {})
            return snapshot

    def _load_usage_sync(self) -> None:
        """线程里执行的实际读取；结果整体替换缓存。"""

        data = self.quota.usage()
        with self._usage_lock:
            self._usage_cache = data

    async def _load_usage(self) -> None:
        await asyncio.to_thread(self._load_usage_sync)

    async def start(self) -> None:
        """启动用量缓存与常驻刷新任务（幂等；进程生命周期内只跑一个）。"""

        if self._refresh_task is not None:
            return
        await self._load_usage()
        self._refresh_task = asyncio.create_task(self._refresh_loop())

    async def _refresh_loop(self) -> None:
        try:
            while True:
                await asyncio.sleep(self.USAGE_REFRESH_SECONDS)
                try:
                    await self._load_usage()
                except Exception as exc:  # noqa: BLE001 - 刷新失败下轮再试
                    _logger.warning(
                        "amap 用量缓存刷新失败 error=%s", type(exc).__name__
                    )
        except asyncio.CancelledError:
            raise

    async def aclose(self) -> None:
        """取消刷新任务；等待取消完成，不遗留后台协程。"""

        task = self._refresh_task
        self._refresh_task = None
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    def set_cancel_check(self, check: Callable[[], bool]) -> None:
        """由编排注入"本轮是否已取消"的检查；默认退回当前任务 cancelling()。"""

        self._external_cancel = check

    @property
    def enabled(self) -> bool:
        return bool(self.settings.enabled)

    @property
    def credentials_configured(self) -> bool:
        resolver=getattr(self,'credential_resolver',None)
        if resolver:return all(resolver.configured(self.settings.api_key_env,name,'https://restapi.amap.com') for name in ('amap_weather','amap_places','amap_route'))
        return bool(os.environ.get(self.settings.api_key_env, "").strip())

    def client(self, is_cancelled: Callable[[], bool], key=None) -> AMapClient:
        return AMapClient(
            api_key=key if key is not None else os.environ.get(self.settings.api_key_env, "").strip(),
            quota=self.quota,
            daily_limit=self.settings.daily_limit,
            timeout_seconds=self.settings.timeout_seconds,
            max_response_bytes=self.settings.max_response_bytes,
            places_limit=self.settings.places_limit,
            is_cancelled=is_cancelled,
        )

    def metadata(self) -> dict:
        return {
            "enabled": self.enabled,
            "credentials_configured": self.credentials_configured,
            "api_key_env": self.settings.api_key_env,
            "daily_limit": self.settings.daily_limit,
            "quota_kind": "per_request",
            "weather_fast_enabled": self.settings.weather_fast_enabled,
            "endpoints": "restapi.amap.com/v3 固定白名单",
            "connectivity": "not_verified",
        }

    async def execute(self, name: str, arguments: dict, is_cancelled: Callable[[], bool]) -> dict:
        if not self.enabled:
            return {"ok": False, "error_code": "amap_disabled", "detail": DISABLED_NOTE}
        if is_cancelled():
            return {"ok": False, "error_code": "cancelled", "detail": CANCELLED_NOTE}
        key=None
        resolver=getattr(self,'credential_resolver',None)
        if resolver:
            from pero.configuration.credentials import CredentialUnavailable
            try:key=await resolver.for_environment(self.settings.api_key_env,name,'https://restapi.amap.com')
            except CredentialUnavailable:return {'ok':False,'error_code':'credential_unavailable'}
        client = self.client(is_cancelled,key)
        try:
            if name == "amap_weather":
                city = str(arguments.get("city") or "")
                forecast = bool(arguments.get("forecast") or False)
                result = await asyncio.wait_for(
                    client.weather(city, forecast), self.settings.timeout_seconds + 2
                )
            elif name == "amap_places":
                result = {
                    "pois": await asyncio.wait_for(
                        client.places(
                            str(arguments.get("city") or ""),
                            str(arguments.get("keywords") or ""),
                        ),
                        self.settings.timeout_seconds + 2,
                    ),
                    "source": "高德地点搜索",
                }
            elif name == "amap_route":
                stops = arguments.get("stops")
                if isinstance(stops, str):
                    import json as _json

                    try:
                        stops = _json.loads(stops)
                    except ValueError:
                        return {
                            "ok": False,
                            "error_code": "invalid_stops",
                            "detail": "stops 必须是 JSON 字符串数组。",
                        }
                if not isinstance(stops, list):
                    return {
                        "ok": False,
                        "error_code": "invalid_stops",
                        "detail": "stops 必须是 2～6 个地点名的数组。",
                    }
                result = await asyncio.wait_for(
                    client.route(
                        str(arguments.get("city") or ""),
                        stops,
                        str(arguments.get("mode") or "driving"),
                    ),
                    self.settings.timeout_seconds * max(2, len(stops) - 1),
                )
            else:  # pragma: no cover - registry 只登记三个名字
                return {"ok": False, "error_code": "unknown_tool", "detail": name}
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - 错误脱敏：只给受控码与受控文案
            if isinstance(exc, AmapError):
                code = exc.code
                detail = str(exc.args[0]) if exc.args else ""
                if code == "cancelled":
                    return {"ok": False, "error_code": "cancelled", "detail": CANCELLED_NOTE}
                return {
                    "ok": False,
                    "error_code": code or "amap_error",
                    # detail 全部来自本模块固定文案；city_ambiguous 的候选是
                    # 裁剪后的 {name, adcode} 结构化 JSON，不含任意上游文本。
                    "detail": detail or "高德请求未完成；不代表数据无效，请如实说明。",
                }
            # 未知异常：不回传 str(exc)/args（可能携带上游响应体/URL/Key）。
            return {
                "ok": False,
                "error_code": f"amap_{type(exc).__name__}",
                "detail": UNKNOWN_NOTE,
            }
        finally:
            # 工具调用后同步刷新用量缓存（线程执行，不阻塞事件循环）。
            try:
                await self._load_usage()
            except Exception as exc:  # noqa: BLE001
                _logger.warning("amap 用量缓存更新失败 error=%s", type(exc).__name__)
        if is_cancelled():
            return {"ok": False, "error_code": "cancelled", "detail": CANCELLED_NOTE}
        result["untrusted_data"] = True
        return {"ok": True, "result": result}


class AmapTool:
    """规格与执行入口；与 WebTool 同型（ephemeral_result=True）。"""

    ephemeral_result = True

    _SPECS = {
        "amap_weather": (
            "查询高德实况天气或短期预报。天气问题直接用此工具，不需要用户明确要求联网。",
            {
                "type": "object",
                "properties": {
                    "city": {
                        "type": "string",
                        "description": "用户明确提供的城市/区县名或 6 位行政区编码；未知时先询问，不要猜。",
                        "maxLength": 40,
                    },
                    "forecast": {
                        "type": "boolean",
                        "description": "false 查询实况，true 查询明天或未来几天的预报。",
                    },
                },
                "required": ["city"],
                "additionalProperties": False,
            },
        ),
        "amap_places": (
            "查询高德景点、车站等地点。用户提到旅游/地点时按需调用；城市必须来自用户明确指定。",
            {
                "type": "object",
                "properties": {
                    "city": {"type": "string", "description": "用户明确指定的城市，不要猜测。", "maxLength": 40},
                    "keywords": {"type": "string", "description": "景点或地点名称。", "maxLength": 100},
                },
                "required": ["city", "keywords"],
                "additionalProperties": False,
            },
        ),
        "amap_route": (
            "按指定顺序查询高德多段路线，不猜测地名，不代替交通或门票预订。",
            {
                "type": "object",
                "properties": {
                    "city": {"type": "string", "description": "所有地点所在的同一城市。", "maxLength": 40},
                    "stops": {
                        "type": "array",
                        "items": {"type": "string"},
                        "minItems": 2,
                        "maxItems": 6,
                        "description": "按游览顺序排列的 2～6 个具体地点名。",
                    },
                    "mode": {
                        "type": "string",
                        "enum": ["driving", "walking", "transit"],
                        "description": "driving 驾车、walking 步行或 transit 公交。",
                    },
                },
                "required": ["city", "stops", "mode"],
                "additionalProperties": False,
            },
        ),
    }

    def __init__(self, name: str, backend: AmapBackend) -> None:
        self.tool_id = name
        self.backend = backend

    def spec(self) -> ToolSpec:
        description, parameters = self._SPECS[self.tool_id]
        return ToolSpec(name=self.tool_id, description=description, parameters=parameters)

    async def run(self, arguments: dict) -> dict:
        from pero.tools.context import current_tool_context

        context = current_tool_context()
        check = (context.is_cancelled if context is not None else None) or _task_cancelled
        outcome = await self.backend.execute(
            self.tool_id, dict(arguments), lambda: (check() or _task_cancelled())
        )
        if outcome.get("ok"):
            return outcome["result"]
        return {
            "error": outcome.get("error_code", "amap_error"),
            "detail": outcome.get("detail", "高德请求未完成。"),
        }
