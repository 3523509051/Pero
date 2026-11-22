"""高德 Web 服务客户端：固定端点、全局持久额度、逐段计次、无 Key 泄露。

移植自 webot 插件 `amap.py`（端点/消歧/分段语义保持一致），差异：

- HTTP：aiohttp → httpx（P08 同款：无重定向、响应字节上限、固定端点白名单）；
- 额度：json → `amap-quota.sqlite3` 持久预留；**全局每日上限**：当天所有 kind
  （district/weather/places/route）合计共用 `daily_limit`（默认 100），在
  `BEGIN IMMEDIATE` 内查询当天合计后才决定放行；每个真实 API 请求仍单独计次
  （城市消歧、天气、地点、每一段路线各计一次）；取消与失败同样计入——预留已发生，
  不返还；按 UTC+8 日期键，重启不重置；已有数据超过上限时明确拒绝，不清零/删库；
- 取消：请求前与请求后各查一次（`is_cancelled`），取消返回受控异常；
- 凭据只从环境变量名读取；错误只透出 infocode 白名单脱敏值，不透 URL/Key/响应体；
  网络层日志（httpx/httpcore）由 `install_network_log_redaction()` 安装的固定
  过滤器统一把 `key=` 查询参数脱敏为 `***`——不是临时切日志级别，过滤器随进程
  常驻，INFO/DEBUG 都不会输出完整带 Key 的 URL。
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import re
import sqlite3
from contextlib import closing
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Optional

import httpx

AMAP_BASE = "https://restapi.amap.com/v3/"
#: 实际会调用的端点白名单（相对 AMAP_BASE）；模型不能传任意路径。
ALLOWED_PATHS = (
    "config/district",
    "weather/weatherInfo",
    "place/text",
    "direction/driving",
    "direction/walking",
    "direction/transit/integrated",
)

#: 网络层日志脱敏：把 URL 查询参数里的 key 值整体替换，不区分大小写。
_KEY_IN_URL = re.compile(r"([?&]key=)[^&\s\"']+", re.IGNORECASE)


class _NetworkKeyRedactor(logging.Filter):
    """固定网络日志过滤器：脱敏记录文本中的 `key=...` 查询参数。

    安装在 httpx/httpcore 相关 logger 与根 handler 上；对每条经过的日志记录
    统一重写 msg 并清空 args（args 里的 URL 同样可能带 Key），进程生命周期内
    常驻，不随请求切换级别，并发请求互不影响。
    """

    def filter(self, record: logging.LogRecord) -> bool:  # noqa: A003
        try:
            text = record.getMessage() if record.args else str(record.msg)
        except Exception:  # noqa: BLE001 - 消息格式化失败时退回原文脱敏
            text = str(record.msg)
        record.msg = _KEY_IN_URL.sub(r"\1***", text)
        record.args = None
        return True


_REDACTOR = _NetworkKeyRedactor()
_REDACTED_LOGGERS = ("httpx", "httpcore")


def install_network_log_redaction() -> None:
    """安装一次网络日志脱敏过滤器（幂等）。

    覆盖范围：
    - `httpx` / `httpcore` 及其全部现有子 logger（发请求方自己打 URL 的地方）；
    - 根 logger 的所有 handler（子 logger 的记录向上传播时最终经 handler 输出，
      handler 级过滤器对传播记录同样生效，覆盖之后新建的子 logger）。

    这是固定的传输日志脱敏机制，不是临时切换全局级别；业务 logger 不受影响。
    """

    seen = set()
    for name in _REDACTED_LOGGERS:
        base = logging.getLogger(name)
        targets = [base, *(
            child
            for child in _iter_loggers(name)
            if child is not base
        )]
        for logger_obj in targets:
            if id(logger_obj) not in seen:
                logger_obj.addFilter(_REDACTOR)
                seen.add(id(logger_obj))
    root = logging.getLogger()
    for handler in root.handlers:
        if id(handler) not in seen:
            handler.addFilter(_REDACTOR)
            seen.add(id(handler))


def _iter_loggers(prefix: str):
    manager = logging.Logger.manager.loggerDict
    for name in list(manager):
        if name == prefix or name.startswith(prefix + "."):
            logger_obj = manager[name]
            if isinstance(logger_obj, logging.Logger):
                yield logger_obj


class AmapError(Exception):
    """受控高德错误：`code` 是脱敏错误码，不携带 URL/Key/响应体。"""

    def __init__(self, code: str, detail: str = "") -> None:
        self.code = code
        super().__init__(detail or code)


def _utc8_day() -> str:
    return datetime.now(timezone(timedelta(hours=8))).date().isoformat()


class AMapQuota:
    """持久每日额度：**全局合计上限**；发出前预留，取消/失败不返还（预留已发生）。

    district/weather/places/route 各真实请求分别计次，但共用当天 `limit` 次业务
    上限：`reserve` 在 `BEGIN IMMEDIATE` 内先查当天**所有 kind 合计**，达到上限
    直接拒绝；只有通过才给对应 kind 加一。已有分类数据合计超过上限时明确拒绝，
    不清零、不删库重置。
    """

    def __init__(self, path: Path) -> None:
        self.path = path

    def reserve(self, kind: str, limit: int) -> bool:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        day = _utc8_day()
        with closing(sqlite3.connect(str(self.path), timeout=3)) as conn, conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS quota("
                "day TEXT, kind TEXT, used INTEGER, PRIMARY KEY(day, kind))"
            )
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT COALESCE(SUM(used), 0) FROM quota WHERE day=?", (day,)
            ).fetchone()
            if int(row[0] if row else 0) >= int(limit):
                return False
            conn.execute(
                "INSERT INTO quota VALUES(?,?,1) "
                "ON CONFLICT(day, kind) DO UPDATE SET used=used+1",
                (day, kind),
            )
            conn.execute(
                "DELETE FROM quota WHERE day < ?",
                ((datetime.now(timezone.utc) - timedelta(days=32)).date().isoformat(),),
            )
        return True

    def usage(self) -> dict:
        """只读计数（状态接口用；不含任何查询内容）：当天合计 + 分类统计。"""

        day = _utc8_day()
        if not self.path.exists():
            return {"day": day, "total": 0, "kinds": {}}
        with closing(sqlite3.connect(str(self.path), timeout=3)) as conn:
            rows = conn.execute(
                "SELECT kind, used FROM quota WHERE day=?", (day,)
            ).fetchall()
        kinds = {str(k): int(v) for k, v in rows}
        return {"day": day, "total": sum(kinds.values()), "kinds": kinds}


def _cancelled() -> bool:
    task = asyncio.current_task()
    if task is None:
        return False
    cancelling = getattr(task, "cancelling", None)
    return bool(cancelling() if callable(cancelling) else False)


class AMapClient:
    """固定端点客户端；一个进程一个实例（httpx 连接复用），工具共享。"""

    def __init__(
        self,
        *,
        api_key: str,
        quota: AMapQuota,
        daily_limit: int,
        timeout_seconds: float,
        max_response_bytes: int,
        places_limit: int = 5,
        is_cancelled: Optional[Callable[[], bool]] = None,
    ) -> None:
        self._key = api_key
        self._quota = quota
        self._daily_limit = int(daily_limit)
        self._timeout = float(timeout_seconds)
        self._max_bytes = int(max_response_bytes)
        self._places_limit = int(places_limit)
        self._is_cancelled = is_cancelled or _cancelled

    async def aclose(self) -> None:
        """连接池随请求结束关闭；保留方法与后端生命周期对齐。"""
        return None

    @property
    def configured(self) -> bool:
        return bool(self._key)

    # ------------------------------------------------------------------ #
    # 请求
    # ------------------------------------------------------------------ #

    async def _request(self, kind: str, path: str, **params) -> dict:
        if path not in ALLOWED_PATHS:
            raise AmapError("endpoint_not_allowed")
        if self._is_cancelled():
            raise AmapError("cancelled", "请求已取消")
        if not self._key:
            raise AmapError("api_key_missing", "尚未配置高德 Web 服务 API Key")
        # 发出前持久预留；取消/失败不返还（预留已发生，重启不重置）。
        if not await asyncio.to_thread(self.quota_reserve, kind):
            raise AmapError("daily_limit", "今日高德请求额度已用完")
        try:
            data = await self._fetch(path, params)
        except asyncio.CancelledError:
            raise
        except httpx.HTTPStatusError as exc:
            raise AmapError(f"http_{exc.response.status_code}") from None
        except httpx.HTTPError:
            raise AmapError("network_error") from None
        if not isinstance(data, dict) or data.get("status") != "1":
            code = "unknown"
            if isinstance(data, dict):
                code = re.sub(r"[^A-Za-z0-9]", "", str(data.get("infocode") or ""))[:20]
            raise AmapError(f"amap_{code or 'unknown'}", "高德请求未成功")
        if self._is_cancelled():
            raise AmapError("cancelled", "请求已取消")
        return data

    async def _fetch(self, path: str, params: dict) -> dict:
        # 本客户端按请求创建（AmapBackend.client()），连接池随请求结束关闭，
        # 不跨请求复用，避免每次调用泄漏一个 AsyncClient。
        # trust_env=False：不读代理环境变量；不跟随重定向（禁止带 Key 跳转别站）。
        async with httpx.AsyncClient(follow_redirects=False, trust_env=False) as client:
            async with client.stream(
                "GET",
                AMAP_BASE + path,
                params={**params, "key": self._key, "output": "JSON"},
                timeout=self._timeout,
            ) as response:
                if response.status_code != 200:
                    raise httpx.HTTPStatusError(
                        "http error", request=response.request, response=response
                    )
                body = bytearray()
                async for chunk in response.aiter_bytes():
                    body.extend(chunk)
                    if len(body) > self._max_bytes:
                        raise AmapError("response_too_large")
        try:
            data = json.loads(bytes(body))
        except (ValueError, UnicodeError):
            raise AmapError("invalid_response") from None
        if not isinstance(data, dict):
            raise AmapError("invalid_response")
        return data

    def quota_reserve(self, kind: str) -> bool:
        return self._quota.reserve(kind, self._daily_limit)

    # ------------------------------------------------------------------ #
    # 业务（端点/参数与旧 amap.py 一致）
    # ------------------------------------------------------------------ #

    async def weather(self, city: str, forecast: bool = False) -> dict:
        city = (city or "").strip()
        if not city or len(city) > 40:
            raise AmapError(
                "city_required", "请先提供要查询天气的城市或区县，不能根据群名猜测位置"
            )
        if re.fullmatch(r"\d{6}", city):
            adcode = city
        else:
            data = await self._request(
                "district", "config/district", keywords=city, subdistrict=0,
                extensions="base",
            )
            districts = [d for d in data.get("districts", []) if isinstance(d, dict)]
            candidates = [
                d
                for d in districts
                if str(d.get("name", "")).removesuffix("市") == city.removesuffix("市")
            ]
            if len(candidates) != 1:
                choices = [
                    {"name": d.get("name"), "adcode": d.get("adcode")}
                    for d in districts[:5]
                ]
                raise AmapError(
                    "city_ambiguous",
                    "城市不明确或未找到，请提供具体城市/区县或行政区编码："
                    + json.dumps(choices, ensure_ascii=False),
                )
            district = candidates[0]
            if district.get("level") not in ("city", "district") and district.get(
                "name"
            ) not in ("北京市", "上海市", "天津市", "重庆市"):
                raise AmapError(
                    "city_too_broad", "请提供具体城市或区县，不能查询整个省的天气"
                )
            adcode = str(district.get("adcode", ""))
            if not re.fullmatch(r"\d{6}", adcode):
                raise AmapError("invalid_adcode", "高德返回无效行政区编码")
        data = await self._request(
            "weather",
            "weather/weatherInfo",
            city=adcode,
            extensions="all" if forecast else "base",
        )
        entries = data.get("forecasts" if forecast else "lives") or []
        entries = [
            item
            for item in entries
            if isinstance(item, dict) and str(item.get("adcode")) == adcode
        ]
        if not entries:
            raise AmapError(
                "no_weather_data",
                "高德没有返回该城市的天气数据，请稍后再试，不要改用搜索或编造天气",
            )
        item = entries[0]
        if forecast:
            result = {
                key: str(item.get(key, ""))[:100]
                for key in ("province", "city", "adcode", "reporttime")
            }
            keys = (
                "date", "week", "dayweather", "nightweather", "daytemp",
                "nighttemp", "daywind", "nightwind", "daypower", "nightpower",
            )
            result["casts"] = [
                {key: str(day.get(key, ""))[:100] for key in keys}
                for day in item.get("casts", [])[:4]
                if isinstance(day, dict)
            ]
        else:
            result = {
                key: str(item.get(key, ""))[:100]
                for key in (
                    "province", "city", "adcode", "reporttime", "weather",
                    "temperature", "winddirection", "windpower", "humidity",
                )
            }
        return {
            "source": "高德天气",
            "forecast": bool(forecast),
            **result,
            "note": (
                "以 reporttime 为更新时间；预报仅覆盖返回日期，不推测远期假期天气。"
                "温度单位℃，湿度单位%。"
            ),
        }

    async def places(self, city: str, keywords: str) -> list:
        city = (city or "").strip()
        keywords = (keywords or "").strip()
        if not city or not keywords:
            raise AmapError(
                "params_required", "请提供城市和景点/地点关键词"
            )
        data = await self._request(
            "places",
            "place/text",
            city=city[:40],
            keywords=keywords[:100],
            citylimit="true",
            offset=self._places_limit,
            page=1,
            extensions="base",
        )
        return [
            {
                key: str(poi.get(key, ""))[:250]
                for key in ("id", "name", "address", "location", "cityname")
            }
            for poi in (data.get("pois", []) or [])[: self._places_limit]
            if isinstance(poi, dict)
        ]

    async def _location(self, name: str, city: str) -> dict:
        results = await self.places(city, name)
        exact = [item for item in results if item["name"] == name and item["location"]]
        if len(exact) == 1:
            return exact[0]
        if len(results) == 1 and results[0]["location"]:
            return results[0]
        if not results:
            raise AmapError("place_not_found", "未找到地点：" + name[:60])
        raise AmapError(
            "place_ambiguous",
            "地点名称不明确，请先选定具体地点和地址："
            + json.dumps(results, ensure_ascii=False),
        )

    async def route(self, city: str, stops, mode: str = "driving") -> dict:
        mode = (mode or "").strip()
        city = (city or "").strip()
        if mode not in ("driving", "walking", "transit"):
            raise AmapError("mode_unsupported", "交通方式仅支持 driving、walking、transit")
        stops = [str(s).strip() for s in (stops or []) if str(s).strip()]
        if not city or not 2 <= len(stops) <= 6 or any(len(s) > 100 for s in stops):
            raise AmapError(
                "stops_invalid", "请提供城市以及按游览顺序排列的 2～6 个地点"
            )
        places = [await self._location(stop, city) for stop in stops]
        for place in places:
            try:
                longitude, latitude = map(float, place["location"].split(","))
                valid = (
                    math.isfinite(longitude)
                    and math.isfinite(latitude)
                    and -180 <= longitude <= 180
                    and -90 <= latitude <= 90
                )
            except (ValueError, KeyError, TypeError):
                valid = False
            if not valid:
                raise AmapError("invalid_coordinates", "高德返回无效坐标，已停止路线查询")
        legs = []
        for origin, destination in zip(places, places[1:]):
            params = {
                "origin": origin["location"],
                "destination": destination["location"],
            }
            path = "direction/" + mode
            if mode == "transit":
                # 跨城公交需要 cityd，旧实现刻意不猜；同城场景两者相同。
                path += "/integrated"
                params.update(city=city[:40], cityd=city[:40])
            # 每段单独计次（kind=route）。
            data = await self._request("route", path, **params)
            route = data.get("route") or {}
            options = route.get("transits" if mode == "transit" else "paths") or []
            if not options:
                raise AmapError(
                    f"route_no_{mode}", "高德未找到这一段可用路线"
                )
            choice = options[0]
            instructions = [
                str(step.get("instruction", ""))[:200]
                for step in choice.get("steps", [])[:8]
            ]
            if mode == "transit":
                instructions = []
                for segment in choice.get("segments", [])[:8]:
                    lines = (segment.get("bus") or {}).get("buslines") or []
                    instructions.extend(
                        str(line.get("name", ""))[:200] for line in lines[:1]
                    )
            legs.append(
                {
                    "from": origin["name"],
                    "to": destination["name"],
                    "distance_m": choice.get("distance"),
                    "duration_s": choice.get("duration"),
                    "instructions": instructions,
                }
            )
        return {
            "source": "高德地图",
            "city": city,
            "mode": mode,
            "places": places,
            "legs": legs,
            "note": (
                "路线按用户给定顺序查询，非全局最优；交通耗时不含游玩、排队、用餐。"
                "请确认日期、预算和偏好再排日程，门票及开放时间需另查。"
            ),
        }
