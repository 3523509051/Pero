"""插件可用能力的受控门面。

插件只拿到本轮只读上下文和这里列出的能力；身份、会话、权限、宿主路径、
Key 和管理令牌都不由插件参数指定，也不能从这里取得。
"""

from __future__ import annotations

import asyncio
import hashlib
import uuid
import mimetypes
import json
import re
from contextvars import ContextVar
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Mapping, Optional

from pero.plugins.errors import PluginError
from pero.plugins.manifest import PluginManifest
from pero.resources.catalog import ResourceCatalogError, sniff_image_mime

_NAME = re.compile(r"^[A-Za-z0-9_.-]{1,80}$")
_RESOURCE_ID = re.compile(r"^r-[0-9a-f]{32}$")
_EVENT_TYPES = frozenset({"plugin.status", "plugin.progress", "plugin.record"})
_MAX_STORAGE_BYTES = 256 * 1024
_MAX_IMAGE_BYTES = 5 * 1024 * 1024


@dataclass(frozen=True)
class PluginRequestContext:
    """本轮真实运行标识与授权会话引用；字段只读，不含凭据。"""

    run_id: str
    agent_id: str
    conversation_id: str
    scope_ref: str
    is_cancelled: Callable[[], bool]
    channel_id: str = ""
    language: str = "zh-CN"
    timezone: str = "Asia/Shanghai"
    background_timeout: Optional[float] = None


def _scope_ref(agent_id: str, conversation_id: str) -> str:
    digest = hashlib.sha256(f"{agent_id}\0{conversation_id}".encode("utf-8")).hexdigest()
    return "scope-" + digest[:24]


class PluginStorage:
    """插件独立数据空间；只接受安全文件名，IO 全部走线程。"""

    def __init__(self, root: Path) -> None:
        self._root = Path(root)

    def _path(self, name: str) -> Path:
        if not isinstance(name, str) or not _NAME.fullmatch(name) or name in {".", ".."}:
            raise PluginError("invalid_storage_name", "存储名称不合法")
        path = (self._root / name).resolve()
        if path.parent != self._root.resolve() or not path.is_file():
            raise PluginError("storage_not_found", "插件存储项不存在")
        return path

    async def exists(self, name: str) -> bool:
        try:
            return self._path(name).exists()
        except PluginError:
            return False

    async def read(self, name: str) -> bytes:
        path = await asyncio.to_thread(self._path, name)
        data = await asyncio.to_thread(path.read_bytes)
        if len(data) > _MAX_STORAGE_BYTES:
            raise PluginError("storage_too_large", "插件存储项超过大小上限")
        return data

    async def write(self, name: str, data: bytes) -> None:
        if not isinstance(name, str) or not _NAME.fullmatch(name) or name in {".", ".."}:
            raise PluginError("invalid_storage_name", "存储名称不合法")
        if not isinstance(data, bytes) or not data or len(data) > _MAX_STORAGE_BYTES:
            raise PluginError("storage_too_large", "插件存储项大小不合法")
        root = self._root.resolve()

        def work() -> None:
            root.mkdir(parents=True, exist_ok=True)
            target = (root / name).resolve()
            if target.parent != root:
                raise PluginError("invalid_storage_name", "存储路径越界")
            temporary = root / (name + ".tmp")
            temporary.write_bytes(data)
            temporary.replace(target)

        await asyncio.to_thread(work)

    async def read_json(self, name: str) -> Any:
        try:
            return json.loads((await self.read(name)).decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            raise PluginError("storage_invalid_json", "插件存储项不是合法 JSON") from None

    async def write_json(self, name: str, value: Any) -> None:
        try:
            data = json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        except (TypeError, ValueError):
            raise PluginError("storage_invalid_json", "插件存储项无法编码为 JSON") from None
        await self.write(name, data)

    # ---- 有配额的独立缓存/产物空间（业务模块自己的 cache_dir） ----

    @property
    def cache_dir(self) -> Path:
        """仅属于当前插件的缓存目录；路径不由模型参数提供。"""

        return self._root.resolve() / "cache"

    def _inside(self, relative: str) -> Path:
        if not isinstance(relative, str) or not relative or len(relative) > 180:
            raise PluginError("invalid_storage_name", "缓存相对路径不合法")
        if any(part in {".", "..", ""} for part in relative.split("/")):
            raise PluginError("invalid_storage_name", "缓存相对路径越界")
        root = self._root.resolve()
        path = (root / relative).resolve()
        if path != root and root not in path.parents:
            raise PluginError("invalid_storage_name", "缓存相对路径越界")
        return path

    async def total_bytes(self) -> int:
        def work() -> int:
            root = self._root.resolve()
            return sum(p.stat().st_size for p in root.rglob("*") if p.is_file()) if root.exists() else 0

        return await asyncio.to_thread(work)

    async def write_blob(self, relative: str, data: bytes, *, quota_bytes: int = 32 * 1024 * 1024) -> str:
        path = self._inside(relative)
        if not isinstance(data, bytes) or len(data) > 5 * 1024 * 1024:
            raise PluginError("storage_too_large", "缓存项大小不合法")

        def work() -> None:
            root = self._root.resolve()
            root.mkdir(parents=True, exist_ok=True)
            used = sum(p.stat().st_size for p in root.rglob("*") if p.is_file())
            if used + len(data) > quota_bytes:
                raise PluginError("storage_quota_exceeded", "插件缓存配额不足")
            path.parent.mkdir(parents=True, exist_ok=True)
            temporary = path.with_name(path.name + ".tmp")
            temporary.write_bytes(data)
            temporary.replace(path)

        await asyncio.to_thread(work)
        return relative

    async def read_blob(self, relative: str) -> bytes:
        path = self._inside(relative)

        def work() -> bytes:
            if not path.is_file():
                raise PluginError("storage_not_found", "缓存项不存在")
            return path.read_bytes()

        return await asyncio.to_thread(work)

    async def list_blobs(self, prefix: str = "cache/") -> list:
        root = self._root.resolve()
        base = self._inside(prefix.rstrip("/")) if prefix else root

        def work() -> list:
            if not base.exists():
                return []
            return sorted(str(p.relative_to(root)).replace("\\", "/") for p in base.rglob("*") if p.is_file())

        return await asyncio.to_thread(work)

    async def delete_blob(self, relative: str) -> bool:
        path = self._inside(relative)

        def work() -> bool:
            if path.is_file():
                path.unlink()
                return True
            return False

        return await asyncio.to_thread(work)


class _ArtifactsFacade:
    def __init__(self, sdk: "PluginSDK") -> None:
        self._sdk = sdk

    async def publish(self, data: bytes, name: str, *, kind: str = 'document') -> dict:
        if kind not in ('image','document'):
            raise PluginError('artifact_invalid','不支持的产物类型')
        self._sdk.require('artifact.image' if kind=='image' else 'artifact.document')
        if not isinstance(data,bytes) or not data or len(data)>_MAX_IMAGE_BYTES or not _NAME.fullmatch(name):
            raise PluginError('artifact_invalid','产物类型、名称或大小不合法')
        if (sniff_image_mime(data) is not None) != (kind=='image'):
            raise PluginError('artifact_invalid','产物类型不匹配')
        if self._sdk._catalog is None:
            raise PluginError('artifact_unavailable','资源目录不可用')
        digest=hashlib.sha256(data).hexdigest()
        try:
            meta=await self._sdk._catalog.ingest(agent_id=self._sdk.context.agent_id,
                conversation_id=self._sdk.context.conversation_id,name=name,data=data,
                event_id=f'plugin:{self._sdk.plugin_id}:{self._sdk.context.run_id}:{digest}:{name}')
        except ResourceCatalogError as exc:
            raise PluginError('artifact_publish_failed',str(exc.code)) from None
        handle='artifact-'+uuid.uuid4().hex
        result={'handle':handle,'kind':kind,'name':name,'mime':meta.get('mime') or mimetypes.guess_type(name)[0] or 'application/octet-stream','size':len(data),'sha256':digest}
        self._sdk._published[handle]={**result,'resource_id':meta['resource_id']}
        return result

    async def attach(self, handle: str) -> dict:
        item=self._sdk._published.get(handle)
        if item is None:raise PluginError('artifact_not_found','未知本轮产物')
        if item['kind']!='image':raise PluginError('attachment_unavailable','文档可发布，当前尚未开放插件自动附文档')
        return await self._sdk.images.attach(item['resource_id'],reason='plugin.artifact')

    async def publish_image(self, data: bytes, name: str = "artifact.png") -> str:
        """校验图片真实性后导入当前会话资源，返回本会话可用的 r-ID。"""
        self._sdk.require("artifact.image")
        if not isinstance(data, bytes) or not data or len(data) > _MAX_IMAGE_BYTES:
            raise PluginError("artifact_invalid", "图片产物大小不合法")
        if not isinstance(name, str) or not _NAME.fullmatch(name):
            raise PluginError("artifact_invalid", "图片产物名称不合法")
        mime = sniff_image_mime(data)
        if mime is None:
            raise PluginError("artifact_not_image", "产物不是可验证的图片")
        if not name.lower().endswith((".png", ".jpg", ".jpeg", ".gif", ".webp")):
            name = "artifact.png" if mime == "image/png" else "artifact." + mime.split("/")[-1]
        try:
            item = await self._sdk._catalog.ingest(
                agent_id=self._sdk.context.agent_id,
                conversation_id=self._sdk.context.conversation_id,
                name=name,
                data=data,
                event_id=f"plugin:{self._sdk.plugin_id}:{self._sdk.context.run_id}:{hashlib.sha256(data).hexdigest()}:{name}",
            )
        except ResourceCatalogError as exc:
            raise PluginError("artifact_publish_failed", str(exc.code)) from None
        return str(item["resource_id"])


class _ImagesFacade:
    def __init__(self, sdk: "PluginSDK") -> None:
        self._sdk = sdk

    async def attach(self, resource_id: str, reason: str = "plugin") -> dict:
        """把本会话资源挂入本轮 ImageScope；只有模型实际收到才算已提供。"""
        if not isinstance(resource_id, str) or not _RESOURCE_ID.fullmatch(resource_id):
            raise PluginError("invalid_resource_id", "资源编号不合法")
        scope = self._sdk._images
        if scope is None or (getattr(scope, "agent_id", None), getattr(scope, "conversation_id", None)) != (
            self._sdk.context.agent_id, self._sdk.context.conversation_id
        ):
            raise PluginError("image_scope_unavailable", "本轮图片作用域不可用")
        try:
            evidence = await scope.mount(resource_id, reason=reason[:64])
        except Exception as exc:
            code = getattr(exc, "code", None) or "image_attach_failed"
            raise PluginError(str(code)[:64], "图片挂载未完成") from None
        return {k: evidence.get(k) for k in ("resource_id", "name", "mime", "size", "provided_to_model")}


class _EventsFacade:
    def __init__(self, sdk: "PluginSDK") -> None:
        self._sdk = sdk

    def emit(self, event_type: str, data: Optional[Mapping[str, Any]] = None) -> Optional[str]:
        """发布到真实有限事件流；返回事件 ID，未发布明确报错。"""

        self._sdk.require("events.emit")
        if event_type not in _EVENT_TYPES:
            raise PluginError("event_type_denied", "事件类型未授权")
        try:
            payload = json.dumps(dict(data or {}), ensure_ascii=False, default=str)[:1000]
            safe = json.loads(payload) if payload else {}
        except (TypeError, ValueError):
            raise PluginError("event_invalid", "事件数据无法编码") from None
        bus = self._sdk._events
        if bus is None:
            raise PluginError("event_bus_unavailable", "事件总线不可用")
        safe.update(plugin=self._sdk.plugin_id, run_id=self._sdk.context.run_id)
        return bus.publish(event_type, agent_id=self._sdk.context.agent_id,
                           session_id=self._sdk.context.conversation_id, data=safe)


class _NetworkFacade:
    """受限的视频资料读取：只读、固定主机、不继承凭据、不跟随重定向。"""

    def __init__(self, sdk: "PluginSDK") -> None:
        self._sdk = sdk

    async def read(self,url,**kwargs):
        self._sdk.require("network.read")
        return await self._public.read(url,**kwargs)

    async def video_read(self, url: str, *, max_bytes: int = 2 * 1024 * 1024,
                         timeout: float = 15.0) -> dict:
        self._sdk.require("network.video_read")
        from urllib.parse import urlparse
        import httpx

        try:
            parsed = urlparse(str(url))
        except ValueError:
            raise PluginError("host_not_allowed", "链接不合法") from None
        host = (parsed.hostname or "").lower()
        allowed = host in {"b23.tv", "bilibili.com", "api.bilibili.com", "www.bilibili.com"} \
            or host.endswith(".bilibili.com") or host.endswith(".bilivideo.com")
        if parsed.scheme != "https" or not allowed:
            raise PluginError("host_not_allowed", "链接主机不在允许范围")
        if not 1 <= max_bytes <= 8 * 1024 * 1024 or not 0 < timeout <= 30:
            raise PluginError("network_limits_invalid", "网络读取限额不合法")
        try:
            async with httpx.AsyncClient(timeout=timeout, follow_redirects=False,
                                         trust_env=False) as client:
                response = await client.get(str(url), headers={"User-Agent": "PeroPlugin/1.0"})
        except httpx.HTTPError:
            raise PluginError("network_unavailable", "视频资料读取失败") from None
        if 300 <= response.status_code < 400:
            raise PluginError("redirect_not_allowed", "拒绝跨主机重定向")
        if response.status_code != 200:
            raise PluginError("network_status", f"HTTP {response.status_code}")
        data = response.content
        if not data or len(data) > max_bytes:
            raise PluginError("network_too_large", "视频资料超过大小上限")
        return {"url": str(response.request.url), "content_type": response.headers.get("content-type", ""),
                "bytes": data}


class _UnavailableFacade:
    def __init__(self, name: str) -> None:
        self._name = name

    def __getattr__(self, _item: str):
        raise PluginError("capability_denied", f"{self._name} 门面尚未开放")


class PluginSDK:
    """插件 handler 唯一可使用的宿主能力入口。"""

    def __init__(
        self,
        *,
        plugin_id: str,
        manifest: PluginManifest,
        context: PluginRequestContext,
        config: Mapping[str, Any],
        storage: PluginStorage,
        catalog: Any,
        images: Any,
        events: Any,
        network_hosts=(),
    ) -> None:
        self.plugin_id = plugin_id
        self.manifest = manifest
        self.context = context
        self._config = dict(config)
        self._capabilities = frozenset(manifest.capabilities)
        self._storage = storage
        self._catalog = catalog
        self._published = {}
        self._images = images
        self._events = events
        self.artifacts = _ArtifactsFacade(self)
        self.images = _ImagesFacade(self)
        self.events = _EventsFacade(self)
        self.network = _NetworkFacade(self)
        from pero.plugins.network import PublicNetwork
        self.network._public = PublicNetwork(set(network_hosts)&set(manifest.network_hosts) if "network.read" in manifest.capabilities else ())
        self.models = _UnavailableFacade("models")
        self.memory = _UnavailableFacade("memory")
        self.knowledge = _UnavailableFacade("knowledge")

    def require(self, capability: str) -> None:
        if capability not in self._capabilities:
            raise PluginError("capability_denied", "插件未声明该能力")

    @property
    def storage(self):
        self.require('cache.private')
        return self._storage

    @property
    def config(self) -> "_ConfigFacade":
        return _ConfigFacade(self._config)

    @property
    def is_cancelled(self) -> bool:
        return bool(self.context.is_cancelled())


class _ConfigFacade:
    def __init__(self, config: Mapping[str, Any]) -> None:
        self._config = dict(config)

    def get(self, name: str, default: Any = None) -> Any:
        return self._config.get(name, default)

    def all(self) -> dict:
        return dict(self._config)


_CURRENT_SDK: ContextVar[Optional[PluginSDK]] = ContextVar("pero_plugin_sdk", default=None)


def current_sdk() -> Optional[PluginSDK]:
    return _CURRENT_SDK.get()


class sdk_scope:
    """仅在本轮 handler 调用期间暴露 SDK；退出即恢复。"""

    def __init__(self, sdk: PluginSDK) -> None:
        self._sdk = sdk
        self._token = None

    def __enter__(self) -> PluginSDK:
        self._token = _CURRENT_SDK.set(self._sdk)
        return self._sdk

    def __exit__(self, *_exc: Any) -> None:
        if self._token is not None:
            _CURRENT_SDK.reset(self._token)
