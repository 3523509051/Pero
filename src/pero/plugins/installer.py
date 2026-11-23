"""插件安装、注册、启停、配置与版本切换。

首版只接受**本机受信任目录**的 Python 包：解析清单、复制白名单文件、
校验工具名冲突，再在宿主进程内加载声明的入口。不执行安装脚本、不下载、
不修改主应用或全局 Python 环境；市场来源默认不取得宿主内权限。
"""

from __future__ import annotations

import asyncio
import hashlib
import importlib.util
import json
import os
import re
import shutil
import sys
import tempfile
import time
import uuid
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Optional

from pero.configuration.catalog import (
    register_plugin_tool_schemas,
    unregister_plugin_tool_schemas,
)
from pero.core.contracts import ToolSpec
from pero.core.settings import repo_root
from pero.plugins.adapter import PluginToolAdapter, ToolBinding
from pero.plugins.env_manager import PluginEnvironment
from pero.plugins.errors import PluginError
from pero.plugins.manifest import (
    PluginManifest,
    read_manifest,
    validate_config,
)
from pero.plugins.worker_client import PluginWorkerClient
from pero.tools.registry import ToolRegistry

_SAFE_SUFFIXES = frozenset({
    ".py", ".json", ".toml", ".md", ".txt", ".png", ".jpg", ".jpeg", ".gif", ".webp",
})
_SCOPE = re.compile(r"^[A-Za-z0-9_.:@/-]{1,200}$")
_MAX_FILE_BYTES = 2 * 1024 * 1024
_MAX_PACKAGE_BYTES = 12 * 1024 * 1024
_MAX_FILES = 200


def _utc() -> str:
    """插件安装/更新时间统一北京时间（带 +08:00）。"""

    from pero.core.timebase import now_iso

    return now_iso()


class PluginRuntime:
    """单个已启用插件的运行态；绑定替换原子化，在途调用持旧快照。"""

    def __init__(
        self,
        *,
        plugin_id: str,
        manifest: PluginManifest,
        data_dir: Path,
        events: Any,
        timeout_seconds: float,
        worker_python: str = "",
        worker_timeout: float = 60.0,
        quota_mb: int = 32,
    ) -> None:
        self.plugin_id = plugin_id
        self.manifest = manifest
        self.data_dir = data_dir
        self.events = events
        self.timeout_seconds = timeout_seconds
        self.worker_python = worker_python
        self.worker_timeout = worker_timeout
        self.quota_mb = quota_mb
        self.worker: Any = None
        self.enabled = False
        self.active_version = manifest.version
        self.config: dict = {}
        self.scopes: dict[str, bool] = {}
        self.scope_configs: dict[str, dict] = {}
        self.network_hosts = ()
        self.container_image = ""
        self.module: Any = None
        self.adapters: dict[str, PluginToolAdapter] = {}
        self._bindings: dict[str, ToolBinding] = {}
        self._last: dict[str, ToolBinding] = {}
        self._generation = 0

    def binding(self, name: str) -> Optional[ToolBinding]:
        return self._bindings.get(name)

    def last_binding(self, name: str) -> Optional[ToolBinding]:
        return self._bindings.get(name) or self._last.get(name)

    def scope_allowed(self, conversation_id: str) -> bool:
        if not conversation_id:
            return False
        return bool(self.scopes.get(str(conversation_id), True))

    def effective_config(self, conversation_id, base):
        defaults={k:v['default'] for k,v in self.manifest.config_schema.get('properties',{}).items() if 'default' in v}
        return {**defaults, **base, **self.scope_configs.get(conversation_id,{})}

    def set_bindings(self, bindings: Mapping[str, ToolBinding], *, version: str) -> None:
        self._generation += 1
        for name, binding in bindings.items():
            self._last[name] = self._bindings.get(name) or binding
            self._bindings[name] = binding
        self.active_version = version

    def set_config(self, config: Mapping[str, Any]) -> None:
        self.config = dict(config)
        self._generation += 1
        for name, binding in list(self._bindings.items()):
            self._bindings[name] = replace(
                binding, config=self.config, generation=self._generation
            )

    def deactivate(self) -> None:
        self.enabled = False

    def _worker_handler(self, name: str):
        async def handler(arguments: Mapping[str, Any], sdk: Any) -> dict:
            if self.worker is None:
                raise PluginError("worker_unavailable", "插件 worker 不可用")
            raw = await self.worker.call(
                name, dict(arguments), sdk.config.all(),
                {"run_id": sdk.context.run_id, "agent_id": sdk.context.agent_id,
                 "conversation_id": sdk.context.conversation_id, "scope_ref":sdk.context.scope_ref,
                 "channel_id":sdk.context.channel_id,
                 # 容器/子进程与宿主用同一时区；显式传，不依赖 worker 端默认值。
                 "timezone": getattr(sdk.context, "timezone", "") or "Asia/Shanghai"},
                network_hosts=self.network_hosts,timeout_override=sdk.context.background_timeout,
            )
            artifacts = []
            warnings = [str(item)[:200] for item in (raw.get("warnings") or [])][:10]
            for item in raw.get("artifacts") or []:
                if not isinstance(item, Mapping):
                    raise PluginError("plugin_result_invalid", "插件产物声明不合法")
                relative = str(item.get("relative") or "")
                # 读取范围由宿主限定在插件自己的数据目录内。
                data = await sdk._storage.read_blob(relative)
                if item.get('handle'):
                    if len(data)!=item.get('size') or hashlib.sha256(data).hexdigest()!=item.get('sha256'):
                        raise PluginError('artifact_invalid','产物完整性校验失败')
                    published=await sdk.artifacts.publish(data,str(item.get('name')),kind=item.get('kind','document'))
                    meta=sdk._published[published['handle']]
                    rid=meta['resource_id']
                    evidence=meta
                    if item.get('attach'):
                        evidence=await sdk.artifacts.attach(published['handle'])
                else:
                    rid=await sdk.artifacts.publish_image(data,str(item.get('name') or 'artifact.png'))
                    evidence=await sdk.images.attach(rid,reason='plugin.worker')
                artifacts.append({"kind": str(item.get("kind") or "image")[:32],
                                  "resource_id": rid,
                                  "mime": evidence.get("mime") or "image/png",
                                  "size": evidence.get("size") or len(data)})
            for event in raw.get("events") or []:
                if not isinstance(event, Mapping):
                    continue
                try:
                    sdk.events.emit(str(event.get("type") or ""), event.get("data") or {})
                except PluginError:
                    warnings.append("插件事件未授权，已忽略")
            return {"ok": bool(raw.get("business_ok",raw.get("ok",True))), "status": str(raw.get("status") or "complete"),
                    "data": dict(raw.get("data") or {}), "artifacts": artifacts,
                    "warnings": warnings[:10], "error": raw.get("error"), "cache": raw.get("cache")}
        return handler

    async def load_worker(self, package_dir: Path) -> None:
        """独立进程加载：不把第三方代码导入 Core 进程。"""

        self.data_dir.mkdir(parents=True, exist_ok=True)
        if self.container_image:
            from pero.plugins.docker_worker import DockerWorkerClient
            self.worker=DockerWorkerClient(package=package_dir,data_dir=self.data_dir,image=self.container_image,
                timeout_seconds=self.worker_timeout,quota_mb=self.quota_mb,network_hosts=set(self.network_hosts)&set(self.manifest.network_hosts) if 'network.read' in self.manifest.capabilities else (),dependencies=self.manifest.dependencies)
        else:
            self.worker = PluginWorkerClient(package=package_dir,data_dir=self.data_dir,python=self.worker_python, timeout_seconds=self.worker_timeout,quota_mb=self.quota_mb)
        await self.worker.start()
        declared = await self.worker.init()
        items = [{"name": str(item.get("name") or ""),
                  "description": str(item.get("description") or ""),
                  "parameters": dict(item.get("parameters") or {}),
                  "handler": self._worker_handler(str(item.get("name") or ""))}
                 for item in declared if isinstance(item, Mapping)]
        bindings = self._validate_declaration(items)
        self.set_bindings(bindings, version=self.manifest.version)

    def load(self, package_dir: Path) -> None:
        """加载包声明的入口；只调用 `register()`，不运行任何安装脚本。"""

        path = Path(package_dir) / (self.manifest.module.replace(".", os.sep) + ".py")
        path = path.resolve()
        if not path.is_file() or package_dir.resolve() not in path.parents:
            raise PluginError("entrypoint_not_found", "插件入口模块不存在")
        name = f"pero_plugin_{uuid.uuid4().hex}"
        prefix = str(package_dir.resolve())
        for key in [k for k, value in list(sys.modules.items())
                    if str(getattr(value, "__file__", "") or "").startswith(prefix)]:
            sys.modules.pop(key, None)
        try:
            spec = importlib.util.spec_from_file_location(name, path)
            if spec is None or spec.loader is None:
                raise PluginError("entrypoint_failed", "插件入口无法加载")
            module = importlib.util.module_from_spec(spec)
            sys.modules[name] = module
            spec.loader.exec_module(module)
            factory = getattr(module, self.manifest.function, None)
            if not callable(factory):
                raise PluginError("entrypoint_failed", "插件入口函数不存在")
            declared = factory()
            self.module = module
        except PluginError:
            raise
        except Exception:
            raise PluginError("entrypoint_failed", "插件入口执行失败") from None
        finally:
            sys.modules.pop(name, None)
        bindings = self._validate_declaration(declared)
        self.set_bindings(bindings, version=self.manifest.version)

    def _validate_declaration(self, declared: Any) -> dict[str, ToolBinding]:
        if not isinstance(declared, (list, tuple)) or not declared:
            raise PluginError("declaration_mismatch", "register() 必须返回工具声明列表")
        names = []
        bindings: dict[str, ToolBinding] = {}
        for item in declared:
            if not isinstance(item, Mapping):
                raise PluginError("declaration_mismatch", "工具声明必须是对象")
            name = str(item.get("name") or "")
            if name not in self.manifest.tools:
                raise PluginError("declaration_mismatch", "声明了清单未登记的工具")
            description = str(item.get("description") or "").strip()
            parameters = item.get("parameters")
            handler = item.get("handler")
            if not description or len(description) > 500 or not isinstance(parameters, Mapping):
                raise PluginError("declaration_mismatch", "工具声明字段不完整")
            if not callable(handler):
                raise PluginError("declaration_mismatch", "工具 handler 不可调用")
            try:
                encoded = json.dumps(dict(parameters), ensure_ascii=False)
            except (TypeError, ValueError):
                raise PluginError("declaration_mismatch", "工具参数 schema 无法编码") from None
            if len(encoded) > 12000:
                raise PluginError("declaration_mismatch", "工具参数 schema 过大")
            if name in names:
                raise PluginError("declaration_mismatch", "工具声明名称重复")
            names.append(name)
            bindings[name] = ToolBinding(
                spec=ToolSpec(name=name, description=description, parameters=dict(parameters)),
                handler=handler,
                version=self.manifest.version,
                config=self.config,
                generation=self._generation,
            )
        if set(names) != set(self.manifest.tools):
            raise PluginError("declaration_mismatch", "工具声明与清单不一致")
        return bindings


class PluginInstaller:
    """插件目录与生命周期；所有状态写入独立 JSON，不接触主记忆库。"""

    def __init__(
        self,
        root: Path,
        tool_registry: ToolRegistry,
        *,
        events: Any = None,
        timeout_seconds: float = 30.0,
        source_roots: Optional[tuple[Path, ...]] = None,
        worker_python: str = "",
        worker_timeout: float = 60.0,
        quota_mb: int = 32,
        network_hosts=(),
        container_image="",
    ) -> None:
        self.root = Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self.packages = self.root / "packages"
        self.packages.mkdir(exist_ok=True)
        self.state_path = self.root / "state.json"
        self.tools = tool_registry
        self.events = events
        self.timeout_seconds = float(timeout_seconds)
        self.worker_python = worker_python
        self.worker_timeout = float(worker_timeout)
        self.quota_mb = int(quota_mb)
        self.network_hosts=tuple(network_hosts)
        self.container_image=container_image
        from pero.plugins.jobs import PluginJobs
        self.jobs=PluginJobs(self.root/"jobs.json",events)
        self.envs = PluginEnvironment(root / "envs", host_python=worker_python)
        self.sources = tuple((p.resolve() for p in source_roots)) if source_roots else (
            repo_root(), self.root.parent
        )
        self._lock = asyncio.Lock()
        self._runtimes: dict[str, PluginRuntime] = {}
        self._state = self._load_state()

    # ------------------------------------------------------------------ #
    # 持久化
    # ------------------------------------------------------------------ #

    def _load_state(self) -> dict:
        if not self.state_path.is_file():
            return {"version": 1, "plugins": {}}
        try:
            data = json.loads(self.state_path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError):
            return {"version": 1, "plugins": {}}
        if not isinstance(data, dict) or data.get("version") != 1 or not isinstance(data.get("plugins"), dict):
            return {"version": 1, "plugins": {}}
        for record in data["plugins"].values():
            if isinstance(record, dict) and "desired_enabled" not in record:
                record["desired_enabled"] = bool(record.get("enabled", False))
        return data

    def _save_state(self) -> None:
        target = self.state_path
        descriptor, temporary = tempfile.mkstemp(prefix="plugins-", suffix=".tmp", dir=self.root)
        try:
            with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
                json.dump(self._state, stream, ensure_ascii=False, sort_keys=True)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, target)
        except OSError:
            raise PluginError("state_write_failed", "插件状态无法保存") from None
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)

    def _persist(self) -> None:
        """同步落盘的唯一入口：状态失败不静默继续。"""

        self._save_state()

    def _public(self, record: Mapping[str, Any], runtime: Optional[PluginRuntime]) -> dict:
        manifest = record.get("manifest") or {}
        desired = bool(record.get("desired_enabled"))
        registered = bool(runtime and runtime.enabled and runtime.adapters)
        error = record.get("last_error")
        if error:
            status = "failed"
        elif registered:
            status = "enabled"
        elif desired:
            status = "recovering"
        else:
            status = "disabled"
        return {
            "id": record.get("id"),
            "name": manifest.get("name", ""),
            "description": manifest.get("description", ""),
            "version": record.get("active_version"),
            "available_versions": list(record.get("available_versions") or []),
            "manifest_sha256": record.get("sha256", ""),
            "license": manifest.get("license", ""),
            "tools": list(manifest.get("tools") or []),
            "capabilities": list(manifest.get("capabilities") or []),
            "runtime_mode": manifest.get("runtime", "host"),
            "network_hosts": list(manifest.get("network_hosts") or []),
            "background_tools":list(manifest.get("background_tools") or []),
            "approved_network_hosts":list((record.get("network_approvals") or {}).get(record.get("active_version"),[])),
            "execution_blocked": "remote_isolation_required" if not self.container_image and ((record.get("version_origins") or {}).get(record.get("active_version")) or {"source":record.get("source")}).get("source")=="github" else None,
            "version_origins":dict(record.get("version_origins") or {}),
            "dependencies": list(manifest.get("dependencies") or []),
            "config_schema": dict(manifest.get("config_schema") or {}),
            "config": dict(record.get("config") or {}),
            "scopes": dict(record.get("scopes") or {}),
            "scope_configs": dict(record.get("scope_configs") or {}),
            "desired_enabled": desired,
            "enabled": registered,
            "status": status,
            "source": record.get("source", "local"),
            "trust": record.get("trust", "local_trusted"),
            "source_url": record.get("source_url", ""),
            "commit": record.get("commit", ""),
            "package_sha256": record.get("package_sha256", ""),
            "installed_at": record.get("installed_at", ""),
            "last_error": error,
            "runtime": {
                "registered": registered,
                "tools": sorted(runtime.adapters) if runtime else [],
                "generation": sum(1 for _ in (runtime._bindings if runtime else {})),
            },
        }

    # ------------------------------------------------------------------ #
    # 安装与包文件
    # ------------------------------------------------------------------ #

    def _check_source(self, source: Path) -> Path:
        resolved = Path(source).resolve()
        if not resolved.is_dir():
            raise PluginError("invalid_source", "插件源目录不存在")
        if not any(resolved == root or root in resolved.parents for root in self.sources):
            raise PluginError("source_not_allowed", "插件源目录不在允许范围内")
        return resolved

    @staticmethod
    def _copy_package(source: Path, target: Path) -> None:
        target.mkdir(parents=True, exist_ok=True)
        total = 0
        count = 0
        for root, dirs, files in os.walk(source):
            dirs[:] = [d for d in dirs if d not in {"__pycache__", ".git", ".venv", "node_modules"}]
            for filename in files:
                item = Path(root) / filename
                if item.suffix.lower() not in _SAFE_SUFFIXES or item.is_symlink():
                    continue
                size = item.stat().st_size
                if size > _MAX_FILE_BYTES:
                    raise PluginError("package_too_large", "插件文件超过大小上限")
                total += size
                count += 1
                if total > _MAX_PACKAGE_BYTES or count > _MAX_FILES:
                    raise PluginError("package_too_large", "插件包超过总量或文件数上限")
                relative = item.relative_to(source)
                destination = target / relative
                destination.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(item, destination)

    async def install(
        self,
        source: Path,
        *,
        expected_sha256: str = "",
        trust: str = "local_trusted",
        source_kind: str = "local",
        source_url: str = "",
        commit: str = "",
        package_sha256: str = "",
        allow_external: bool = False,
    ) -> dict:
        """安装一个新版本；默认不启用，安装阶段不加载任何包内代码。"""

        async with self._lock:
            resolved = Path(source).resolve() if allow_external else await asyncio.to_thread(self._check_source, source)
            if not resolved.is_dir():
                raise PluginError("invalid_source", "插件源目录不存在")
            manifest, digest = await asyncio.to_thread(read_manifest, resolved)
            if expected_sha256 and str(expected_sha256).strip().lower() != digest:
                raise PluginError("manifest_hash_mismatch", "插件清单哈希不匹配")
            if trust.startswith("remote") and manifest.runtime != "worker":
                raise PluginError("remote_host_runtime_denied", "远程插件必须在独立 worker 中运行")
            record = self._state["plugins"].get(manifest.id)
            if record is not None and tuple((record.get("manifest") or {}).get("tools") or ()) != manifest.tools:
                raise PluginError("tool_set_changed", "首版不支持安装时增减工具，请用新插件 ID")
            versions = list((record or {}).get("available_versions") or [])
            target = self.packages / manifest.id / manifest.version
            existing = manifest.version in versions and target.is_dir()
            if not existing:
                owned = self._runtimes.get(manifest.id)
                owned_names = set(owned.adapters) if owned else set()
                for name in manifest.tools:
                    if name in owned_names:
                        continue
                    if self.tools.has(name):
                        raise PluginError("duplicate_tool", "工具名与现有工具冲突")
            if existing:
                await asyncio.to_thread(self._persist)
                return self._public(
                    {"id": manifest.id, "manifest": self._manifest_dict(manifest),
                     "sha256": digest, "active_version": (record or {}).get("active_version", manifest.version),
                     "available_versions": versions, "config": (record or {}).get("config") or {},
                     "desired_enabled": (record or {}).get("desired_enabled", False),
                     "trust": (record or {}).get("trust", trust),
                     "source": (record or {}).get("source", source_kind),
                     "source_url": (record or {}).get("source_url", source_url),
                     "commit": (record or {}).get("commit", commit),
                     "package_sha256": (record or {}).get("package_sha256", package_sha256),
                     "installed_at": (record or {}).get("installed_at", _utc()),
                     "last_error": (record or {}).get("last_error")},
                    self._runtimes.get(manifest.id),
                )
            await asyncio.to_thread(self._copy_package, resolved, target)
            copied_manifest, copied_digest = await asyncio.to_thread(read_manifest, target)
            if copied_digest != digest:
                raise PluginError("install_failed", "复制后的清单与源不一致")
            if record is None:
                record = {
                    "id": manifest.id,
                    "manifest": self._manifest_dict(copied_manifest),
                    "sha256": copied_digest,
                    "source": source_kind,
                    "source_url": source_url,
                    "commit": commit,
                    "package_sha256": package_sha256,
                    "installed_at": _utc(),
                    "active_version": manifest.version,
                    "available_versions": [],
                    "desired_enabled": False,
                    "trust": trust,
                    "config": {},
                    "scopes": {},
                    "last_error": None,
                }
                self._state["plugins"][manifest.id] = record
            origin=dict(record.get('version_origins') or {})
            origin[manifest.version]={'source':source_kind,'commit':commit,'package_sha256':package_sha256,'source_url':source_url}
            record['version_origins']=origin
            record['available_versions'] = sorted(set(versions + [manifest.version]))
            record["last_error"] = None
            await asyncio.to_thread(self._persist)
            return self._public(record, self._runtimes.get(manifest.id))

    @staticmethod
    def _manifest_dict(manifest: PluginManifest) -> dict:
        return {
            "id": manifest.id,
            "name": manifest.name,
            "description": manifest.description,
            "version": manifest.version,
            "sdk_version": manifest.sdk_version,
            "entrypoint": manifest.entrypoint,
            "license": manifest.license,
            "tools": list(manifest.tools),
            "capabilities": list(manifest.capabilities),
            "config_schema": dict(manifest.config_schema),
            "runtime": manifest.runtime,
            "dependencies": list(manifest.dependencies),
            "network_hosts": list(manifest.network_hosts),
            "background_tools":list(manifest.background_tools),
            "background_timeout_seconds":manifest.background_timeout_seconds,
        }

    # ------------------------------------------------------------------ #
    # 生命周期
    # ------------------------------------------------------------------ #

    def _record(self, plugin_id: str) -> dict:
        record = self._state["plugins"].get(plugin_id)
        if record is None:
            raise PluginError("not_installed", "插件尚未安装")
        return record

    async def enable(self, plugin_id: str, version: str = "") -> dict:
        """整批校验后切换；任何一步失败都不留半注册状态。"""

        async with self._lock:
            record = self._record(plugin_id)
            target_version = version or str(record.get("active_version") or "")
            if target_version not in (record.get("available_versions") or []):
                raise PluginError("version_not_installed", "指定版本未安装")
            package = self.packages / plugin_id / target_version
            manifest, _digest = await asyncio.to_thread(read_manifest, package)
            origin=(record.get('version_origins') or {}).get(target_version) or {'source':record.get('source')}
            remote=origin.get('source')=='github'
            if remote and not self.container_image:
                raise PluginError('remote_isolation_required','远程插件执行需配置独立低权限运行环境；已安装包可保留')
            worker_python = ""
            if manifest.runtime == "worker" and not remote:
                worker_python = await self.envs.prepare(
                    plugin_id, target_version, package, manifest.dependencies)
            runtime = self._runtimes.get(plugin_id)
            approved=tuple(set(self.network_hosts)|set((record.get('network_approvals') or {}).get(target_version,[])))
            newly: list[tuple[str, PluginToolAdapter]] = []
            try:
                if runtime is None:
                    runtime = PluginRuntime(
                        plugin_id=plugin_id,
                        manifest=manifest,
                        data_dir=self.root / "data" / plugin_id,
                        events=self.events,
                        timeout_seconds=self.timeout_seconds,
                        worker_python=worker_python,
                        worker_timeout=self.worker_timeout,
                        quota_mb=self.quota_mb,
                    )
                    # 启动恢复：实例配置与会话覆盖必须在工具可见前恢复。
                    runtime.network_hosts=approved
                    runtime.container_image=self.container_image if remote else ""
                    runtime.config = dict(record.get("config") or {})
                    runtime.scopes = dict(record.get("scopes") or {})
                    runtime.scope_configs = dict(record.get("scope_configs") or {})
                    if manifest.runtime == "worker":
                        await runtime.load_worker(package)
                    else:
                        await asyncio.to_thread(runtime.load, package)
                    for name in runtime._bindings:
                        if self.tools.has(name):
                            raise PluginError("duplicate_tool", "工具名与现有工具冲突")
                    for name in runtime._bindings:
                        adapter = PluginToolAdapter(runtime, name)
                        runtime.adapters[name] = adapter
                        self.tools.register(adapter)
                        newly.append((name, adapter))
                    self._runtimes[plugin_id] = runtime
                elif target_version != runtime.active_version:
                    if tuple(runtime.adapters) != manifest.tools:
                        raise PluginError("tool_set_changed", "版本间工具集合不一致，拒绝切换")
                    fresh = PluginRuntime(
                        plugin_id=plugin_id,
                        manifest=manifest,
                        data_dir=runtime.data_dir,
                        events=self.events,
                        timeout_seconds=self.timeout_seconds,
                        worker_python=worker_python,
                        worker_timeout=self.worker_timeout,
                        quota_mb=self.quota_mb,
                    )
                    fresh.network_hosts=approved
                    fresh.container_image=self.container_image if remote else ""
                    fresh.config = dict(record.get("config") or {})
                    fresh.scopes = dict(record.get("scopes") or {})
                    fresh.scope_configs = dict(record.get("scope_configs") or {})
                    if manifest.runtime == "worker":
                        await fresh.load_worker(package)
                    else:
                        await asyncio.to_thread(fresh.load, package)
                    if tuple(fresh._bindings) != tuple(runtime.adapters):
                        raise PluginError("tool_set_changed", "版本间工具集合不一致，拒绝切换")
                    fresh.enabled = True
                    for name, adapter in runtime.adapters.items():
                        adapter.retarget(fresh)
                        fresh.adapters[name] = adapter
                    if runtime.worker is not None:
                        await runtime.worker.close()
                    runtime.deactivate()
                    runtime = fresh
                    self._runtimes[plugin_id] = runtime
                runtime.network_hosts=approved
                if getattr(runtime.worker,'healthy',False):runtime.worker.hosts=tuple(set(approved)&set(manifest.network_hosts))
                if manifest.runtime=='worker' and (runtime.worker is None or not getattr(runtime.worker,'healthy',getattr(runtime.worker,'_proc',None) is not None)):
                    await runtime.load_worker(package)
                runtime.enabled = True
                runtime.set_config(record.get("config") or {})
                runtime.scopes = dict(record.get("scopes") or {})
                register_plugin_tool_schemas(
                    plugin_id, manifest.tools, manifest.config_schema,
                    {name: dict(binding.spec.parameters) for name, binding in runtime._bindings.items()},
                )
                record['source']=origin.get('source') or record.get('source','local')
                for k in ('commit','package_sha256','source_url'):
                    if k in origin:record[k]=origin[k]
                record["active_version"] = target_version
                record["manifest"] = self._manifest_dict(manifest)
                record["desired_enabled"] = True
                record["last_error"] = None
            except PluginError as exc:
                for name, adapter in newly:
                    self.tools.unregister(name, adapter)
                if runtime is not None and runtime.worker is not None and not runtime.adapters:
                    await runtime.worker.close()
                record["desired_enabled"] = False
                record["last_error"] = exc.code
                await asyncio.to_thread(self._persist)
                raise
            except Exception:
                for name, adapter in newly:
                    self.tools.unregister(name, adapter)
                record["desired_enabled"] = False
                record["last_error"] = "enable_failed"
                await asyncio.to_thread(self._persist)
                raise PluginError("enable_failed", "插件启用失败") from None
            await asyncio.to_thread(self._persist)
            return self._public(record, runtime)

    async def disable(self, plugin_id: str) -> dict:
        self.jobs.cancel_plugin(plugin_id)
        async with self._lock:
            record = self._record(plugin_id)
            runtime = self._runtimes.get(plugin_id)
            if runtime is not None:
                runtime.deactivate()
            record["desired_enabled"] = False
            record["last_error"] = None
            await asyncio.to_thread(self._persist)
            return self._public(record, runtime)

    async def uninstall(self, plugin_id: str) -> dict:
        self.jobs.cancel_plugin(plugin_id)
        async with self._lock:
            record = self._record(plugin_id)
            runtime = self._runtimes.get(plugin_id)
            if runtime is not None:
                runtime.deactivate()
                if runtime.worker is not None:
                    await runtime.worker.close()
                for name, adapter in runtime.adapters.items():
                    self.tools.unregister(name, adapter)
                self._runtimes.pop(plugin_id, None)
            unregister_plugin_tool_schemas(plugin_id)
            await asyncio.to_thread(shutil.rmtree, self.packages / plugin_id, True)
            await asyncio.to_thread(shutil.rmtree, self.root / "envs" / plugin_id, True)
            self._state["plugins"].pop(plugin_id, None)
            await asyncio.to_thread(self._persist)
            return {"id": plugin_id, "uninstalled": True, "data_preserved": True,
                    "status": "disabled"}

    async def set_config(self, plugin_id: str, config: Mapping[str, Any]) -> dict:
        async with self._lock:
            record = self._record(plugin_id)
            manifest = PluginManifest(**{
                k: v for k, v in record["manifest"].items()
                if k in {f for f in PluginManifest.__dataclass_fields__}
            })
            defaults={k:v['default'] for k,v in manifest.config_schema.get('properties',{}).items() if 'default' in v}
            errors = validate_config(manifest,{**defaults,**config})
            for override in (record.get('scope_configs') or {}).values():
                errors += validate_config(manifest,{**defaults,**config,**override})
            if errors:
                raise PluginError("invalid_config", "插件配置不符合声明的 schema")
            value = dict(config or {})
            record["config"] = value
            runtime = self._runtimes.get(plugin_id)
            if runtime is not None:
                runtime.set_config(value)
            await asyncio.to_thread(self._persist)
            return self._public(record, runtime)

    async def set_network_hosts(self,plugin_id,hosts):
        async with self._lock:
            record=self._record(plugin_id)
            active=self._runtimes.get(plugin_id)
            if active is not None and active.enabled:
                raise PluginError('plugin_running','请先停用插件，再调整网络授权')
            declared=set(record['manifest'].get('network_hosts') or [])
            if not isinstance(hosts,list) or any(not isinstance(h,str) or h not in declared for h in hosts):
                raise PluginError('network_origin_denied','只能批准该版本声明的域名')
            record.setdefault('network_approvals',{})[record['active_version']]=sorted(set(hosts))
            await asyncio.to_thread(self._persist)
            return self._public(record,active)

    async def set_scope(self, plugin_id: str, conversation_id: str, enabled: bool | None, config: dict | None = None) -> dict:
        if not _SCOPE.fullmatch(str(conversation_id)):
            raise PluginError("invalid_scope", "会话标识不合法")
        async with self._lock:
            record = self._record(plugin_id)
            scopes = dict(record.get("scopes") or {})
            if enabled is None:
                scopes.pop(str(conversation_id),None)
            else:
                scopes[str(conversation_id)] = bool(enabled)
            configs=dict(record.get('scope_configs') or {})
            if config is not None:
                manifest = PluginManifest(**{k:v for k,v in record['manifest'].items() if k in PluginManifest.__dataclass_fields__})
                defaults={k:v['default'] for k,v in manifest.config_schema.get('properties',{}).items() if 'default' in v}
                if validate_config(manifest,{**defaults,**(record.get('config') or {}),**config}):
                    raise PluginError('invalid_config','会话配置不符合 schema')
                if config:configs[str(conversation_id)]=dict(config)
                else:configs.pop(str(conversation_id),None)
            record['scope_configs']=configs
            record["scopes"] = scopes
            runtime = self._runtimes.get(plugin_id)
            if runtime is not None:
                runtime.scopes = dict(scopes)
                runtime.scope_configs=dict(configs)
            await asyncio.to_thread(self._persist)
            return {"id": plugin_id, "conversation_id": conversation_id, "enabled": enabled, "config": configs.get(str(conversation_id),{}),
                    "effective_from": "next_turn"}

    # ------------------------------------------------------------------ #
    # 查询与恢复
    # ------------------------------------------------------------------ #

    def snapshot(self) -> dict:
        items = []
        for plugin_id, record in sorted(self._state["plugins"].items()):
            items.append(self._public(record, self._runtimes.get(plugin_id)))
        return {
            "api_version": "v1",
            "sdk_version": "1",
            "installed": len(items),
            "enabled": sum(1 for item in items if item["enabled"]),
            "items": items,
        }

    def detail(self, plugin_id: str) -> dict:
        record = self._record(plugin_id)
        result = self._public(record, self._runtimes.get(plugin_id))
        result["scopes"] = dict(record.get("scopes") or {})
        result["scope_configs"] = dict(record.get("scope_configs") or {})
        return result

    async def restore(self) -> dict:
        """进程启动时恢复上次启用的插件；失败只记录状态，不挂空工具。"""

        restored = 0
        failed = 0
        for plugin_id, record in sorted(self._state["plugins"].items()):
            if not record.get("desired_enabled"):
                continue
            expected = dict(record.get("scopes") or {}), dict(record.get("config") or {})
            try:
                await self.enable(plugin_id, str(record.get("active_version") or ""))
                runtime = self._runtimes.get(plugin_id)
                if runtime is not None:
                    runtime.scopes, runtime.config = expected[0], expected[1]
                restored += 1
            except PluginError as exc:
                record["desired_enabled"] = True
                record["last_error"] = exc.code
                failed += 1
        await asyncio.to_thread(self._persist)
        return {"restored": restored, "failed": failed}

    def active_tools(self) -> dict:
        result = {}
        for plugin_id, runtime in self._runtimes.items():
            if runtime.enabled:
                result[plugin_id] = sorted(runtime.adapters)
        return result

    async def aclose(self) -> None:
        await self.jobs.aclose()
        for runtime in list(self._runtimes.values()):
            if runtime.worker is not None:
                await runtime.worker.close()
