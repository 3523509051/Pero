"""独立插件 worker：在单独的 Python 进程中执行第三方插件业务。

用法：`python -m pero.plugins.worker --package <dir> --data-dir <dir>`
协议：stdin/stdout 逐行 JSON。Core 只发送参数与受控上下文；worker 不获得
管理令牌、主记忆库路径或模型凭据，产物只写入自己的数据目录。
"""

from __future__ import annotations

import argparse
import asyncio
import importlib.metadata
import importlib.util
import json
import os
import re
import sys
import uuid
import hashlib
import mimetypes
import base64
from pero_plugin_sdk import RequestContext
from pathlib import Path
from typing import Any, Mapping, Optional

from pero.plugins.errors import PluginError
from pero.plugins.manifest import read_manifest
from pero.plugins.sdk import PluginStorage


class _Config:
    def __init__(self, value: Mapping[str, Any]) -> None:
        self._value = dict(value)

    def get(self, key: str, default: Any = None) -> Any:
        return self._value.get(key, default)

    def all(self) -> dict:
        return dict(self._value)


class _Artifacts:
    def __init__(self, root: Path, capabilities=()) -> None:
        self._root = root
        self._capabilities = frozenset(capabilities)
        self._published = {}
        self._attached = set()

    async def publish(self, data: bytes, name: str, *, kind: str = 'document') -> dict:
        if kind not in ('image','document') or ('artifact.image' if kind=='image' else 'artifact.document') not in self._capabilities:
            raise PluginError('capability_denied','产物类型未授权')
        if not isinstance(data,bytes) or not data or len(data)>5*1024*1024 or not re.fullmatch(r'[A-Za-z0-9_.-]{1,80}',name):
            raise PluginError('artifact_invalid','产物参数不合法')
        from pero.resources.catalog import sniff_image_mime
        mime=sniff_image_mime(data)
        if bool(mime)!=(kind=='image'):raise PluginError('artifact_invalid','产物类型不匹配')
        handle='artifact-'+uuid.uuid4().hex
        relative='artifacts/'+handle+Path(name).suffix
        await PluginStorage(self._root).write_blob(relative,data)
        result={'handle':handle,'kind':kind,'name':name,'mime':mime or mimetypes.guess_type(name)[0] or 'application/octet-stream','size':len(data),'sha256':hashlib.sha256(data).hexdigest()}
        self._published[handle]={**result,'relative':relative}
        return result

    async def attach(self, handle: str) -> dict:
        item=self._published.get(handle)
        if item is None:raise PluginError('artifact_not_found','未知本轮产物')
        if item['kind']!='image':raise PluginError('attachment_unavailable','文档自动挂载尚未开放')
        self._attached.add(handle)
        return {'handle':handle,'provided_to_model':False}

    async def publish_image(self, data: bytes, name: str,
                            quota_bytes: int = 32 * 1024 * 1024) -> str:
        if "artifact.image" not in self._capabilities:raise PluginError("capability_denied","图片产物未授权")
        if not isinstance(data, bytes) or not data or len(data) > 5 * 1024 * 1024:
            raise PluginError("artifact_invalid", "图片产物大小不合法")
        if not re.fullmatch(r"[A-Za-z0-9_.-]{1,80}", name):
            raise PluginError("artifact_invalid", "产物名称不合法")
        relative = "artifacts/" + name
        self._root.joinpath("artifacts").mkdir(exist_ok=True)
        await asyncio.to_thread((self._root / relative).write_bytes, data)
        used = await asyncio.to_thread(lambda: sum(
            p.stat().st_size for p in self._root.rglob("*") if p.is_file()))
        if used > quota_bytes:
            await asyncio.to_thread((self._root / relative).unlink)
            raise PluginError("storage_quota_exceeded", "插件产物超过配额")
        return relative

    async def write(self, data: bytes, name: str, quota_bytes: int) -> dict:
        relative = await self.publish_image(data, name, quota_bytes)
        return {"relative": relative, "name": name, "size": len(data)}


class _WorkerImages:
    """worker 侧不挂载模型图片，只返回待宿主验证的受控产物引用。"""

    async def attach(self, handle: str, reason: str = "plugin.worker") -> dict:
        relative = str(handle or "")
        if not relative.startswith("artifacts/"):
            raise PluginError("artifact_invalid", "产物句柄不合法")
        return {"resource_id": relative, "name": Path(relative).name,
                "mime": "image/png", "size": 0, "provided_to_model": False,
                "reason": reason[:64]}


class _Events:
    def __init__(self, allowed=False) -> None:
        self.items: list[dict] = []
        self.allowed=allowed

    def emit(self, event_type: str, data: Optional[Mapping[str, Any]] = None) -> None:
        if not self.allowed or len(self.items)>=32 or len(json.dumps(dict(data or {})))>1000:
            raise PluginError("event_type_denied","事件不在许可或预算内")
        if event_type not in {"plugin.status", "plugin.progress", "plugin.record"}:
            raise PluginError("event_type_denied", "事件类型未授权")
        self.items.append({"type": event_type, "data": dict(data or {})})


class _BrokerNetwork:
    def __init__(self,request_id):self.request_id=request_id
    async def read(self,url,*,max_bytes=2*1024*1024,timeout=15,redirect_metadata=False):
        broker=uuid.uuid4().hex
        line=json.dumps({'op':'network_read','request_id':self.request_id,'broker_id':broker,'url':url,'max_bytes':max_bytes,'redirect_metadata':bool(redirect_metadata)})
        print(line,flush=True)
        raw=await asyncio.to_thread(sys.stdin.readline)
        response=json.loads(raw)
        if response.get('broker_id')!=broker:raise PluginError('worker_response_mismatch','网络响应错配')
        if not response.get('ok'):
            raise PluginError(str((response.get('error') or {}).get('code') or 'network_unavailable'),'公开资料读取失败')
        return {'url':response['url'],'content_type':response['content_type'],'bytes':base64.b64decode(response['content'],validate=True),'status_code':response.get('status_code',200),'location':response.get('location')}


class WorkerSDK:
    """worker 内可用门面：配置、独立存储、产物文件、受控事件。

    没有模型、记忆、知识库和资源目录访问；artifacts 返回的是 Core 后续
    必须验证并导入的相对产物路径，不是可直接对外使用的 r-ID。
    """

    def __init__(self, *, config: Mapping[str, Any], data_dir: Path, quota_bytes: int, context=None, capabilities=(), network_hosts=()) -> None:
        self.config = _Config(config)
        from pero.plugins.network import PublicNetwork
        self.network=PublicNetwork(network_hosts if "network.read" in capabilities else ())
        self.context = RequestContext(**(context or {"run_id":"","scope_ref":"","agent_id":"","conversation_id":""}))
        self._storage = PluginStorage(data_dir)
        self._capabilities=frozenset(capabilities)
        self.artifacts = _Artifacts(data_dir, capabilities)
        self.images = _WorkerImages()
        self.events = _Events("events.emit" in capabilities)
        self._quota = quota_bytes

    @property
    def storage(self):
        if 'cache.private' not in self._capabilities:raise PluginError('capability_denied','未声明独立缓存能力')
        return self._storage

    @property
    def is_cancelled(self) -> bool:
        return False

    async def write_artifact(self, data: bytes, name: str) -> dict:
        return await self.artifacts.write(data, name, self._quota)


def _check_dependencies(items) -> None:
    for requirement in items:
        match = re.fullmatch(r"([A-Za-z0-9_.-]+)(==|>=)([0-9A-Za-z.*+!-]+)", str(requirement))
        if match is None:
            raise PluginError("invalid_dependency", "依赖声明格式不合法")
        name, operator, wanted = match.groups()
        try:
            installed = importlib.metadata.version(name)
        except importlib.metadata.PackageNotFoundError:
            raise PluginError("dependency_missing", f"缺少依赖：{name}") from None
        if operator == "==" and installed != wanted:
            raise PluginError("dependency_mismatch", f"依赖版本不匹配：{name}")
        if operator == ">=" and tuple(installed.split(".")) < tuple(wanted.split(".")):
            raise PluginError("dependency_mismatch", f"依赖版本过低：{name}")


def _load_entry(package: Path, factory_name: str):
    manifest, _digest = read_manifest(package)
    path = (package / (manifest.module.replace(".", os.sep) + ".py")).resolve()
    if not path.is_file() or package.resolve() not in path.parents:
        raise PluginError("entrypoint_not_found", "插件入口模块不存在")
    prefix = str(package.resolve())
    for key in [k for k, value in list(sys.modules.items())
                if str(getattr(value, "__file__", "") or "").startswith(prefix)]:
        sys.modules.pop(key, None)
    module_name = "plugin_worker_" + uuid.uuid4().hex
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise PluginError("entrypoint_failed", "插件入口无法加载")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    factory = getattr(module, factory_name, None)
    if not callable(factory):
        raise PluginError("entrypoint_failed", "插件入口函数不存在")
    return manifest, factory()


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--package", required=True)
    parser.add_argument("--data-dir", required=True)
    parser.add_argument("--quota-mb", type=int, default=32)
    parser.add_argument("--isolated-scope", action="store_true")
    parser.add_argument("--seed-dir",default="")
    args = parser.parse_args()
    package = Path(args.package).resolve()
    data_dir = Path(args.data_dir).resolve()
    quota = args.quota_mb * 1024 * 1024
    if args.seed_dir:
        import shutil
        seed=Path(args.seed_dir).resolve();total=0;count=0
        for file in seed.rglob('*'):
            if file.is_symlink():raise PluginError('storage_invalid','缓存包含无效引用')
            if file.is_file():
                total+=file.stat().st_size;count+=1
                if total>quota or count>512:raise PluginError('storage_quota_exceeded','缓存超过预算')
                target=data_dir/file.relative_to(seed);target.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(file,target)
    handlers: dict[str, Any] = {}
    manifest = None
    error: Optional[PluginError] = None
    try:
        manifest, declared = _load_entry(package, "register")
        _check_dependencies(manifest.dependencies)
        if not isinstance(declared, (list, tuple)):
            raise PluginError("declaration_mismatch", "register() 必须返回声明列表")
        for item in declared:
            if not isinstance(item, Mapping) or not callable(item.get("handler")):
                raise PluginError("declaration_mismatch", "工具声明缺少 handler")
            handlers[str(item.get("name"))] = item["handler"]
        if set(handlers) != set(manifest.tools):
            raise PluginError("declaration_mismatch", "工具声明与清单不一致")
    except PluginError as exc:
        error = exc

    loop = asyncio.get_running_loop()
    while True:
        line = await loop.run_in_executor(None, sys.stdin.readline)
        if not line:
            return
        try:
            request = json.loads(line)
            op = request.get("op")
            request_id = str(request.get("request_id") or "")
            if error is not None:
                raise error
            if op == "init":
                response = {"ok": True, "request_id": request_id, "tools": [
                    {"name": name, "description": str(item.get("description") or ""),
                     "parameters": dict(item.get("parameters") or {})}
                    for name, item in ((d["name"], d) for d in declared)
                ]}
            elif op == "call":
                name = str(request.get("tool") or "")
                handler = handlers.get(name)
                if handler is None:
                    raise PluginError("unknown_tool", "worker 未登记该工具")
                context = dict(request.get('context') or {})
                scope_ref=str(context.get('scope_ref') or '')
                if not re.fullmatch(r'scope-[0-9a-f]{24}',scope_ref):
                    raise PluginError('scope_denied','缺少真实作用域')
                scoped_dir=data_dir if args.isolated_scope else data_dir/'scopes'/scope_ref
                scoped_dir.mkdir(parents=True,exist_ok=True)
                sdk = WorkerSDK(config=request.get('config') or {}, data_dir=scoped_dir, quota_bytes=quota, context=context, capabilities=manifest.capabilities, network_hosts=set(request.get("network_hosts") or ()) & set(manifest.network_hosts))
                if args.isolated_scope and request.get('broker_network') and 'network.read' in manifest.capabilities:
                    sdk.network=_BrokerNetwork(request_id)
                result = handler(dict(request.get("arguments") or {}), sdk)
                if asyncio.iscoroutine(result) or hasattr(result, "__await__"):
                    result = await result
                if not isinstance(result, Mapping):
                    raise PluginError("plugin_result_invalid", "插件结果必须是对象")
                artifacts = []
                for item in result.get("artifacts") or []:
                    if not isinstance(item, Mapping):
                        raise PluginError("plugin_result_invalid", "非法产物引用")
                    if item.get('handle'):
                        record=sdk.artifacts._published.get(item['handle'])
                        if record is None:raise PluginError('artifact_not_found','未知本轮产物')
                        artifacts.append({**record,'attach':item['handle'] in sdk.artifacts._attached})
                        continue
                    relative = str(item.get("relative") or item.get("resource_id") or "")
                    if not relative.startswith("artifacts/"):
                        raise PluginError("plugin_result_invalid", "非法产物引用")
                    artifacts.append({"kind": str(item.get("kind") or "image")[:32],
                                      "relative": relative,
                                      "name": str(item.get("name") or Path(relative).name)[:80],
                                      "size": int(item.get("size") or 0)})
                response = {
                    "ok": True,
                    "business_ok": bool(result.get("ok", True)),
                    "request_id": request_id,
                    "status": str(result.get("status") or "complete"),
                    "data": dict(result.get("data") or {}),
                    "artifacts": artifacts,
                    "warnings": list(result.get("warnings") or []),
                    "events": sdk.events.items,
                    "error": result.get("error"),
                    "cache": result.get("cache"),
                }
            else:
                raise PluginError("unknown_op", "未知 worker 操作")
        except PluginError as exc:
            response = {"ok": False, "request_id": request_id, "error": exc.public()}
        except Exception as exc:
            response = {"ok": False, "request_id": request_id,
                        "error": {"code": "worker_error", "message": type(exc).__name__}}
        if args.isolated_scope and request.get('op')=='call' and response.get('ok') and response.get('business_ok'):
            files=[];total=0
            for file in data_dir.rglob('*'):
                if file.is_symlink():raise PluginError('storage_invalid','无效缓存引用')
                if file.is_file():
                    content=file.read_bytes();total+=len(content)
                    if total>quota or len(files)>=512:raise PluginError('storage_quota_exceeded','缓存超过预算')
                    files.append({'relative':file.relative_to(data_dir).as_posix(),'sha256':hashlib.sha256(content).hexdigest(),'content':base64.b64encode(content).decode()})
            response['storage_snapshot']=files
        await loop.run_in_executor(None, sys.stdout.write, json.dumps(response, ensure_ascii=False) + "\n")
        await loop.run_in_executor(None, sys.stdout.flush)


if __name__ == "__main__":
    asyncio.run(main())
