"""plugin.json manifest 的严格解析与校验；不执行任何包内代码。"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Tuple

from pero.plugins.errors import PluginError
from pero.plugins.validation import validate_arguments

PLUGIN_SCHEMA_VERSION = 1
SDK_VERSION = "1"
MAX_MANIFEST_BYTES = 64 * 1024

_ID = re.compile(r"^[a-z][a-z0-9_.-]{1,79}$")
_VERSION = re.compile(r"^\d{1,5}\.\d{1,5}\.\d{1,5}(?:-[0-9A-Za-z.-]{1,40})?$")
_TOOL = re.compile(r"^[a-z][a-z0-9_]{2,63}$")
_ENTRYPOINT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*:[A-Za-z_][A-Za-z0-9_]*$")

#: 只登记真实可授予的能力；未知能力一律拒绝，不猜测含义。
ALLOWED_CAPABILITIES = frozenset({
    "cache.private",
    "network.video_read",
    "network.read",
    "artifact.image",
    "artifact.document",
    "events.emit",
    "models.text",
    "memory.read",
    "knowledge.read",
})


@dataclass(frozen=True)
class PluginManifest:
    id: str
    name: str
    version: str
    sdk_version: str
    entrypoint: str
    license: str
    tools: Tuple[str, ...]
    capabilities: Tuple[str, ...]
    config_schema: Mapping[str, Any]
    description: str = ""
    #: host=可信本地插件在宿主内适配；worker=独立进程，市场来源不得用 host。
    runtime: str = "host"
    #: 固定版本依赖声明（pip requirement 形式）；不自动安装、不执行安装脚本。
    dependencies: Tuple[str, ...] = ()
    network_hosts: Tuple[str, ...] = ()
    background_tools: Tuple[str, ...] = ()
    background_timeout_seconds: int = 180

    @property
    def module(self) -> str:
        return self.entrypoint.split(":", 1)[0]

    @property
    def function(self) -> str:
        return self.entrypoint.split(":", 1)[1]


def _text(value: Any, field: str, limit: int) -> str:
    if not isinstance(value, str):
        raise PluginError("invalid_manifest", f"{field} 必须是字符串")
    result = value.strip()
    if not result or len(result) > limit or any(ord(c) < 32 for c in result):
        raise PluginError("invalid_manifest", f"{field} 取值不合法")
    return result


def _check_schema(node: Any, depth: int = 0) -> None:
    if depth > 6:
        raise PluginError("invalid_manifest", "config_schema 层级过深")
    if isinstance(node, dict):
        if len(node) > 64:
            raise PluginError("invalid_manifest", "config_schema 字段过多")
        for key, value in node.items():
            if not isinstance(key, str) or len(key) > 64:
                raise PluginError("invalid_manifest", "config_schema 键名不合法")
            _check_schema(value, depth + 1)
    elif isinstance(node, (list, tuple)):
        if len(node) > 32:
            raise PluginError("invalid_manifest", "config_schema 数组过长")
        for value in node:
            _check_schema(value, depth + 1)


def parse_manifest(raw: Any) -> PluginManifest:
    """把 JSON 对象解析为清单；任何不合法字段都明确拒绝。"""

    if not isinstance(raw, Mapping):
        raise PluginError("invalid_manifest", "plugin.json 顶层必须是对象")
    version = raw.get("schema_version")
    if isinstance(version, bool) or version != PLUGIN_SCHEMA_VERSION:
        raise PluginError("unsupported_manifest", "清单 schema_version 不受支持")
    sdk = _text(raw.get("sdk_version"), "sdk_version", 16)
    if sdk != SDK_VERSION:
        raise PluginError("unsupported_sdk", "插件 SDK 版本不受支持")
    plugin_id = _text(raw.get("id"), "id", 80)
    if not _ID.fullmatch(plugin_id):
        raise PluginError("invalid_manifest", "插件 ID 格式不合法")
    plugin_version = _text(raw.get("version"), "version", 60)
    if not _VERSION.fullmatch(plugin_version):
        raise PluginError("invalid_manifest", "插件版本号不合法")
    entrypoint = _text(raw.get("entrypoint"), "entrypoint", 128)
    if not _ENTRYPOINT.fullmatch(entrypoint):
        raise PluginError("invalid_manifest", "插件入口格式不合法")
    license_name = _text(raw.get("license"), "license", 80)
    tools = raw.get("tools")
    if not isinstance(tools, (list, tuple)) or not 1 <= len(tools) <= 32:
        raise PluginError("invalid_manifest", "tools 必须是 1-32 个名称")
    tool_names = tuple(_text(name, "tool name", 64) for name in tools)
    if len(set(tool_names)) != len(tool_names) or any(not _TOOL.fullmatch(n) for n in tool_names):
        raise PluginError("invalid_manifest", "工具名不合法或重复")
    caps = raw.get("capabilities", [])
    if not isinstance(caps, (list, tuple)) or len(caps) > len(ALLOWED_CAPABILITIES):
        raise PluginError("invalid_manifest", "capabilities 声明不合法")
    capabilities = tuple(_text(name, "capability", 64) for name in caps)
    if len(set(capabilities)) != len(capabilities) or any(c not in ALLOWED_CAPABILITIES for c in capabilities):
        raise PluginError("capability_denied", "包含未登记的能力声明")
    schema = raw.get("config_schema", {"type": "object", "properties": {}})
    if not isinstance(schema, Mapping) or schema.get("type", "object") != "object":
        raise PluginError("invalid_manifest", "config_schema 必须是对象类型")
    _check_schema(schema)
    if len(json.dumps(schema, ensure_ascii=False)) > 8000:
        raise PluginError("invalid_manifest", "config_schema 内容过大")
    description = raw.get("description", "")
    if not isinstance(description, str) or len(description) > 200:
        raise PluginError("invalid_manifest", "description 过长")
    runtime = raw.get("runtime", "host")
    if runtime not in {"host", "worker"}:
        raise PluginError("invalid_manifest", "runtime 只能是 host 或 worker")
    deps = raw.get("dependencies", [])
    if not isinstance(deps, (list, tuple)) or len(deps) > 16:
        raise PluginError("invalid_manifest", "dependencies 声明不合法")
    dependencies = tuple(_text(item, "dependency", 120) for item in deps)
    if any(not re.fullmatch(r"[A-Za-z0-9_.-]+(==|>=)[0-9A-Za-z.*+!-]+", item) for item in dependencies):
        raise PluginError("invalid_manifest", "依赖必须声明固定版本或最低版本")
    hosts=raw.get('network_hosts',[])
    if not isinstance(hosts,list) or len(hosts)>20 or any(not isinstance(h,str) or not re.fullmatch(r'[a-z0-9-]+(?:\.[a-z0-9-]+)+',h) for h in hosts):
        raise PluginError('invalid_manifest','network_hosts 必须是明确 HTTPS 服务域名')
    if hosts and 'network.read' not in capabilities:raise PluginError('invalid_manifest','缺少 network.read 能力')
    background=raw.get('background_tools',[])
    duration=raw.get('background_timeout_seconds',180)
    if not isinstance(background,list) or any(not isinstance(x,str) or x not in tool_names for x in background) or len(set(background))!=len(background):raise PluginError('invalid_manifest','后台工具必须属于已声明工具')
    if type(duration)!=int or not 10<=duration<=600:raise PluginError('invalid_manifest','后台任务限时须为10-600秒')
    return PluginManifest(
        id=plugin_id,
        name=_text(raw.get("name", plugin_id), "name", 80),
        version=plugin_version,
        sdk_version=sdk,
        entrypoint=entrypoint,
        license=license_name,
        tools=tool_names,
        capabilities=capabilities,
        config_schema=dict(schema),
        description=description.strip(),
        runtime=runtime,
        dependencies=dependencies,
        network_hosts=tuple(hosts),
        background_tools=tuple(background),
        background_timeout_seconds=duration,
    )


def read_manifest(directory: Path) -> Tuple[PluginManifest, str]:
    """读取包目录内的 plugin.json，返回清单与文件 SHA256。"""

    path = Path(directory) / "plugin.json"
    try:
        raw = path.read_bytes()
    except OSError:
        raise PluginError("manifest_not_found", "缺少 plugin.json") from None
    if len(raw) > MAX_MANIFEST_BYTES:
        raise PluginError("invalid_manifest", "plugin.json 超过大小上限")
    try:
        data = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise PluginError("invalid_manifest", "plugin.json 不是合法 UTF-8 JSON") from None
    return parse_manifest(data), hashlib.sha256(raw).hexdigest()


def validate_config(manifest: PluginManifest, value: Any) -> list:
    """按清单 schema 校验实例配置；返回受控错误列表。"""

    if value is None:
        return []
    if not isinstance(value, Mapping):
        return ["config_schema 要求对象类型"]
    return validate_arguments(dict(value), manifest.config_schema, path="config")
