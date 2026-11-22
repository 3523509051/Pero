"""Pero 插件 SDK v1。

包初始化保持轻量：独立 worker 只导入 errors/manifest/sdk，不触发宿主
工具注册链与 pydantic 依赖。公开符号按需延迟解析。
"""

from __future__ import annotations

from typing import Any

from .errors import PluginError

__all__ = ["PluginError", "PluginManifest", "parse_manifest", "PluginInstaller"]


def __getattr__(name: str) -> Any:
    if name in {"PluginManifest", "parse_manifest"}:
        from .manifest import PluginManifest, parse_manifest
        return {"PluginManifest": PluginManifest, "parse_manifest": parse_manifest}[name]
    if name == "PluginInstaller":
        from .installer import PluginInstaller
        return PluginInstaller
    raise AttributeError(name)
