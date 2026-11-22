"""Core 内部插件管理路由：只接受管理令牌，不向外网暴露。"""

from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field

from pero.configuration.auth import require_internal
from pero.plugins.errors import PluginError
from pero.plugins.github_source import GitHubSource


class InstallRequest(BaseModel):
    source: str = Field(min_length=1, max_length=500)
    expected_sha256: Optional[str] = Field(default=None, max_length=64)


class ConfigRequest(BaseModel):
    config: dict[str, Any] = Field(default_factory=dict)


class VersionRequest(BaseModel):
    version: str = Field(default="", max_length=60)


class NetworkRequest(BaseModel):
    hosts:list[str]=Field(default_factory=list,max_length=20)


class ScopeRequest(BaseModel):
    conversation_id: str = Field(min_length=1, max_length=200)
    enabled: bool | None = None
    config: dict[str, Any] | None = None


class SourcePreviewRequest(BaseModel):
    url: str = Field(min_length=1, max_length=500)
    ref: str = Field(default="", max_length=120)


class GithubInstallRequest(BaseModel):
    url: str = Field(min_length=1, max_length=500)
    commit: str = Field(min_length=40, max_length=40)
    package_sha256: str = Field(min_length=64, max_length=64)
    manifest_sha256: str = Field(min_length=64, max_length=64)


def _status(exc: PluginError) -> int:
    if exc.code in {"not_installed", "version_not_installed", "job_not_found"}:
        return 404
    if exc.code in {"duplicate_tool", "version_conflict", "invalid_config", "declaration_mismatch",
                    "remote_host_runtime_denied", "package_hash_mismatch", "manifest_hash_mismatch"}:
        return 409
    if exc.code in {"invalid_source", "source_not_allowed", "invalid_scope", "invalid_github_url",
                    "ref_required", "ref_not_found", "invalid_commit", "dependency_lock_missing",
                    "license_missing", "dependency_not_approved"}:
        return 422
    return 400


def plugins_router(installer: Any) -> APIRouter:
    router = APIRouter(
        prefix="/api/v1/plugins",
        dependencies=[Depends(require_internal)],
    )

    @router.get('/jobs')
    async def jobs():return installer.jobs.snapshot()

    @router.delete('/jobs/finished')
    async def clear_finished_jobs():
        try:return await installer.jobs.remove()
        except PluginError as exc:raise HTTPException(status_code=_status(exc),detail=exc.public()) from None

    @router.delete('/jobs/{job_id}')
    async def delete_job(job_id:str):
        try:return await installer.jobs.remove(job_id)
        except PluginError as exc:raise HTTPException(status_code=_status(exc),detail=exc.public()) from None

    @router.post('/jobs/{job_id}/cancel')
    async def cancel_job(job_id:str):
        try:return await installer.jobs.cancel(job_id)
        except PluginError as exc:raise HTTPException(status_code=_status(exc),detail=exc.public()) from None

    @router.get("")
    async def list_plugins():
        return installer.snapshot()

    @router.post("/install")
    async def install(request: InstallRequest):
        try:
            return await installer.install(
                Path(request.source), expected_sha256=request.expected_sha256 or ""
            )
        except PluginError as exc:
            raise HTTPException(status_code=_status(exc), detail=exc.public()) from None

    @router.post("/source/preview")
    async def source_preview(request: SourcePreviewRequest):
        try:
            return await GitHubSource().preview(request.url, request.ref)
        except PluginError as exc:
            raise HTTPException(status_code=_status(exc), detail=exc.public()) from None

    @router.post("/install-github")
    async def install_github(request: GithubInstallRequest):
        source = GitHubSource()
        package: Path | None = None
        try:
            package, _manifest, _preview = await source.fetch(
                request.url, request.commit,
                expected_package_sha256=request.package_sha256,
            )
            return await installer.install(
                package,
                expected_sha256=request.manifest_sha256,
                trust="remote_reviewed",
                source_kind="github",
                source_url=request.url,
                commit=request.commit,
                package_sha256=request.package_sha256,
                allow_external=True,
            )
        except PluginError as exc:
            raise HTTPException(status_code=_status(exc), detail=exc.public()) from None
        finally:
            if package is not None:
                shutil.rmtree(package, ignore_errors=True)

    @router.get("/{plugin_id}")
    async def detail(plugin_id: str):
        try:
            return installer.detail(plugin_id)
        except PluginError as exc:
            raise HTTPException(status_code=_status(exc), detail=exc.public()) from None

    @router.delete("/{plugin_id}")
    async def uninstall(plugin_id: str):
        try:
            return await installer.uninstall(plugin_id)
        except PluginError as exc:
            raise HTTPException(status_code=_status(exc), detail=exc.public()) from None

    @router.patch("/{plugin_id}/config")
    async def config(plugin_id: str, request: ConfigRequest):
        try:
            return await installer.set_config(plugin_id, request.config)
        except PluginError as exc:
            raise HTTPException(status_code=_status(exc), detail=exc.public()) from None

    @router.post("/{plugin_id}/enable")
    async def enable(plugin_id: str, request: VersionRequest):
        try:
            return await installer.enable(plugin_id, request.version)
        except PluginError as exc:
            raise HTTPException(status_code=_status(exc), detail=exc.public()) from None

    @router.post("/{plugin_id}/disable")
    async def disable(plugin_id: str):
        try:
            return await installer.disable(plugin_id)
        except PluginError as exc:
            raise HTTPException(status_code=_status(exc), detail=exc.public()) from None

    @router.post("/{plugin_id}/update")
    async def update(plugin_id: str, request: VersionRequest):
        if not request.version:
            raise HTTPException(status_code=422, detail={"code": "version_required"})
        try:
            return await installer.enable(plugin_id, request.version)
        except PluginError as exc:
            raise HTTPException(status_code=_status(exc), detail=exc.public()) from None

    @router.put('/{plugin_id}/network')
    async def network(plugin_id:str,request:NetworkRequest):
        try:return await installer.set_network_hosts(plugin_id,request.hosts)
        except PluginError as exc:raise HTTPException(status_code=_status(exc),detail=exc.public()) from None

    @router.put("/scopes/{scope_id}/plugins/{plugin_id}")
    async def scope(plugin_id: str, scope_id: str, request: ScopeRequest):
        try:
            return await installer.set_scope(plugin_id, request.conversation_id, request.enabled, request.config)
        except PluginError as exc:
            raise HTTPException(status_code=_status(exc), detail=exc.public()) from None

    return router


__all__ = ["plugins_router"]
