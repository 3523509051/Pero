"""Manager 侧插件管理代理：复用现有认证/CSRF，只转发固定内部路由。"""

from __future__ import annotations

import asyncio
import os
import re
from typing import Any, Optional
from urllib.parse import quote

import httpx
from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field

from pero.configuration.auth import TOKEN_ENV, require_management

_SUFFIX = re.compile(r"^/[A-Za-z0-9/_{}.\-]{0,240}$")
_ID = re.compile(r"^[A-Za-z0-9_.-]{1,80}$")


class InstallBody(BaseModel):
    source: str = Field(min_length=1, max_length=500)
    expected_sha256: Optional[str] = Field(default=None, max_length=64)


class ConfigBody(BaseModel):
    config: dict[str, Any] = Field(default_factory=dict)


class VersionBody(BaseModel):
    version: str = Field(default="", max_length=60)


class NetworkBody(BaseModel):
    hosts:list[str]=Field(default_factory=list,max_length=20)


class ScopeBody(BaseModel):
    enabled: bool | None = None
    config: dict[str, Any] | None = None


class SourcePreviewBody(BaseModel):
    url: str = Field(min_length=1, max_length=500)
    ref: str = Field(default="", max_length=120)


class GithubInstallBody(BaseModel):
    url: str = Field(min_length=1, max_length=500)
    commit: str = Field(min_length=40, max_length=40)
    package_sha256: str = Field(min_length=64, max_length=64)
    manifest_sha256: str = Field(min_length=64, max_length=64)


def plugins_router(reader: Any, directory: Any) -> APIRouter:
    router = APIRouter(prefix="/manage/v1", dependencies=[Depends(require_management)])

    async def proxy(method: str, suffix: str, payload: Optional[dict] = None):
        if suffix and not _SUFFIX.fullmatch(suffix):
            raise HTTPException(422, detail={"code": "invalid_plugin_route"})
        token = os.environ.get(TOKEN_ENV, "").strip()
        if not token:
            raise HTTPException(503, detail={"code": "management_not_configured"})
        try:
            response = await reader.client.request(
                method,
                "/api/v1/plugins" + suffix,
                json=payload,
                headers={"Authorization": "Bearer " + token},
            )
        except httpx.HTTPError:
            raise HTTPException(503, detail={"code": "core_unreachable"}) from None
        try:
            data = response.json()
        except ValueError:
            raise HTTPException(502, detail={"code": "core_invalid_response"}) from None
        if not isinstance(data, dict):
            raise HTTPException(502, detail={"code": "core_invalid_response"})
        if response.status_code >= 400:
            detail = data.get("detail")
            raise HTTPException(response.status_code, detail=detail if isinstance(detail, dict)
                                else {"code": "core_plugin_rejected"})
        return data

    def ident(plugin_id: str) -> str:
        if not _ID.fullmatch(plugin_id):
            raise HTTPException(422, detail={"code": "invalid_plugin_id"})
        return quote(plugin_id, safe="")

    @router.get('/plugins/jobs')
    async def jobs():return await proxy('GET','/jobs')

    @router.delete('/plugins/jobs/finished')
    async def clear_finished_jobs():return await proxy('DELETE','/jobs/finished')

    @router.delete('/plugins/jobs/{job_id}')
    async def delete_job(job_id:str):
        if not re.fullmatch(r'job-[0-9a-f]{32}',job_id):raise HTTPException(422,detail={'code':'invalid_job_id'})
        return await proxy('DELETE','/jobs/'+job_id)

    @router.post('/plugins/jobs/{job_id}/cancel')
    async def cancel_job(job_id:str):
        if not re.fullmatch(r'job-[0-9a-f]{32}',job_id):raise HTTPException(422,detail={'code':'invalid_job_id'})
        return await proxy('POST','/jobs/'+job_id+'/cancel')

    @router.get("/plugins")
    async def list_plugins():
        return await proxy("GET", "")

    @router.post("/plugins/install")
    async def install(body: InstallBody):
        return await proxy("POST", "/install", body.model_dump())

    @router.post("/plugins/source/preview")
    async def source_preview(body: SourcePreviewBody):
        return await proxy("POST", "/source/preview", body.model_dump())

    @router.post("/plugins/install-github")
    async def install_github(body: GithubInstallBody):
        return await proxy("POST", "/install-github", body.model_dump())

    @router.get("/plugins/{plugin_id}")
    async def detail(plugin_id: str):
        return await proxy("GET", "/" + ident(plugin_id))

    @router.delete("/plugins/{plugin_id}")
    async def uninstall(plugin_id: str):
        return await proxy("DELETE", "/" + ident(plugin_id))

    @router.patch("/plugins/{plugin_id}/config")
    async def config(plugin_id: str, body: ConfigBody):
        return await proxy("PATCH", "/" + ident(plugin_id) + "/config", body.model_dump())

    @router.post("/plugins/{plugin_id}/enable")
    async def enable(plugin_id: str, body: VersionBody):
        return await proxy("POST", "/" + ident(plugin_id) + "/enable", body.model_dump())

    @router.post("/plugins/{plugin_id}/disable")
    async def disable(plugin_id: str):
        return await proxy("POST", "/" + ident(plugin_id) + "/disable")

    @router.post("/plugins/{plugin_id}/update")
    async def update(plugin_id: str, body: VersionBody):
        return await proxy("POST", "/" + ident(plugin_id) + "/update", body.model_dump())

    @router.put('/plugins/{plugin_id}/network')
    async def network(plugin_id:str,body:NetworkBody):
        return await proxy('PUT','/'+ident(plugin_id)+'/network',body.model_dump())

    @router.put("/scopes/{scope_id}/plugins/{plugin_id}")
    async def scope(plugin_id: str, scope_id: str, body: ScopeBody):
        if not _ID.fullmatch(plugin_id):
            raise HTTPException(422, detail={"code": "invalid_plugin_id"})
        conversation = await asyncio.to_thread(directory.get, scope_id)
        if conversation is None:
            raise HTTPException(404, detail={"code": "scope_not_found"})
        payload = {"conversation_id": conversation.conversation_id, "enabled": body.enabled, "config": body.config}
        return await proxy("PUT", "/scopes/" + quote(scope_id, safe="")
                           + "/plugins/" + quote(plugin_id, safe=""), payload)

    return router


__all__ = ["plugins_router"]
