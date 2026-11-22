"""F03 management: scope-bound reads and authenticated versioned configuration."""
import asyncio
import os
import sqlite3
import httpx
from pathlib import Path
from pero.core.settings import repo_root
from pero.configuration.auth import TOKEN_ENV
from .store import ConfigStore
from .sync import ConfigurationSync
from .config_api import configuration_router
from .plugins_api import plugins_router as manager_plugins_router
from pero.configuration.auth import Sessions,auth_router,require_management
from .vault import CredentialVault
from .credential_api import credential_router
from .services import Operations,load_bindings
from .service_api import service_router
from .private_routes import router as private_router
from contextlib import asynccontextmanager
import time
from urllib.parse import quote

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from starlette.middleware.trustedhost import TrustedHostMiddleware

from .scopes import DirectoryUnavailable, ScopeDirectory
from .upstream import CoreReader, CoreUnavailable


def create_manager_app(settings, *, transport=None, service_adapter=None):
    directory = ScopeDirectory(settings.config_path, settings.labels_path)
    reader = CoreReader(settings, transport)
    started = time.monotonic()
    store = ConfigStore(settings.state_dir or (repo_root() / 'data' / 'manager'))
    sync = ConfigurationSync(store, reader)
    state_dir=Path(settings.state_dir or (repo_root() / 'data' / 'manager'))
    vault=CredentialVault(state_dir)
    realm=Sessions()
    operations=Operations(reader,load_bindings(settings.services_path),state_dir/'operations.json',service_adapter)

    @asynccontextmanager
    async def lifespan(app):
        task = asyncio.create_task(sync.run()) if os.environ.get(TOKEN_ENV, '').strip() else None
        try:
            yield
        finally:
            if task is not None:
                task.cancel()
                try:await task
                except asyncio.CancelledError:pass
            await operations.close()
            await reader.close()

    app = FastAPI(title='Pero Manager', version='0.1.0', lifespan=lifespan)
    app.add_middleware(TrustedHostMiddleware, allowed_hosts=list(settings.allowed_hosts))
    app.state.core_reader = reader
    app.state.scope_directory = directory
    app.state.configuration_store = store
    app.state.auth_sessions=realm
    app.state.credential_vault=vault
    app.state.operations=operations
    app.include_router(auth_router(realm))
    app.include_router(credential_router(vault,store))
    app.include_router(service_router(operations))
    app.include_router(private_router)  # 私聊管理路由

    @app.middleware('http')
    async def owner_access(request,call_next):
        if settings.allowed_clients and (request.client is None or request.client.host not in settings.allowed_clients):
            return JSONResponse(status_code=403, content={'detail': {'code': 'client_not_allowed'}})
        public=request.url.path in {'/manage/v1/health','/manage/v1/capabilities'}
        login=request.url.path=='/manage/v1/auth/session' and request.method=='POST'
        if request.url.path.startswith('/manage/') and not public and not login:
            try:require_management(request)
            except HTTPException as exc:return JSONResponse(status_code=exc.status_code,content={'detail':exc.detail})
        if request.url.path.startswith('/manage/v1/scopes'):
            try:
                response = await reader.client.get('/internal/v1/private/manage/scopes',
                    headers={'Authorization':'Bearer ' + os.environ.get(TOKEN_ENV, '').strip()})
                response.raise_for_status()
                payload = response.json()
                directory.replace_private_scopes(payload['scopes'])
            except (httpx.HTTPError, ValueError, TypeError, KeyError, DirectoryUnavailable):
                directory.replace_private_scopes([])
                return JSONResponse(status_code=503,content={'detail':{'code':'private_scopes_unavailable'}})
        return await call_next(request)
    app.include_router(configuration_router(directory, store, sync, reader, vault))
    app.include_router(manager_plugins_router(reader, directory))

    async def store_error(request, exc):
        return JSONResponse(status_code=503, content={'detail':{'code':'manager_config_unavailable'}})
    app.add_exception_handler(sqlite3.Error, store_error)
    app.add_exception_handler(OSError, store_error)

    @app.exception_handler(DirectoryUnavailable)
    async def directory_error(request, exc):
        return JSONResponse(status_code=503, content={'detail': {'code': exc.code}})

    @app.exception_handler(CoreUnavailable)
    async def core_error(request, exc):
        status = 404 if exc.upstream_status == 404 else 503
        return JSONResponse(status_code=status, content={
            'detail': {'code': exc.code, 'upstream_status': exc.upstream_status}})

    async def resolve(scope_id, request):
        if {'agent_id', 'conversation_id', 'session_id', 'scope_id'} & set(request.query_params):
            raise HTTPException(422, detail={'code': 'scope_override_not_allowed'})
        scope = await asyncio.to_thread(directory.get, scope_id)
        if scope is None:
            raise HTTPException(404, detail={'code': 'scope_not_found'})
        return scope

    def api(scope, suffix):
        return '/api/v1/agents/' + quote(scope.current_agent_id, safe='') + suffix

    def envelope(scope, data):
        return {'api_version': 'v1', 'scope_id': scope.scope_id,
                'current_agent_id': scope.current_agent_id, 'data': data}

    @app.get('/manage/v1/health')
    async def health():
        return {'ok': True, 'service': 'pero-manager', 'phase': 'F05',
                'uptime_seconds': round(time.monotonic() - started, 3)}

    @app.get('/manage/v1/capabilities')
    async def capabilities():
        return {'api_version': 'v1', 'phase': 'F05', 'read_only': not bool(os.environ.get(TOKEN_ENV, '').strip()),
                'scope_directory': True, 'scope_bound_reads': True,
                'configuration_write': bool(os.environ.get(TOKEN_ENV, '').strip()), 'runtime_apply': bool(os.environ.get(TOKEN_ENV, '').strip()), 'configuration_protocol_implemented': True,
                'credential_management': True, 'service_control': bool(operations.bindings),
                'plugin_management': True, 'plugin_sdk_version': '1',
                'frontend_delivered': (repo_root() / 'frontend' / 'dist' / 'index.html').is_file(), 'owner_sessions':True}

    @app.get('/manage/v1/status')
    async def status():
        observed = await reader.status()
        # Transport failure does not establish whether the OS process is stopped.
        if 'pero' not in observed:
            observed = {'pero': observed, 'bridge': {'connected': None, 'state': 'unknown'}}
        return {'manager': await health(), **observed, 'process_control_supported': bool(operations.bindings)}

    @app.get('/manage/v1/scopes')
    async def scopes():
        items = await asyncio.to_thread(directory.load)
        return {'api_version': 'v1', 'source': 'configured_routes_and_authorized_private',
                'items': [item.public() for item in items], 'total': len(items)}

    @app.get('/manage/v1/scopes/{scope_id}')
    async def scope_detail(scope_id: str, request: Request):
        return (await resolve(scope_id, request)).public()

    @app.get('/manage/v1/scopes/{scope_id}/memory/records')
    async def records(scope_id: str, request: Request,
                      layer: str = Query('L2', pattern=r'^(raw|L2|L20|L3|L4|journal)$'),
                      limit: int = Query(20, ge=1, le=100), offset: int = Query(0, ge=0)):
        scope = await resolve(scope_id, request)
        data = await reader.read(api(scope, '/memory/records'), {
            'conversation_id': scope.conversation_id, 'layer': layer, 'limit': limit, 'offset': offset})
        return envelope(scope, data)

    @app.get('/manage/v1/scopes/{scope_id}/memory/records/{record_id}')
    async def record_detail(scope_id: str, record_id: str, request: Request):
        scope = await resolve(scope_id, request)
        data = await reader.read(api(scope, '/memory/records/' + quote(record_id, safe='')))
        record = data.get('record')
        if not isinstance(record, dict) or record.get('conversation_id') != scope.conversation_id:
            raise HTTPException(404, detail={'code': 'record_not_in_scope'})
        return envelope(scope, data)

    @app.get('/manage/v1/scopes/{scope_id}/memory/live')
    async def live(scope_id: str, request: Request,
                   layer: str = Query('L1', pattern=r'^(L1|L10)$'), limit: int = Query(20, ge=1, le=50)):
        scope = await resolve(scope_id, request)
        return envelope(scope, await reader.read(api(scope, '/memory/live'), {
            'conversation_id': scope.conversation_id, 'layer': layer, 'limit': limit}))

    @app.get('/manage/v1/scopes/{scope_id}/chat/window')
    async def chat_window(scope_id: str, request: Request,
                          limit: int = Query(0, ge=0, le=200)):
        scope = await resolve(scope_id, request)
        return envelope(scope, await reader.read(api(scope, '/chat/window'), {
            'conversation_id': scope.conversation_id, 'limit': limit}))

    @app.get('/manage/v1/scopes/{scope_id}/chat/history')
    async def chat_history(scope_id: str, request: Request,
                           limit: int = Query(20, ge=1, le=100),
                           offset: int = Query(0, ge=0, le=100000),
                           before_seq: int = Query(0, ge=0)):
        scope = await resolve(scope_id, request)
        return envelope(scope, await reader.read(api(scope, '/chat/history'), {
            'conversation_id': scope.conversation_id,
            'limit': limit, 'offset': offset, 'before_seq': before_seq}))

    @app.get('/manage/v1/scopes/{scope_id}/chat/status')
    async def chat_status(scope_id: str, request: Request):
        scope = await resolve(scope_id, request)
        return envelope(scope, await reader.read(api(scope, '/chat/status'), {
            'conversation_id': scope.conversation_id}))

    @app.get('/manage/v1/scopes/{scope_id}/memory/l2')
    async def memory_l2(scope_id: str, request: Request,
                        limit: int = Query(20, ge=1, le=100),
                        offset: int = Query(0, ge=0, le=100000),
                        state: str = Query('', max_length=16)):
        scope = await resolve(scope_id, request)
        return envelope(scope, await reader.read(api(scope, '/memory/l2'), {
            'conversation_id': scope.conversation_id,
            'limit': limit, 'offset': offset, 'state': state}))

    @app.get('/manage/v1/scopes/{scope_id}/memory/l2/status')
    async def memory_l2_status(scope_id: str, request: Request):
        scope = await resolve(scope_id, request)
        return envelope(scope, await reader.read(api(scope, '/memory/l2/status'), {
            'conversation_id': scope.conversation_id}))

    @app.get('/manage/v1/scopes/{scope_id}/memory/l2/config')
    async def memory_l2_config(scope_id: str, request: Request):
        scope = await resolve(scope_id, request)
        return envelope(scope, await reader.read(api(scope, '/memory/l2/config'), {
            'conversation_id': scope.conversation_id}))

    @app.patch('/manage/v1/scopes/{scope_id}/memory/l2/config')
    async def memory_l2_config_save(scope_id: str, request: Request):
        # Cookie 会话写请求的 CSRF 由 owner_access 中间件（require_management）校验。
        scope = await resolve(scope_id, request)
        try:
            body = await request.json()
        except Exception:  # noqa: BLE001
            raise HTTPException(422, detail={'code': 'invalid_l2_config'}) from None
        if not isinstance(body, dict):
            raise HTTPException(422, detail={'code': 'invalid_l2_config'})
        if set(body) - {'expected_revision', 'trigger_count', 'retain_count', 'enabled'}:
            raise HTTPException(422, detail={'code': 'invalid_l2_config'})
        token = os.environ.get(TOKEN_ENV, '').strip()
        if not token:
            raise HTTPException(503, detail={'code': 'management_not_configured'})
        response = await reader.client.patch(
            api(scope, '/memory/l2/config'),
            params={'conversation_id': scope.conversation_id},
            json=body,
            headers={'Authorization': 'Bearer ' + token},
        )
        if response.status_code >= 400:
            raise HTTPException(
                response.status_code,
                detail={'code': 'revision_conflict'
                        if response.status_code == 409 else 'l2_config_rejected'},
            )
        return envelope(scope, response.json())

    @app.get('/manage/v1/scopes/{scope_id}/chat/l3')
    async def chat_l3(scope_id: str, request: Request,
                      status: str = Query('active', max_length=20),
                      identity_id: str = Query('', max_length=100),
                      limit: int = Query(50, ge=1, le=200)):
        scope = await resolve(scope_id, request)
        return envelope(scope, await reader.read(api(scope, '/chat/l3'), {
            'conversation_id': scope.conversation_id,
            'status': status, 'identity_id': identity_id, 'limit': limit}))

    @app.get('/manage/v1/scopes/{scope_id}/chat/tasks')
    async def chat_tasks(scope_id:str,request:Request):
        scope=await resolve(scope_id,request)
        response=await reader.client.get(api(scope,'/chat/tasks'),params={'conversation_id':scope.conversation_id},headers={'Authorization':'Bearer '+os.environ.get(TOKEN_ENV,'')})
        if response.status_code!=200:raise HTTPException(503,detail={'code':'chat_tasks_unavailable'})
        return envelope(scope,response.json())

    @app.post('/manage/v1/scopes/{scope_id}/chat/tasks/retry')
    async def chat_task_retry(scope_id:str,request:Request):
        scope=await resolve(scope_id,request)
        try:
            body=await request.json()
            if not isinstance(body,dict) or set(body)!={'kind'} or body['kind'] not in {'facts','events','archive'}:raise ValueError()
        except (ValueError,TypeError):raise HTTPException(422,detail={'code':'invalid_chat_task'}) from None
        response=await reader.client.post(api(scope,'/chat/tasks/retry'),params={'conversation_id':scope.conversation_id,'kind':body['kind']},headers={'Authorization':'Bearer '+os.environ.get(TOKEN_ENV,'')})
        if response.status_code!=200:raise HTTPException(404 if response.status_code==404 else 503,detail={'code':'chat_task_retry_rejected'})
        return envelope(scope,response.json())

    @app.post('/manage/v1/scopes/{scope_id}/chat/l3/{fact_id}/status')
    async def chat_l3_status(scope_id: str, fact_id: str, request: Request):
        # Cookie 会话写请求的 CSRF 由 owner_access 中间件（require_management）校验。
        scope = await resolve(scope_id, request)
        body = await request.json()
        token = os.environ.get(TOKEN_ENV, '').strip()
        if not token:
            raise HTTPException(503, detail={'code': 'management_not_configured'})
        response = await reader.client.post(
            api(scope, '/chat/l3/' + quote(fact_id, safe='') + '/status'),
            params={
                'conversation_id': scope.conversation_id,
                'status': str(body.get('status') or ''),
                'reason': str(body.get('reason') or '')[:200],
                'replaced_by': str(body.get('replaced_by') or '')[:80],
            },
            headers={'Authorization': 'Bearer ' + token},
        )
        if response.status_code >= 400:
            raise HTTPException(
                response.status_code,
                detail={'code': 'chat_l3_update_failed',
                        'upstream_status': response.status_code},
            )
        return envelope(scope, response.json())

    @app.get('/manage/v1/scopes/{scope_id}/chat/archive')
    async def chat_archive(scope_id: str, request: Request,
                           limit: int = Query(20, ge=1, le=100)):
        scope = await resolve(scope_id, request)
        return envelope(scope, await reader.read(api(scope, '/chat/archive'), {
            'conversation_id': scope.conversation_id, 'limit': limit}))

    @app.get('/manage/v1/scopes/{scope_id}/chat/events')
    async def chat_events(scope_id: str, request: Request,
                          status: str = Query('', max_length=20),
                          limit: int = Query(20, ge=1, le=100)):
        scope = await resolve(scope_id, request)
        return envelope(scope, await reader.read(api(scope, '/chat/events'), {
            'conversation_id': scope.conversation_id,
            'status': status, 'limit': limit}))

    # ------------------------------------------------------------------ #
    # 群知识库：固定 scope 代理（不接受任意 URL；写请求受 CSRF 检查）
    # ------------------------------------------------------------------ #

    async def kb_call(scope, method: str, suffix: str, *,
                      params=None, body=None):
        token = os.environ.get(TOKEN_ENV, '').strip()
        if not token:
            raise HTTPException(503, detail={'code': 'management_not_configured'})
        merged = dict(params or {})
        merged['conversation_id'] = scope.conversation_id
        response = await reader.client.request(
            method, api(scope, suffix), params=merged, json=body,
            headers={'Authorization': 'Bearer ' + token},
        )
        if response.status_code >= 400:
            detail = None
            try:
                detail = response.json().get('detail')
            except Exception:
                detail = None
            raise HTTPException(
                response.status_code,
                detail=detail or {'code': 'knowledge_upstream_failed',
                                  'upstream_status': response.status_code},
            )
        return envelope(scope, response.json())

    @app.get('/manage/v1/scopes/{scope_id}/knowledge/config')
    async def kb_config(scope_id: str, request: Request):
        scope = await resolve(scope_id, request)
        return await kb_call(scope, 'GET', '/knowledge/config')

    @app.patch('/manage/v1/scopes/{scope_id}/knowledge/config')
    async def kb_patch_config(scope_id: str, request: Request,
                              revision: int = Query(-1)):
        scope = await resolve(scope_id, request)
        body = await request.json()
        if isinstance(body.get('embedding'),dict) and body['embedding'].get('credential_ref'):
            refs={v['ref'] for v in await asyncio.to_thread(vault.metadata)}
            if body['embedding'].get('credential_ref') not in refs:raise HTTPException(422,detail={'code':'credential_not_found'})
        return await kb_call(scope, 'PATCH', '/knowledge/config',
                             params={'revision': revision}, body=body)

    @app.get('/manage/v1/scopes/{scope_id}/knowledge/status')
    async def kb_status(scope_id: str, request: Request):
        scope = await resolve(scope_id, request)
        return await kb_call(scope, 'GET', '/knowledge/status')

    @app.get('/manage/v1/scopes/{scope_id}/knowledge/documents')
    async def kb_documents(scope_id: str, request: Request,
                           query: str = Query('', max_length=100),
                           category: str = Query('', max_length=60),
                           status: str = Query('', max_length=30),
                           limit: int = Query(20, ge=1, le=100),
                           offset: int = Query(0, ge=0, le=100000)):
        scope = await resolve(scope_id, request)
        return await kb_call(scope, 'GET', '/knowledge/documents', params={
            'query': query, 'category': category, 'status': status,
            'limit': limit, 'offset': offset})

    @app.get('/manage/v1/scopes/{scope_id}/knowledge/documents/{doc_id}')
    async def kb_document(scope_id: str, doc_id: str, request: Request):
        scope = await resolve(scope_id, request)
        return await kb_call(scope, 'GET',
                             '/knowledge/documents/' + quote(doc_id, safe=''))

    @app.get('/manage/v1/scopes/{scope_id}/knowledge/documents/{doc_id}/chunks')
    async def kb_chunks(scope_id: str, doc_id: str, request: Request,
                        limit: int = Query(20, ge=1, le=50),
                        offset: int = Query(0, ge=0, le=100000)):
        scope = await resolve(scope_id, request)
        return await kb_call(scope, 'GET',
                             '/knowledge/documents/' + quote(doc_id, safe='') + '/chunks',
                             params={'limit': limit, 'offset': offset})

    @app.post('/manage/v1/scopes/{scope_id}/knowledge/documents/text')
    async def kb_add_text(scope_id: str, request: Request):
        scope = await resolve(scope_id, request)
        return await kb_call(scope, 'POST', '/knowledge/documents/text',
                             body=await request.json())

    @app.post('/manage/v1/scopes/{scope_id}/knowledge/documents/upload')
    async def kb_upload(scope_id: str, request: Request):
        scope = await resolve(scope_id, request)
        return await kb_call(scope, 'POST', '/knowledge/documents/upload',
                             body=await request.json())

    @app.post('/manage/v1/scopes/{scope_id}/knowledge/documents/from-resource')
    async def kb_from_resource(scope_id: str, request: Request):
        scope = await resolve(scope_id, request)
        return await kb_call(scope, 'POST', '/knowledge/documents/from-resource',
                             body=await request.json())

    @app.post('/manage/v1/scopes/{scope_id}/knowledge/documents/upload-stream')
    async def kb_upload_stream(scope_id:str,request:Request,filename:str=Query(...,max_length=200),title:str=Query('',max_length=200),category:str=Query('',max_length=60)):
        scope=await resolve(scope_id,request)
        total=0
        async def stream():
            nonlocal total
            async for chunk in request.stream():
                total+=len(chunk)
                if total>104857600:raise HTTPException(413,detail={'code':'file_too_large'})
                yield chunk
        response=await reader.client.post(api(scope,'/knowledge/documents/upload-stream'),params={'conversation_id':scope.conversation_id,'filename':filename,'title':title,'category':category},headers={'Authorization':'Bearer '+os.environ.get(TOKEN_ENV,'')},content=stream(),timeout=180)
        if response.status_code!=200:raise HTTPException(response.status_code,detail={'code':'knowledge_upload_failed'})
        return envelope(scope,response.json())

    @app.post('/manage/v1/scopes/{scope_id}/knowledge/documents/{doc_id}/versions')
    async def kb_add_version(scope_id: str, doc_id: str, request: Request):
        scope = await resolve(scope_id, request)
        return await kb_call(scope, 'POST',
                             '/knowledge/documents/' + quote(doc_id, safe='') + '/versions',
                             body=await request.json())

    @app.patch('/manage/v1/scopes/{scope_id}/knowledge/documents/{doc_id}')
    async def kb_patch_document(scope_id: str, doc_id: str, request: Request):
        scope = await resolve(scope_id, request)
        return await kb_call(scope, 'PATCH',
                             '/knowledge/documents/' + quote(doc_id, safe=''),
                             body=await request.json())

    @app.delete('/manage/v1/scopes/{scope_id}/knowledge/documents/{doc_id}')
    async def kb_delete_document(scope_id: str, doc_id: str, request: Request):
        scope = await resolve(scope_id, request)
        return await kb_call(scope, 'DELETE',
                             '/knowledge/documents/' + quote(doc_id, safe=''))

    @app.post('/manage/v1/scopes/{scope_id}/knowledge/documents/{doc_id}/reindex')
    async def kb_reindex(scope_id: str, doc_id: str, request: Request):
        scope = await resolve(scope_id, request)
        return await kb_call(scope, 'POST',
                             '/knowledge/documents/' + quote(doc_id, safe='') + '/reindex')

    @app.get('/manage/v1/scopes/{scope_id}/knowledge/jobs')
    async def kb_jobs(scope_id: str, request: Request,
                      limit: int = Query(20, ge=1, le=100)):
        scope = await resolve(scope_id, request)
        return await kb_call(scope, 'GET', '/knowledge/jobs', params={'limit': limit})

    @app.post('/manage/v1/scopes/{scope_id}/knowledge/jobs/{job_id}/retry')
    async def kb_retry_job(scope_id: str, job_id: str, request: Request):
        scope = await resolve(scope_id, request)
        return await kb_call(scope, 'POST',
                             '/knowledge/jobs/' + quote(job_id, safe='') + '/retry')

    @app.post('/manage/v1/scopes/{scope_id}/knowledge/search')
    async def kb_search(scope_id: str, request: Request):
        scope = await resolve(scope_id, request)
        return await kb_call(scope, 'POST', '/knowledge/search',
                             body=await request.json())

    @app.get('/manage/v1/scopes/{scope_id}/resources')
    async def resources(scope_id: str, request: Request,
                        limit: int = Query(20, ge=1, le=100), offset: int = Query(0, ge=0),kind: str=Query('',pattern='^(|image|document)$')):
        scope = await resolve(scope_id, request)
        return envelope(scope, await reader.read(api(scope, '/resources'), {
            'conversation_id': scope.conversation_id, 'limit': limit, 'offset': offset,'kind':kind}))

    @app.get('/manage/v1/scopes/{scope_id}/stickers')
    async def stickers(scope_id: str, request: Request,
                       limit: int = Query(100, ge=1, le=100), offset: int = Query(0, ge=0)):
        scope = await resolve(scope_id, request)
        return envelope(scope, await reader.read(api(scope, '/stickers'), {
            'conversation_id': scope.conversation_id, 'limit': limit, 'offset': offset}))

    @app.get('/manage/v1/scopes/{scope_id}/resources/{resource_id}/image')
    async def resource_image(scope_id: str, resource_id: str, request: Request):
        import re
        from fastapi.responses import Response
        if not re.fullmatch(r'r-[0-9a-f]{32}',resource_id):
            raise HTTPException(404,detail={'code':'resource_not_found'})
        scope = await resolve(scope_id,request)
        path=api(scope,'/resources/'+quote(resource_id,safe=''))
        params={'conversation_id':scope.conversation_id}
        meta=(await reader.read(path,params)).get('item',{})
        if meta.get('kind')!='image' or meta.get('mime') not in {'image/jpeg','image/png','image/gif','image/webp','image/bmp','image/tiff'} or not 0<int(meta.get('size',0))<=10_000_000:
            raise HTTPException(404,detail={'code':'image_preview_unavailable'})
        try:
            response=await reader.client.get(path+'/content',params=params)
        except Exception:
            raise HTTPException(503,detail={'code':'image_preview_unavailable'}) from None
        if response.status_code!=200 or len(response.content)!=meta['size']:
            raise HTTPException(503,detail={'code':'image_preview_unavailable'})
        return Response(response.content,media_type=meta['mime'],headers={'Cache-Control':'private, no-store','X-Content-Type-Options':'nosniff'})

    # F05: serve only the built console and its public assets. API routes stay
    # under the existing authenticated management namespace.
    from fastapi.responses import FileResponse
    from fastapi.staticfiles import StaticFiles
    frontend = repo_root() / 'frontend' / 'dist'
    if (frontend / 'index.html').is_file():
        app.mount('/assets', StaticFiles(directory=frontend / 'assets'), name='console-assets')

        @app.get('/mascot-home.png', include_in_schema=False)
        async def mascot():
            return FileResponse(frontend / 'mascot-home.png')

        @app.get('/', include_in_schema=False)
        @app.get('/agents', include_in_schema=False)
        @app.get('/agents/persona', include_in_schema=False)
        @app.get('/agents/wake', include_in_schema=False)
        @app.get('/channels', include_in_schema=False)
        @app.get('/tools', include_in_schema=False)
        @app.get('/tools/{tool_id}', include_in_schema=False)
        @app.get('/plugins', include_in_schema=False)
        @app.get('/memory', include_in_schema=False)
        @app.get('/knowledge', include_in_schema=False)
        @app.get('/chat', include_in_schema=False)
        @app.get('/resources', include_in_schema=False)
        @app.get('/settings', include_in_schema=False)
        async def console():
            return FileResponse(frontend / 'index.html', headers={'Cache-Control': 'no-store'})

    from .stickers import sticker_router
    app.include_router(sticker_router(reader, resolve))
    return app
