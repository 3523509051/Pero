"""Authenticated drain and explicit provider-connection checks."""
import httpx
import os
from fastapi import APIRouter,Depends,HTTPException,Request
from .auth import require_internal
from .credentials import CredentialUnavailable


def control_router(runtime):
    router=APIRouter(prefix='/api/v1/management',dependencies=[Depends(require_internal)])
    def state():
        orchestrator=runtime.orchestrator
        queued=sum(q.depth() for q in orchestrator._queues.values())
        active=len(getattr(orchestrator,'_active_batches',set()))
        http_busy=runtime.sessions.busy_count()
        archive_busy=bool(runtime.archive and runtime.archive._guard.locked())
        jobs=getattr(orchestrator,'_plugin_jobs',None)
        plugin_jobs=sum(not task.done() for task in jobs.tasks.values()) if jobs else 0
        return {'draining':getattr(orchestrator,'_draining',False),
                'busy':bool(queued or active or http_busy or archive_busy or plugin_jobs),
                'queued':queued,'active_batches':active,'http_busy':http_busy,'archive_busy':archive_busy,
                'plugin_jobs':plugin_jobs}
    @router.get('/drain')
    async def drain_state():return state()
    @router.post('/drain')
    async def drain(request:Request):
        try:
            body=await request.json()
            if set(body)!={'enabled'} or type(body['enabled']) is not bool:raise ValueError()
        except (ValueError,TypeError):raise HTTPException(422,detail={'code':'invalid_drain'}) from None
        runtime.orchestrator._draining=body['enabled']
        return state()
    @router.post('/models/check')
    async def check_model(request:Request):
        try:
            body=await request.json()
            if set(body)!={'profile_id'} or not isinstance(body['profile_id'],str):raise ValueError()
            resolver=runtime.credentials
            _name,profile,_thinking=resolver.profile(profile_id=body['profile_id'])
            key=(await resolver.resolve(profile['credential_ref']) if profile.get('credential_ref')
                 else os.environ.get(resolver.provider.api_key_env,'').strip())
            if not key:raise CredentialUnavailable('credential_unavailable')
        except (ValueError,TypeError,CredentialUnavailable):
            raise HTTPException(422,detail={'code':'model_or_credential_unavailable'}) from None
        try:
            async with httpx.AsyncClient(trust_env=False,follow_redirects=False,timeout=15) as client:
                response=await client.get(profile['base_url'].rstrip('/')+'/models',headers={'Authorization':'Bearer '+key})
            listed=False
            if response.status_code==200:
                values=response.json().get('data',[])
                listed=any(isinstance(v,dict) and v.get('id')==profile['model'] for v in values)
            return {'models_endpoint_ok':response.status_code==200,'http_status':response.status_code,
                    'model_listed':listed,'inference_tested':False,'thinking_tested':False}
        except (httpx.HTTPError,ValueError,AttributeError):
            return {'models_endpoint_ok':False,'state':'connection_check_failed','inference_tested':False,'thinking_tested':False}
    return router
