"""Internal authenticated configuration publication; separate from business tools."""
import os
from fastapi import APIRouter, Depends, HTTPException, Request
from .auth import require_internal
from .catalog import tool_catalog


def management_router(runtime):
    router = APIRouter(prefix='/api/v1/management', dependencies=[Depends(require_internal)])

    @router.get('/config-state')
    async def state():
        return runtime.configuration.state()

    @router.put('/config-snapshot')
    async def apply(request: Request):
        try:
            result = runtime.configuration.apply(await request.json())
        except OSError:
            raise HTTPException(503, detail={'code':'configuration_storage_unavailable'}) from None
        except (ValueError, TypeError) as exc:
            known = {'unknown_tool','invalid_tool_defaults','tool_backend_not_registered','tool_backend_disabled',
                     'configuration_source_or_revision_conflict','configuration_revision_conflict','unknown_configuration_scope'}
            reason = str(exc) if str(exc) in known else 'configuration_invalid'
            raise HTTPException(422, detail={'code': 'configuration_rejected','reason':reason}) from None
        if runtime.events is not None:
            runtime.events.publish('component.status', data={'component_id':'configuration',
                'state':'applied', 'generation':result['generation']})
        return {**result, 'state':'applied', 'effective_from':'next_turn'}

    @router.get('/tools')
    async def tools():
        return {'items': tool_catalog(runtime.tools, runtime.configuration.snapshot('*'), runtime.settings),
                'generation': runtime.configuration.state()['generation']}

    @router.get('/models')
    async def models():
        profiles,policy=runtime.configuration.snapshot('*').model_configuration()
        names=set(profiles)|{'default'}
        values={}
        for name in sorted(names):
            _id,profile,_thinking=runtime.credentials.profile(profile_id=name)
            values[name]={**profile,'credential_source':'reference' if profile.get('credential_ref') else 'environment',
                          'credential_configured':bool(profile.get('credential_ref')) or bool(os.environ.get(runtime.settings.provider.api_key_env,'').strip())}
        return {'profiles':values,'policy':policy,'generation':runtime.configuration.state()['generation']}

    return router
