"""Versioned local administration APIs; writes are never Agent tools."""
import asyncio
from fastapi import APIRouter, Depends, HTTPException, Request
from pero.configuration.auth import require_management
from pero.configuration.models import SaveRequest, validate_changes
from pero.configuration.catalog import validate_defaults
from .store import ConfigurationConflict
from .upstream import CoreUnavailable


def configuration_router(directory, store, sync, reader, vault):
    router = APIRouter(prefix='/manage/v1', dependencies=[Depends(require_management)])
    save_lock = asyncio.Lock()

    async def cid(scope_id):
        scope = await asyncio.to_thread(directory.get,scope_id)
        if scope is None:raise HTTPException(404,detail={'code':'scope_not_found'})
        return scope.conversation_id

    async def view(kind, scope, item):
        async with save_lock:
            doc = await asyncio.to_thread(store.get,kind,scope,item)
            snapshot = await asyncio.to_thread(store.snapshot)
        application = await sync.state()
        result = {**doc,'application':application,'effective_from':'next_turn',
                  'configuration_generation':snapshot.generation}
        if kind=='tool':
            merged = {**snapshot.tools.get('*',{}).get(item,{}), **snapshot.tools.get(scope,{}).get(item,{})}
            definitions=await reader.management('GET','/tools')
            definition=next((d for d in definitions.get('items',[]) if d['tool_id']==item),{})
            result['default_prompt']=definition.get('default_prompt','')
            result['effective_prompt']=merged.get('prompt') or result['default_prompt']
            result['prompt_source']=definition.get('prompt_source')
            result['editable_fields']=definition.get('editable_fields',[])
            result['ui_fields']=definition.get('ui_fields',[])
            result['capabilities']=definition.get('capabilities',{})
            result['code_configuration']=definition.get('code_configuration',{})
            defaults=result['code_configuration'].get('defaults',{})
            result['inherited_configuration']={**defaults,**snapshot.tools.get('*',{}).get(item,{})} if scope!='*' else defaults
            result['overrides']=doc['value']
            desired={**defaults,**merged}
            result['desired_configuration']=desired
            result['effective_configuration']=(desired if application['state']=='applied'
                                               and application['generation']==snapshot.generation else None)
        else:result['inherits_agent_default']=not bool(doc['value'].get('prompt'))
        return result

    async def save(kind, scope, item, request):
        try:
            payload = SaveRequest.model_validate(await request.json())
            changes = validate_changes(kind,payload.changes)
            if not changes:raise ValueError('empty_changes')
        except (ValueError,TypeError):
            raise HTTPException(422,detail={'code':'configuration_invalid'}) from None
        async with save_lock:
            current = await asyncio.to_thread(store.get,kind,scope,item)
            candidate = {**current['value'],**changes}
            candidate = {k:v for k,v in candidate.items() if v is not None}
            if candidate.get('credential_ref'):
                refs={v['ref'] for v in await asyncio.to_thread(vault.metadata)}
                if candidate['credential_ref'] not in refs:raise HTTPException(422,detail={'code':'credential_not_found'})
            if kind=='tool':
                try:validate_defaults(item,candidate)
                except ValueError:raise HTTPException(422,detail={'code':'tool_configuration_invalid'}) from None
                if candidate.get('enabled') is True:
                    try:
                        catalog = await reader.management('GET','/tools')
                    except CoreUnavailable:
                        catalog = None  # Offline changes remain explicitly pending.
                    if catalog is not None:
                        definition = next((tool for tool in catalog.get('items',[]) if tool['tool_id']==item), None)
                        if not definition or not definition['registered'] or not definition['backend_enabled']:
                            raise HTTPException(409,detail={'code':'tool_backend_not_ready'})
            try:
                receipt = await asyncio.to_thread(store.save,kind,scope,item,payload,changes)
            except ConfigurationConflict as exc:
                raise HTTPException(409,detail={'code':str(exc)}) from None
            application = await sync.apply()
            doc = await asyncio.to_thread(store.get,kind,scope,item)
            return {**receipt,'document':doc,'application':application,'effective_from':'next_turn'}

    @router.get('/config/schema')
    async def schemas():
        from pero.configuration.models import PersonaChanges,ToolChanges,WakeChanges
        from pero.configuration.model_policy import ModelChanges,PolicyChanges
        return {'persona':PersonaChanges.model_json_schema(),'tool':ToolChanges.model_json_schema(),
                'model':ModelChanges.model_json_schema(),'policy':PolicyChanges.model_json_schema(),'wake':WakeChanges.model_json_schema(),
                'save':SaveRequest.model_json_schema()}

    @router.get('/config/state')
    async def state():return await sync.state()

    @router.post('/config/apply')
    async def apply():return await sync.apply()

    @router.get('/scopes/{scope_id}/persona')
    async def persona(scope_id:str):
        scope=await asyncio.to_thread(directory.get,scope_id)
        if scope is None:raise HTTPException(404,detail={'code':'scope_not_found'})
        result=await view('persona',scope.conversation_id,'persona')
        from pero.core.settings import load_settings
        agent=(await asyncio.to_thread(load_settings,directory.config_path)).find_agent(scope.current_agent_id)
        result['agent_default_prompt']=agent.persona if agent else None
        result['conversation_type']=scope.conversation_type
        result['inherits_agent_default']=not bool(result['value'].get('prompt'))
        result['effective_prompt']=(result['value'].get('prompt') or result['agent_default_prompt']) if result['application']['state']=='applied' else None
        return result

    @router.patch('/scopes/{scope_id}/persona')
    async def save_persona(scope_id:str,request:Request):return await save('persona',await cid(scope_id),'persona',request)

    @router.get('/tools')
    async def tools():return await reader.management('GET','/tools')

    @router.get('/scopes/{scope_id}/wake')
    async def wake(scope_id:str):
        conversation=await cid(scope_id)
        result=await view('wake',conversation,'wake')
        from pero.core.settings import load_settings
        settings=await asyncio.to_thread(load_settings,directory.config_path)
        defaults={k:getattr(settings.orchestration,k) for k in ('wake_words','wake_match','at_is_trusted_mention')}
        defaults['group_enabled']=True
        result['defaults']=defaults
        result['desired_configuration']={**defaults,**result['value']}
        result['effective_configuration']=result['desired_configuration'] if result['application']['state']=='applied' else None
        return result

    @router.patch('/scopes/{scope_id}/wake')
    async def save_wake(scope_id:str,request:Request):return await save('wake',await cid(scope_id),'wake',request)

    @router.get('/tools/{tool_id}/config')
    async def tool_config(tool_id:str):
        try:validate_defaults(tool_id,{})
        except ValueError:raise HTTPException(404,detail={'code':'unknown_tool'}) from None
        return await view('tool','*',tool_id)

    @router.patch('/tools/{tool_id}/config')
    async def save_tool(tool_id:str,request:Request):return await save('tool','*',tool_id,request)

    @router.get('/scopes/{scope_id}/tools/{tool_id}/config')
    async def scoped_tool(scope_id:str,tool_id:str):
        try:validate_defaults(tool_id,{})
        except ValueError:raise HTTPException(404,detail={'code':'unknown_tool'}) from None
        return await view('tool',await cid(scope_id),tool_id)

    @router.patch('/scopes/{scope_id}/tools/{tool_id}/config')
    async def save_scoped_tool(scope_id:str,tool_id:str,request:Request):return await save('tool',await cid(scope_id),tool_id,request)

    @router.get('/models')
    async def models():
        snapshot=await asyncio.to_thread(store.snapshot)
        refs={v['ref'] for v in await asyncio.to_thread(vault.metadata)}
        try:actual=await reader.management('GET','/models')
        except CoreUnavailable:actual=None
        return {'runtime':actual,'profiles':{name:{**value,'credential_present':(value.get('credential_ref') in refs if value.get('credential_ref') else None)}
                            for name,value in snapshot.models.items()},'policy':snapshot.policy,'application':await sync.state()}

    @router.get('/provider-options')
    async def provider_options():
        return {'deepseek':{'base_url':'https://api.deepseek.com','models':[
            {'id':'deepseek-flash','label':'DeepSeek Flash'},
            {'id':'deepseek-v4-pro','label':'DeepSeek V4 Pro'}],
            'source':'https://api-docs.deepseek.com/quick_start/pricing/','verified_at':'2026-11-20'},
            'custom':{'protocol':'openai_chat_completions','full_feature_compatibility_guaranteed':False},
            'amap':{'credential_kind':'web_service','tools':['amap_weather','amap_places','amap_route']}}

    @router.get('/models/{profile_id}/config')
    async def model_config(profile_id:str):return await view('model','*',profile_id)

    @router.patch('/models/{profile_id}/config')
    async def save_model(profile_id:str,request:Request):
        import re
        if not re.fullmatch(r'[A-Za-z0-9_-]{1,80}',profile_id):raise HTTPException(422,detail={'code':'invalid_profile_id'})
        return await save('model','*',profile_id,request)

    @router.get('/model-policy/config')
    async def policy():return await view('policy','*','default')

    @router.patch('/model-policy/config')
    async def save_policy(request:Request):return await save('policy','*','default',request)

    @router.post('/models/{profile_id}/check')
    async def check_model(profile_id:str):return await reader.management('POST','/models/check',{'profile_id':profile_id})

    return router
