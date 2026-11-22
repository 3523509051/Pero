from __future__ import annotations
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.routing import APIRoute
from pydantic import BaseModel, ConfigDict, Field, SecretStr
from pero.access import AccessError, service_for
from pero.configuration.auth import require_internal

class SafeRoute(APIRoute):
    def get_route_handler(self):
        handler=super().get_route_handler()
        async def safe(request):
            try:return await handler(request)
            except RequestValidationError:
                raise HTTPException(422,detail={'code':'invalid_private_request'}) from None
        return safe

router=APIRouter(route_class=SafeRoute,prefix='/internal/v1/private',tags=['internal-private'],dependencies=[Depends(require_internal)])

class Model(BaseModel):
    model_config=ConfigDict(extra='forbid',hide_input_in_errors=True)

class Redeem(Model):
    channel_id:str
    account_id:str
    user_ref:str
    user_wxid:str
    activation_code:SecretStr
    source_event_id:str
    display_name:str|None=None

class Token(Model):
    channel_id:str
    account_id:str
    label:str=Field(default='',max_length=200)
    hours_valid:float=24
    agent_id:str|None=None
    request_id:str=Field(min_length=1,max_length=128)

class Status(Model):
    status:str
    revision:int

class Groups(Model):
    conversation_ids:list[str]
    revision:int

class Policy(Model):
    revision:int
    quiet_seconds:float|None=None
    max_wait_seconds:float|None=None
    prompt:str|None=None
    failure_strategy:str|None=None
    max_messages:int|None=None
    max_chars:int|None=None
    # 私聊激活窗口时长（P04）：1–120 秒；缺省/旧数据按 30 补齐。
    window_seconds:float|None=None

class Transfer(Model):
    grant_id:str
    revision:int
    confirm:bool=False

async def service(request:Request):
    runtime=getattr(request.app.state,'runtime',None)
    if runtime is None:raise HTTPException(503,detail={'code':'runtime_unavailable'})
    try:
        svc=service_for(runtime)
        if svc._snapshot is None:await svc.initialize()
        return svc
    except AccessError as exc:raise HTTPException(exc.status,detail={'code':exc.code}) from None

async def call(svc,method,**kwargs):
    try:return await svc.run(method,**kwargs)
    except AccessError as exc:raise HTTPException(exc.status,detail={'code':exc.code}) from None

@router.get('/authorized')
async def authorized(svc=Depends(service)):
    grants=await call(svc,'list_grants')
    result=[g for g in grants['grants'] if g['status']=='active']
    return {'privates':result,'count':len(result),'revision':grants['revision']}

@router.post('/redeem')
async def redeem(req:Redeem,svc=Depends(service)):
    values=req.model_dump();values['activation_code']=req.activation_code.get_secret_value()
    return await call(svc,'redeem',**values)

@router.get('/manage/tokens')
async def tokens(svc=Depends(service)):return await call(svc,'list_tokens')

@router.post('/manage/tokens')
async def create_token(req:Token,svc=Depends(service)):return await call(svc,'create_token',**req.model_dump())

@router.post('/manage/tokens/{token_id}/revoke')
async def revoke_token(token_id:str,svc=Depends(service)):return await call(svc,'revoke_token',token_id=token_id)

@router.get('/manage/grants')
async def grants(svc=Depends(service)):return await call(svc,'list_grants')

@router.get('/manage/grants/{grant_id}')
async def grant(grant_id:str,svc=Depends(service)):
    data=await call(svc,'list_grants')
    item=next((g for g in data['grants'] if g['grant_id']==grant_id),None)
    if item is None:raise HTTPException(404,detail={'code':'grant_not_found'})
    return item

@router.patch('/manage/grants/{grant_id}')
async def update_grant(grant_id:str,req:Status,svc=Depends(service)):return await call(svc,'update_grant',grant_id=grant_id,**req.model_dump())

@router.get('/manage/owner')
async def owner(svc=Depends(service)):return await call(svc,'get_owner')

@router.get('/manage/owner/group-access')
async def groups(svc=Depends(service)):return await call(svc,'group_access')

@router.patch('/manage/owner/group-access')
async def update_groups(req:Groups,svc=Depends(service)):return await call(svc,'update_group_access',**req.model_dump())

@router.get('/manage/policy')
async def policy(svc=Depends(service)):return await call(svc,'get_policy')

@router.patch('/manage/policy')
async def update_policy(req:Policy,svc=Depends(service)):
    values=req.model_dump(exclude_none=True)
    current=svc.policy_snapshot();current.pop('revision',None)
    return await call(svc,'update_policy',**{**current,**values})

@router.post('/manage/owner/transfer')
async def transfer(req:Transfer,svc=Depends(service)):return await call(svc,'transfer_owner',**req.model_dump())

@router.get('/manage/scopes')
async def scopes(svc=Depends(service)):
    return {'scopes':svc.private_scopes()}
