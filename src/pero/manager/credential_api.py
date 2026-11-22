"""Write-only public credentials and bearer-only private resolution."""
import asyncio
from fastapi import APIRouter,Depends,HTTPException,Request,Query
from pero.configuration.auth import require_management,require_internal
from .vault import VaultError


def credential_router(vault,store):
    router=APIRouter()
    async def parse(request):
        try:
            body=await request.json()
            if not isinstance(body,dict) or set(body)-{'label','value','expected_revision'}:raise ValueError()
            return body
        except (ValueError,TypeError):raise HTTPException(422,detail={'code':'credential_invalid'}) from None
    def error(exc):
        code=str(exc)
        return HTTPException(404 if code=='credential_not_found' else 409 if 'conflict' in code else 503 if 'crypto' in code else 422,detail={'code':code})
    @router.get('/manage/v1/credentials',dependencies=[Depends(require_management)])
    async def metadata():return {'items':await asyncio.to_thread(vault.metadata),'values_returned':False}
    @router.post('/manage/v1/credentials',dependencies=[Depends(require_management)])
    async def add(request:Request):
        b=await parse(request)
        try:return await asyncio.to_thread(vault.put,b.get('label'),b.get('value'),None,b.get('expected_revision',0))
        except VaultError as exc:raise error(exc) from None
    @router.put('/manage/v1/credentials/{ref}',dependencies=[Depends(require_management)])
    async def replace(ref:str,request:Request):
        b=await parse(request)
        try:return await asyncio.to_thread(vault.put,b.get('label'),b.get('value'),ref,b.get('expected_revision',-1))
        except VaultError as exc:raise error(exc) from None
    @router.delete('/manage/v1/credentials/{ref}',dependencies=[Depends(require_management)])
    async def delete(ref:str,expected_revision:int=Query(ge=1)):
        desired=await asyncio.to_thread(store.snapshot)
        applied=await asyncio.to_thread(store.last_applied)
        def references(value):
            if isinstance(value,dict):return value.get('credential_ref')==ref or any(references(v) for v in value.values())
            if isinstance(value,list):return any(references(v) for v in value)
            return False
        if references(desired.model_dump()) or (applied and references(applied.model_dump())):
            raise HTTPException(409,detail={'code':'credential_in_use'})
        try:await asyncio.to_thread(vault.delete,ref,expected_revision)
        except VaultError as exc:raise error(exc) from None
        return {'deleted':True,'ref':ref}
    @router.get('/manage/internal/credentials/{ref}',dependencies=[Depends(require_internal)])
    async def resolve(ref:str):
        try:return {'value':await asyncio.to_thread(vault.resolve,ref)}
        except VaultError:raise HTTPException(404,detail={'code':'credential_unavailable'}) from None
    return router
