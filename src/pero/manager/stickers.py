"""Only authenticated manager clients can mutate scoped sticker assets."""
import os
from urllib.parse import quote
import httpx
from fastapi import APIRouter,HTTPException,Request
from fastapi.responses import JSONResponse
from pero.configuration.auth import TOKEN_ENV

def sticker_router(reader,resolve):
    router=APIRouter()
    @router.api_route('/manage/v1/scopes/{scope_id}/stickers/{operation}',methods=['POST','PATCH','DELETE'])
    async def mutate(scope_id:str,operation:str,request:Request,filename:str='sticker.png',auto_name:bool=True):
        scope=await resolve(scope_id,request)
        if (request.method=='POST' and operation!='upload') or (request.method!='POST' and not __import__('re').fullmatch(r'sa-[0-9a-f]{32}',operation)):
            raise HTTPException(404,detail={'code':'sticker_operation_not_found'})
        body=bytearray()
        async for chunk in request.stream():
            if len(body)+len(chunk)>100_000_000:raise HTTPException(413,detail={'code':'image_too_large'})
            body.extend(chunk)
        headers={'Authorization':'Bearer '+os.environ.get(TOKEN_ENV,'').strip(),'Content-Type':request.headers.get('content-type','application/octet-stream')}
        params={'conversation_id':scope.conversation_id}
        if operation=='upload':params.update(filename=filename,auto_name=str(auto_name).lower())
        try:
            response=await reader.client.request(request.method,'/api/v1/agents/'+quote(scope.current_agent_id,safe='')+'/stickers/'+operation,
                params=params,content=bytes(body),headers=headers,timeout=90)
        except httpx.HTTPError:raise HTTPException(503,detail={'code':'sticker_operation_unavailable'}) from None
        try:result=response.json()
        except ValueError:raise HTTPException(503,detail={'code':'sticker_invalid_response'}) from None
        return JSONResponse(result,status_code=response.status_code,headers={'Cache-Control':'no-store'})
    return router
