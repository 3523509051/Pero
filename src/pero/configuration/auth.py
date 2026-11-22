"""Local owner sessions for the manager; internal calls always require a bearer."""
import hashlib
import hmac
import os
import secrets
import time
from fastapi import APIRouter, Depends, HTTPException, Request, Response

TOKEN_ENV = 'PERO_MANAGEMENT_TOKEN'
COOKIE = 'pero_session'


def require_internal(request: Request):
    token = os.environ.get(TOKEN_ENV, '').strip()
    if not token:raise HTTPException(503,detail={'code':'management_not_configured'})
    if not hmac.compare_digest(request.headers.get('authorization','').encode(),('Bearer '+token).encode()):
        raise HTTPException(401,detail={'code':'management_unauthorized'})
    return None


class Sessions:
    def __init__(self):self.values={}
    def get(self, cookie):
        now=time.time()
        for key,value in list(self.values.items()):
            if value['expires_at']<=now:self.values.pop(key,None)
        token=os.environ.get(TOKEN_ENV,'').strip()
        entry=self.values.get(hashlib.sha256((cookie or '').encode()).hexdigest())
        if not token or not entry or entry['owner']!=hashlib.sha256(token.encode()).hexdigest():return None
        return entry
    def create(self):
        if len(self.values)>=64:self.values.pop(next(iter(self.values)))
        cookie=secrets.token_urlsafe(32)
        entry={'csrf_token':secrets.token_urlsafe(32),'expires_at':time.time()+8*3600,
               'owner':hashlib.sha256(os.environ[TOKEN_ENV].strip().encode()).hexdigest()}
        self.values[hashlib.sha256(cookie.encode()).hexdigest()]=entry
        return cookie,entry


def require_management(request: Request):
    if not os.environ.get(TOKEN_ENV,'').strip():raise HTTPException(503,detail={'code':'management_not_configured'})
    if request.headers.get('authorization'):
        return require_internal(request)
    realm=getattr(request.app.state,'auth_sessions',None)
    entry=realm.get(request.cookies.get(COOKIE)) if realm is not None else None
    if entry is None:raise HTTPException(401,detail={'code':'management_unauthorized'})
    if request.method not in {'GET','HEAD','OPTIONS'} and not hmac.compare_digest(
            request.headers.get('x-pero-csrf','').encode(),entry['csrf_token'].encode()):
        raise HTTPException(403,detail={'code':'csrf_required'})
    return None


def auth_router(realm):
    router=APIRouter(prefix='/manage/v1/auth')
    @router.post('/session')
    async def login(request:Request,response:Response):
        origin=request.headers.get('origin')
        if origin and origin.rstrip('/')!=str(request.base_url).rstrip('/'):
            raise HTTPException(403,detail={'code':'origin_not_allowed'})
        token=os.environ.get(TOKEN_ENV,'').strip()
        if not token:raise HTTPException(503,detail={'code':'management_not_configured'})
        try:body=await request.json();supplied=body['token']
        except (ValueError,KeyError,TypeError):raise HTTPException(422,detail={'code':'invalid_login'}) from None
        if not isinstance(supplied,str) or not hmac.compare_digest(supplied.encode(),token.encode()):
            raise HTTPException(401,detail={'code':'management_unauthorized'})
        cookie,entry=realm.create()
        response.set_cookie(COOKIE,cookie,httponly=True,samesite='strict',secure=request.url.scheme=='https',max_age=8*3600,path='/manage')
        return {'authenticated':True,'csrf_token':entry['csrf_token'],'expires_at':entry['expires_at']}
    @router.get('/session',dependencies=[Depends(require_management)])
    async def session(request:Request):
        entry=realm.get(request.cookies.get(COOKIE))
        return {'authenticated':True,**({k:entry[k] for k in ('csrf_token','expires_at')} if entry else {'mode':'bearer'})}
    @router.delete('/session',dependencies=[Depends(require_management)])
    async def logout(request:Request,response:Response):
        realm.values.pop(hashlib.sha256(request.cookies.get(COOKIE,'').encode()).hexdigest(),None)
        response.delete_cookie(COOKIE,path='/manage')
        return {'authenticated':False}
    return router
