"""Server-only credential resolution; values are never config or history fields."""
import os
from urllib.parse import urlsplit
import httpx


class CredentialUnavailable(Exception):
    pass


class CredentialResolver:
    def __init__(self,configuration,provider,model,transport=None):
        self.configuration,self.provider=configuration,provider
        self._model=model
        base=os.environ.get('PERO_MANAGER_URL','http://127.0.0.1:8766').rstrip('/')
        p=urlsplit(base)
        if p.scheme!='http' or p.hostname not in {'127.0.0.1','localhost','::1'} or p.username or p.password or p.query or p.fragment or p.path not in {'','/'}:
            raise ValueError('manager_url_must_be_loopback')
        self.client=httpx.AsyncClient(base_url=base,transport=transport,trust_env=False,follow_redirects=False,timeout=5)
    def snapshot(self):
        from pero.tools.context import current_tool_context
        context=current_tool_context()
        return getattr(context,'configuration',None) or self.configuration.snapshot('*')
    def profile(self,request=None,profile_id=None):
        snap=self.snapshot()
        profiles,policy=snap.model_configuration()
        tool=snap.tool(request.tool_name) if request is not None and request.tool_name else {}
        name=profile_id or (tool.get('model_profile') if request is not None and request.tool_name else None) or policy.get('default_profile','default')
        if name!='default' and name not in profiles:raise CredentialUnavailable('model_profile_missing')
        profile={'model':self.provider_model,'base_url':self.provider.base_url,
                 'credential_ref':None,'reply_thinking':False,**profiles.get(name,{})}
        thinking=False
        purpose=getattr(getattr(request,'purpose',None),'value',None)
        if purpose in {'main_reply','tool_continuation','tool_retry'}:
            thinking=profile.get('reply_thinking',False)
        elif purpose=='tool_analysis':
            mode=tool.get('thinking_mode') or 'inherit'
            thinking=(policy.get('tool_thinking_default',False) if mode=='inherit' else mode=='enabled')
        return name,profile,bool(thinking)
    @property
    def provider_model(self):return self._model
    async def resolve(self,ref):
        from pero.tools.context import current_tool_context
        context=current_tool_context()
        cache=getattr(context,'credential_cache',None)
        if cache is not None and ref in cache:return cache[ref]
        token=os.environ.get('PERO_MANAGEMENT_TOKEN','').strip()
        if not token:raise CredentialUnavailable('credential_manager_unconfigured')
        try:
            response=await self.client.get('/manage/internal/credentials/'+ref,headers={'Authorization':'Bearer '+token})
            if response.status_code!=200:raise CredentialUnavailable('credential_unavailable')
            value=response.json()['value']
            if not isinstance(value,str) or not value:raise ValueError()
        except (httpx.HTTPError,ValueError,KeyError):raise CredentialUnavailable('credential_unavailable') from None
        if cache is not None:cache[ref]=value
        return value
    async def for_environment(self,env,tool_name=None,target_url=None):
        snap=self.snapshot()
        ref=snap.tool(tool_name).get('credential_ref') if tool_name else None
        if ref:return await self.resolve(ref)
        _name,profile,_thinking=self.profile()
        def origin(url):
            p=urlsplit(url);return p.scheme,p.hostname,p.port
        if (env==self.provider.api_key_env and profile.get('credential_ref')
                and (target_url is None or origin(target_url)==origin(profile['base_url']))):
            return await self.resolve(profile['credential_ref'])
        return os.environ.get(env,'').strip()
    def configured(self,env,tool_name=None,target_url=None):
        if os.environ.get(env,'').strip():return True
        if tool_name and self.snapshot().tool(tool_name).get('credential_ref'):return True
        try:
            profile=self.profile()[1]
            if env!=self.provider.api_key_env or not profile.get('credential_ref'):return False
            if target_url:
                p,q=urlsplit(target_url),urlsplit(profile['base_url'])
                if (p.scheme,p.hostname,p.port)!=(q.scheme,q.hostname,q.port):return False
            return True
        except CredentialUnavailable:return False
    async def close(self):await self.client.aclose()
