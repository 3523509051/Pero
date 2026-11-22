"""Isolated, one-invocation Linux container worker. No host network or secrets."""
from __future__ import annotations
import asyncio,base64,json,re,uuid,tempfile,hashlib,shutil,os
from pathlib import Path
from pero.plugins.errors import PluginError
from pero.plugins.network import PublicNetwork


class DockerWorkerClient:
    def __init__(self,*,package,data_dir,image,timeout_seconds,quota_mb=32,network_hosts=(),dependencies=()):
        self.package=Path(package).resolve();self.data_dir=Path(data_dir).resolve()
        self.image=image;self.timeout=float(timeout_seconds);self.quota_mb=int(quota_mb)
        self.hosts=tuple(network_hosts);self.dependencies=tuple(dependencies)
        self._lock=asyncio.Lock();self.healthy=False;self._active=None;self._closed=False

    async def start(self):
        if not re.fullmatch(r'sha256:[0-9a-f]{64}',self.image or ''):
            raise PluginError('remote_isolation_required','缺少固定容器镜像')
        # The runtime image is host-built, not supplied by the plugin.
        from pero.plugins.env_manager import APPROVED
        if any(dep not in APPROVED for dep in self.dependencies):
            raise PluginError('dependency_not_approved','容器不支持该依赖版本')
        try:
            p=await asyncio.create_subprocess_exec('docker','image','inspect',self.image,stdout=asyncio.subprocess.DEVNULL,stderr=asyncio.subprocess.DEVNULL)
            if await asyncio.wait_for(p.wait(),timeout=8)!=0:raise PluginError('worker_image_unavailable','容器镜像不存在')
        except (OSError,asyncio.TimeoutError):raise PluginError('worker_image_unavailable','容器运行环境不可用') from None
        self.data_dir.mkdir(parents=True,exist_ok=True);self.healthy=True

    async def _remove(self,name):
        try:
            p=await asyncio.create_subprocess_exec('docker','rm','-f',name,stdout=asyncio.subprocess.DEVNULL,stderr=asyncio.subprocess.DEVNULL)
            await asyncio.wait_for(p.wait(),timeout=12)
        except (OSError,asyncio.TimeoutError):pass

    async def _exchange(self,p,request,allow_network):
        rid=request['request_id'];p.stdin.write((json.dumps(request,ensure_ascii=False)+'\n').encode());await p.stdin.drain()
        while True:
            line=await p.stdout.readline()
            if not line:raise PluginError('worker_exited','容器未返回结果')
            try:msg=json.loads(line)
            except ValueError:raise PluginError('worker_invalid_response','容器响应异常') from None
            if not isinstance(msg,dict) or msg.get('request_id')!=rid:raise PluginError('worker_response_mismatch','响应与请求不一致')
            if msg.get('op')!='network_read':return msg
            broker_id=msg.get('broker_id')
            limit=msg.get('max_bytes',2*1024*1024)
            try:
                if not allow_network:raise PluginError('network_origin_denied','没有可用的公开网络授权')
                data=await PublicNetwork(self.hosts).read(msg.get('url'),max_bytes=limit,timeout=15,redirect_metadata=msg.get('redirect_metadata') is True)
                reply={'ok':True,'content_type':data['content_type'],'url':data['url'],'content':base64.b64encode(data['bytes']).decode(),'status_code':data.get('status_code',200),'location':data.get('location')}
            except PluginError as exc:reply={'ok':False,'error':exc.public()}
            p.stdin.write((json.dumps({'broker_id':broker_id,**reply})+'\n').encode());await p.stdin.drain()

    def _commit_storage(self,data_dir,files):
        if not isinstance(files,list) or len(files)>512:raise PluginError('storage_invalid','缓存快照不合法')
        total=0;seen=set();root=data_dir.resolve()
        with tempfile.TemporaryDirectory(prefix='sdk-state-',dir=root.parent) as td:
            stage=Path(td)
            for item in files:
                if not isinstance(item,dict):raise PluginError('storage_invalid','缓存快照不合法')
                rel=item.get('relative')
                if not isinstance(rel,str) or len(rel)>180 or any(not re.fullmatch(r'[A-Za-z0-9_.-]{1,100}',part) or part in ('.','..') for part in rel.split('/')) or rel.lower() in seen:
                    raise PluginError('storage_invalid','缓存文件名不合法')
                seen.add(rel.lower());raw=base64.b64decode(item.get('content',''),validate=True);total+=len(raw)
                if total>self.quota_mb*1024*1024 or hashlib.sha256(raw).hexdigest()!=item.get('sha256'):
                    raise PluginError('storage_invalid','缓存大小或摘要不合法')
                target=stage/rel;target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(raw)
            # Commit only validated data to the exact scope directory. Keep a
            # recoverable old snapshot until the new directory is in place.
            old=root.with_name(root.name+'.previous-'+uuid.uuid4().hex)
            if old.parent!=root.parent:raise PluginError('storage_invalid','缓存目录异常')
            os.replace(root,old)
            try:
                os.replace(stage,root)
            except BaseException:
                os.replace(old,root);raise
            if old.parent==root.parent and old.name.startswith(root.name+'.previous-'):
                shutil.rmtree(old)

    async def _invoke(self,payload,data_dir,allow_network=False,timeout_override=None):
        if not self.healthy or self._closed:raise PluginError('worker_unavailable','容器未就绪')
        async with self._lock:
            name='pero-plugin-'+uuid.uuid4().hex;data_dir.mkdir(parents=True,exist_ok=True)
            # Explicit read-only package, writable scope ONLY, no engine socket or host network.
            args=['docker','run','--rm','-i','--pull','never','--name',name,'--network','none','--read-only','--cap-drop','ALL','--security-opt','no-new-privileges','--pids-limit','64','--memory','512m','--cpus','1.5','--user','65532:65532','--tmpfs','/tmp:rw,size=32m,mode=1777','--mount',f'type=bind,source={self.package},target=/plugin,readonly','--mount',f'type=bind,source={data_dir.resolve()},target=/sdk-seed,readonly','--tmpfs',f'/sdk-data:rw,size={self.quota_mb}m,mode=0700,uid=65532,gid=65532','--workdir','/sdk-data',self.image,'python','-m','pero.plugins.worker','--package','/plugin','--data-dir','/sdk-data','--isolated-scope','--seed-dir','/sdk-seed','--quota-mb',str(self.quota_mb)]
            p=None;self._active=name
            try:
                p=await asyncio.create_subprocess_exec(*args,stdin=asyncio.subprocess.PIPE,stdout=asyncio.subprocess.PIPE,stderr=asyncio.subprocess.DEVNULL,limit=max(16*1024*1024,self.quota_mb*1024*1024*2+1024*1024))
                msg=await asyncio.wait_for(self._exchange(p,{**payload,'request_id':uuid.uuid4().hex},allow_network),timeout=float(timeout_override or self.timeout))
                if not msg.get('ok'):
                    detail=msg.get('error') or {};raise PluginError(str(detail.get('code') or 'worker_rejected')[:64],'容器拒绝执行')
                files=msg.pop('storage_snapshot',None)
                if files is not None:
                    p.stdin.close();await self._remove(name)
                    try:await asyncio.wait_for(p.wait(),timeout=3)
                    except asyncio.TimeoutError:p.kill();await p.wait()
                    await asyncio.to_thread(self._commit_storage,data_dir,files)
                return msg
            except (OSError,asyncio.TimeoutError,ValueError):raise PluginError('worker_unavailable','隔离容器启动或执行失败') from None
            finally:
                if p is not None and p.stdin is not None:p.stdin.close()
                await self._remove(name)
                if p is not None:
                    try:await asyncio.wait_for(p.wait(),timeout=3)
                    except asyncio.TimeoutError:p.kill();await p.wait()
                self._active=None

    async def init(self):
        with tempfile.TemporaryDirectory(prefix='pero-plugin-init-',dir=self.data_dir) as td:
            result=await self._invoke({'op':'init'},Path(td))
        return result.get('tools') or []

    async def call(self,tool,arguments,config,context,network_hosts=(),timeout_override=None):
        ref=context.get('scope_ref') or ''
        if not re.fullmatch(r'scope-[0-9a-f]{24}',ref):raise PluginError('scope_denied','会话范围无效')
        result=await self._invoke({'op':'call','tool':tool,'arguments':arguments,'config':config,'context':context,'broker_network':True,'network_hosts':list(self.hosts)},self.data_dir/'scopes'/ref,bool(self.hosts),timeout_override=timeout_override)
        return result

    async def close(self):
        self._closed=True;self.healthy=False
        if self._active:await self._remove(self._active)
