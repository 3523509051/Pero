"""Fixed Windows task bindings and observable operations, never arbitrary UI commands."""
import asyncio
from dataclasses import dataclass
import json
from pathlib import Path
import subprocess
import sys
import time
import uuid


class ServiceError(Exception):pass


@dataclass(frozen=True)
class Binding:
    start_task:str
    stop_task:str
    process_executable:str
    command_marker:str
    start_timeout_seconds:float=90
    stop_timeout_seconds:float=30
    def __post_init__(self):
        if not all(isinstance(v,str) and v.strip() and len(v)<1024 and '\0' not in v for v in (self.start_task,self.stop_task,self.process_executable,self.command_marker)):
            raise ValueError('invalid_service_binding')
        if not Path(self.process_executable).is_absolute():raise ValueError('process_executable_must_be_absolute')
        import math
        if any(type(v) not in {int,float} or not math.isfinite(v) or not 1<=v<=300 for v in (self.start_timeout_seconds,self.stop_timeout_seconds)):raise ValueError('invalid_service_timeout')


def load_bindings(path):
    if path is None:return {}
    raw=json.loads(Path(path).read_text(encoding='utf-8-sig'))
    if not isinstance(raw,dict) or set(raw)-{'pero','bridge'}:raise ValueError('invalid_service_manifest')
    return {name:Binding(**value) for name,value in raw.items()}


class WindowsTasks:
    @staticmethod
    async def powershell(script):
        if sys.platform!='win32':raise ServiceError('windows_adapter_unavailable')
        def execute():
            r=subprocess.run(['powershell.exe','-NoProfile','-NonInteractive','-Command',script],
                capture_output=True,timeout=15,creationflags=0x08000000)
            if r.returncode:raise ServiceError('service_task_or_probe_failed')
            return r.stdout.decode('utf-8-sig',errors='replace').strip()
        return await asyncio.to_thread(execute)
    @staticmethod
    def literal(value):return "'"+value.replace("'","''")+"'"
    async def trigger(self,task):
        await self.powershell('Start-ScheduledTask -TaskName '+self.literal(task)+' -ErrorAction Stop')
    async def running(self,binding):
        script=("$ErrorActionPreference='Stop'; $items=@(Get-CimInstance Win32_Process | Where-Object { $_.ExecutablePath -eq "
                +self.literal(binding.process_executable)+" -and $_.CommandLine -and $_.CommandLine.Contains("
                +self.literal(binding.command_marker)+") }); [Console]::Write($items.Count)")
        return int(await self.powershell(script))>0


class Operations:
    def __init__(self,reader,bindings,path,adapter=None):
        self.reader,self.bindings,self.path=reader,bindings,Path(path)
        self.adapter=adapter or WindowsTasks()
        self.items={};self.task=None
        if self.path.is_file():
            values=json.loads(self.path.read_text(encoding='utf-8'))
            for item in values:
                if item['state'] in {'queued','running'}:item.update(state='unknown',reason='manager_restarted')
                self.items[item['operation_id']]=item
    def persist(self):
        self.path.parent.mkdir(parents=True,exist_ok=True)
        temp=self.path.with_suffix('.tmp')
        temp.write_text(json.dumps(list(self.items.values())[-100:],ensure_ascii=False),encoding='utf-8')
        temp.replace(self.path)
    def submit(self,target,action,force=False,request_id=None):
        if target not in {'pero','bridge','stack'} or action not in {'start','stop','restart'} or type(force) is not bool:
            raise ServiceError('invalid_operation')
        import re
        if request_id is not None and (not isinstance(request_id,str) or not re.fullmatch(r'[A-Za-z0-9_-]{8,128}',request_id)):raise ServiceError('invalid_operation_request_id')
        for prior in self.items.values():
            if request_id is not None and prior.get('request_id')==request_id:
                if (prior['target'],prior['action'],prior['force'])!=(target,action,force):raise ServiceError('operation_request_id_conflict')
                return prior
        needed={'pero','bridge'} if target=='stack' else {target}
        if needed-set(self.bindings):raise ServiceError('service_binding_missing')
        if self.task is not None and not self.task.done():
            current=list(self.items.values())[-1]
            if (current['target'],current['action'],current['force'])==(target,action,force):return current
            raise ServiceError('operation_in_progress')
        item={'operation_id':'op-'+uuid.uuid4().hex,'target':target,'action':action,'force':force,
              'state':'queued','stage':'queued','created_at':time.time(),'request_id':request_id}
        self.items[item['operation_id']]=item
        while len(self.items)>100:self.items.pop(next(iter(self.items)))
        self.persist();self.task=asyncio.create_task(self.run(item));return item
    async def observe(self,name):
        running=await self.adapter.running(self.bindings[name])
        if name=='pero':
            try:
                h=await self.reader.read('/health')
                ready=h.get('service')=='pero' and h.get('ok') is True
                return {'running':running,'ready':ready,'pid':h.get('pid')}
            except Exception:return {'running':running,'ready':False,'pid':None}
        try:
            channels=(await self.reader.read('/api/v1/channels')).get('channels',[])
            ready=any(c.get('connected') is True for c in channels)
        except Exception:ready=False
        return {'running':running,'ready':ready}
    async def drain(self,item):
        self.step(item,'waiting_for_tasks')
        state=await self.reader.management('POST','/drain',{'enabled':True})
        deadline=time.monotonic()+60
        while state.get('busy'):
            if item['force']:return
            if time.monotonic()>=deadline:raise ServiceError('drain_timeout')
            await asyncio.sleep(.3)
            state=await self.reader.management('GET','/drain')
    def step(self,item,stage):item.update(state='running',stage=stage);self.persist()
    async def start(self,name,item):
        state=await self.observe(name)
        if state['ready'] and state['running']:return
        require_connection=True
        if name=='bridge':
            try:ready=(await self.reader.read('/health')).get('ok') is True
            except Exception:ready=False
            if not ready:
                if item['target']=='stack':raise ServiceError('pero_not_ready')
                require_connection=False
                if state['running']:return
        self.step(item,'starting_'+name)
        if not state['running']:await self.adapter.trigger(self.bindings[name].start_task)
        deadline=time.monotonic()+self.bindings[name].start_timeout_seconds
        while time.monotonic()<deadline:
            state=await self.observe(name)
            if state['running'] and (state['ready'] or not require_connection):return
            await asyncio.sleep(.3)
        raise ServiceError(name+'_start_unconfirmed')
    async def stop(self,name,item):
        if not (await self.observe(name))['running']:return
        self.step(item,'stopping_'+name)
        await self.adapter.trigger(self.bindings[name].stop_task)
        deadline=time.monotonic()+self.bindings[name].stop_timeout_seconds
        while time.monotonic()<deadline:
            if not (await self.observe(name))['running']:return
            await asyncio.sleep(.3)
        raise ServiceError(name+'_stop_unconfirmed')
    async def run(self,item):
        completion={}
        try:
            action,target=item['action'],item['target']
            if action in {'stop','restart'}:
                try:await self.drain(item)
                except Exception:
                    if (await self.reader.status()).get('pero',{}).get('reachable'):raise
                if target in {'stack','bridge'}:await self.stop('bridge',item)
                if target in {'stack','pero'}:await self.stop('pero',item)
            if action in {'start','restart'}:
                if target in {'stack','pero'}:await self.start('pero',item)
                if target in {'stack','bridge'}:await self.start('bridge',item)
            targets=('pero','bridge') if target=='stack' else (target,)
            item['result']={name:await self.observe(name) for name in targets}
            completion={'state':'succeeded','stage':'complete'}
        except asyncio.CancelledError:
            completion={'state':'unknown','reason':'manager_shutdown'}
            raise
        except Exception as exc:
            reason=str(exc) if isinstance(exc,ServiceError) else type(exc).__name__
            completion={'state':'unknown' if reason.endswith('_unconfirmed') else 'failed','reason':reason}
        finally:
            # A failed task or bridge-only action must not leave the live core drained.
            try:await self.reader.management('POST','/drain',{'enabled':False})
            except Exception:pass
            item.update(completion)
            item['finished_at']=time.time();self.persist()
    async def close(self):
        if self.task and not self.task.done():
            self.task.cancel()
            try:await self.task
            except asyncio.CancelledError:pass
