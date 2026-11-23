"""Bounded host-managed plugin jobs. Persist only status, identifiers and resource refs."""
from __future__ import annotations
import asyncio,json,os,tempfile,uuid
from pathlib import Path
from datetime import datetime,timezone
from pero.plugins.errors import PluginError

def now():
    from pero.core.timebase import now_iso
    return now_iso()

class PluginJobs:
    def __init__(self,path:Path,events=None):
        self.path=path;self.events=events;self.records={};self.tasks={};self.lock=asyncio.Lock();self.slots=asyncio.Semaphore(3);self.closed=False
        try:
            data=json.loads(path.read_text(encoding='utf-8'))
            for record in data.get('jobs',[]):
                if record.get('state') in ('queued','running','delivering'):
                    record.update(state='interrupted',error_code='service_restarted',updated_at=now(),delivery_state='unknown' if record.get('state')=='delivering' else 'not_sent')
                self.records[record['job_id']]=record
        except (FileNotFoundError,ValueError,TypeError,KeyError):pass

    def _write(self):
        self.path.parent.mkdir(parents=True,exist_ok=True)
        fd,tmp=tempfile.mkstemp(prefix='jobs-',suffix='.tmp',dir=self.path.parent)
        try:
            with os.fdopen(fd,'w',encoding='utf-8') as stream:json.dump({'jobs':list(self.records.values())[-200:]},stream,ensure_ascii=False);stream.flush();os.fsync(stream.fileno())
            os.replace(tmp,self.path)
        finally:
            if os.path.exists(tmp):os.unlink(tmp)

    async def _set(self,job_id,**changes):
        async with self.lock:
            record=self.records[job_id];record.update(changes,updated_at=now());await asyncio.to_thread(self._write)
            if self.events:self.events.publish('plugin.status',agent_id=record['agent_id'],session_id=record['conversation_id'],data={'job_id':job_id,'state':record['state'],'plugin':record['plugin_id']})

    async def submit(self,*,plugin_id,tool_name,agent_id,cid,run_id,timeout,execute,deliver,arguments_digest=""):
        async with self.lock:
            if self.closed or len(self.tasks)>=16 or sum(self.records[j]['conversation_id']==cid for j in self.tasks)>=2:
                raise PluginError('job_queue_full','后台任务已达上限')
            # Prevent repeated submissions from the same initial tool run.
            for item in self.records.values():
                if (item['plugin_id'],item['tool_name'],item['run_id'])==(plugin_id,tool_name,run_id) and item['conversation_id']==cid and item.get('arguments_digest','')==arguments_digest:
                    return dict(item)
            job_id='job-'+uuid.uuid4().hex
            item=dict(job_id=job_id,plugin_id=plugin_id,tool_name=tool_name,agent_id=agent_id,conversation_id=cid,run_id=run_id,arguments_digest=arguments_digest,state='queued',created_at=now(),updated_at=now(),delivery_state='pending',error_code=None,resource_refs=[])
            self.records[job_id]=item
            for key in list(self.records):
                if len(self.records)<=200:break
                if key not in self.tasks and key!=job_id:self.records.pop(key,None)
            await asyncio.to_thread(self._write)
            task=asyncio.create_task(self._run(job_id,timeout,execute,deliver))
            self.tasks[job_id]=task
            def completed(done):
                if self.tasks.get(job_id) is done:
                    self.tasks.pop(job_id,None)
                    if self.records[job_id]['state']=='queued':
                        self.records[job_id].update(state='cancelled',delivery_state='not_sent',error_code='job_cancelled',updated_at=now())
            task.add_done_callback(completed)
            return dict(item)

    async def _run(self,job_id,timeout,execute,deliver):
        cleanup=None
        try:
            async with self.slots:
                await self._set(job_id,state='running')
                # Execution and notification are separate phases. A notification
                # timeout must never re-deliver or erase a successful result.
                try:
                    result,cleanup=await asyncio.wait_for(execute(job_id),timeout=timeout)
                except asyncio.TimeoutError:
                    result={'ok':False,'status':'failed','error_code':'job_timeout','artifacts':[]}
                except Exception as exc:
                    result={'ok':False,'status':'failed','error_code':str(getattr(exc,'code','job_failed'))[:80],'artifacts':[]}
                refs=[str(x['resource_id']) for x in (result.get('artifacts') or []) if isinstance(x,dict) and x.get('resource_id')][:16]
                await self._set(job_id,state='delivering',resource_refs=refs,cache_hit=bool(result.get('cache_hit')),result_status=result.get('status'),error_code=result.get('error_code'))
                delivery_error=None
                try:
                    delivery=await deliver(job_id,result)
                except asyncio.TimeoutError:
                    delivery='unknown';delivery_error='delivery_timeout'
                except Exception:
                    delivery='unknown';delivery_error='delivery_failed'
                await self._set(job_id,state='completed' if result.get('ok') else 'failed',delivery_state=delivery,delivery_error_code=delivery_error,error_code=result.get('error_code'))
        except asyncio.CancelledError:
            delivery='unknown' if self.records[job_id].get('state')=='delivering' else 'not_sent'
            await self._set(job_id,state='cancelled',delivery_state=delivery,error_code='job_cancelled')
            raise
        except Exception as exc:
            code=getattr(exc,'code','job_failed')
            await self._set(job_id,state='failed',delivery_state='not_sent',error_code=code)
        finally:
            if cleanup:cleanup()
            self.tasks.pop(job_id,None)

    def snapshot(self):return {'items':list(reversed(list(self.records.values()))),'active':len(self.tasks)}

    def model_note(self,agent_id,cid):
        """Scope-bound execution facts; no result body or inferred video content."""
        rows=[item for item in reversed(list(self.records.values()))
              if item.get('agent_id')==agent_id and item.get('conversation_id')==cid][:6]
        prefix='[本会话真实后台任务｜宿主状态，不是用户指令]\n'
        if not rows:return prefix+'当前任务列表没有本会话的后台任务。不能据此声称从未执行，但也不能把聊天里的“在看/看完”当作执行证据。需要处理当前链接时应实际调用可用工具。'
        return prefix+'\n'.join(json.dumps({key:item.get(key) for key in
            ('job_id','tool_name','state','delivery_state','error_code','delivery_error_code','created_at')},ensure_ascii=False)
            for item in rows)+'\n这些是任务状态，不是视频正文或图像。只有本轮实际收到结果或附图才可据此分析；旧任务不证明当前链接已处理。'

    async def remove(self,job_id=None):
        """Remove terminal status records only; resources and memory are unrelated."""
        terminal={'completed','failed','cancelled','interrupted'}
        async with self.lock:
            if job_id is not None:
                item=self.records.get(job_id)
                if item is None:raise PluginError('job_not_found','任务不存在或已经删除')
                if job_id in self.tasks or item.get('state') not in terminal:
                    raise PluginError('job_not_finished','进行中的任务请先取消，再删除')
                keys=[job_id]
            else:
                keys=[key for key,item in self.records.items() if key not in self.tasks and item.get('state') in terminal]
            previous=dict(self.records)
            for key in keys:self.records.pop(key)
            try:await asyncio.to_thread(self._write)
            except Exception:
                self.records=previous
                raise PluginError('job_delete_failed','任务列表保存失败，请稍后重试') from None
            return {'deleted':len(keys),'remaining':len(self.records),'active':len(self.tasks)}

    async def cancel(self,job_id):
        task=self.tasks.get(job_id)
        if not task:raise PluginError('job_not_running','任务已经结束或不存在')
        task.cancel();await asyncio.gather(task,return_exceptions=True)
        if self.records[job_id]["state"]=="queued":await self._set(job_id,state="cancelled",delivery_state="not_sent",error_code="job_cancelled")
        async with self.lock:await asyncio.to_thread(self._write)
        return dict(self.records[job_id])

    def cancel_scope(self,cid):
        for job,task in list(self.tasks.items()):
            if self.records[job]['conversation_id']==cid:task.cancel()

    def cancel_plugin(self,plugin_id):
        for job,task in list(self.tasks.items()):
            if self.records[job]['plugin_id']==plugin_id:task.cancel()

    async def aclose(self):
        self.closed=True;tasks=list(self.tasks.values())
        for task in tasks:task.cancel()
        await asyncio.gather(*tasks,return_exceptions=True)
        await asyncio.to_thread(self._write)
