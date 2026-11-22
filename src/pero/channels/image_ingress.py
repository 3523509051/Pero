"""Bounded ordered intake: persist actual inline bytes outside the WS receive loop."""
from __future__ import annotations

import asyncio
import base64
import hashlib
import logging
import time
from dataclasses import replace

from pero.core.contracts import AttachmentRef
from pero.core.message_content import is_image_only

logger=logging.getLogger(__name__)


class ImageIngress:
    def __init__(self,settings,catalog,routes,allowed,on_event):
        self.settings,self.catalog=settings,catalog
        self.routes={(r.account,r.conversation_type,r.target):r.agent_id for r in routes}
        self.allowed=set(allowed)
        self.on_event=on_event
        self.queue=asyncio.Queue(maxsize=settings.ingress_max_items)
        self.bytes=0
        self.task=None
        self.counters={"stored":0,"rejected":0,"queue_full":0,"failed":0,"image_only_cached":0}

    async def start(self):
        if self.settings.enabled and self.task is None:
            self.task=asyncio.create_task(self._run())

    async def stop(self):
        task,self.task=self.task,None
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        while not self.queue.empty():
            self.queue.get_nowait()
        self.bytes=0

    def enqueue(self,event,payload):
        if not self.settings.enabled or self.task is None:
            self.on_event(event)
            return
        msg=event.message
        if msg is None:
            return
        private = msg.conversation_type == 'private'
        access = getattr(self, '_private_access', None)
        grant = access.grant_for(msg) if private and access else None
        agent = grant['agent_id'] if grant else self.routes.get((msg.account_id,msg.conversation_type,msg.target_ref))
        if (private and not grant) or not agent or (not private and msg.conversation_id not in self.allowed):
            self.counters["rejected"]+=1
            return
        parts=payload.get("message") if isinstance(payload,dict) else None
        sources=[]
        if self.settings.accept_inline and isinstance(parts,list):
            for part in parts:
                if isinstance(part,dict) and part.get("type")=="image":
                    data=part.get("data")
                    value=data.get("file") if isinstance(data,dict) else None
                    if isinstance(value,str) and (value.startswith("base64://") or value.startswith("data:image/")):
                        sources.append((value, str(data.get("name") or "")[:180]))
        size=sum(len(value) for value,_name in sources)
        if len(sources)>self.settings.max_per_turn or self.bytes+size>self.settings.ingress_max_bytes:
            self.counters["queue_full"]+=1
            return
        try:
            self.queue.put_nowait((event,agent,sources,size))
            self.bytes+=size
        except asyncio.QueueFull:
            self.counters["queue_full"]+=1

    def _decode(self,value):
        encoded=value[len("base64://"):] if value.startswith("base64://") else value.split(",",1)[1]
        if len(encoded)>((self.settings.max_bytes+2)//3)*4:
            raise ValueError("image_too_large")
        content=base64.b64decode(encoded,validate=True)
        if not content or len(content)>self.settings.max_bytes:
            raise ValueError("invalid_image_size")
        return content

    async def _run(self):
        from pero.resources.catalog import image_mime
        suffix={"image/jpeg":"jpg","image/png":"png","image/gif":"gif","image/webp":"webp"}
        while True:
            event,agent,sources,size=await self.queue.get()
            try:
                msg=event.message
                refs=[]
                if msg.conversation_type == 'private' and not self._private_access.authorizes(msg):
                    continue
                for index,(value,name) in enumerate(sources):
                    data=await asyncio.to_thread(self._decode,value)
                    mime=image_mime(data)
                    name=name or f"image-{hashlib.sha256(data).hexdigest()[:16]}.{suffix[mime]}"
                    if msg.conversation_type == 'private' and not self._private_access.authorizes(msg):
                        break
                    item=await self.catalog.ingest(agent_id=agent,conversation_id=msg.conversation_id,
                        name=name,data=data,event_id=f"{msg.event_id}:image:{index}")
                    await self.catalog.register_image_source(agent_id=agent,conversation_id=msg.conversation_id,
                        resource_id=item["resource_id"],event_id=msg.event_id,sender_ref=msg.sender_ref,
                        observed_at=time.time())
                    refs.append(AttachmentRef(handle=item["resource_id"],kind="image",name=item["name"],
                        size=item["size"],sha256=item["sha256"]))
                    self.counters["stored"]+=1
                if refs:
                    # Preserve unsupported attachment references, replacing only actual inline images.
                    others=tuple(r for r in msg.attachments if r.kind!="image")
                    event=replace(event,message=replace(msg,attachments=others+tuple(refs)))
                if is_image_only(event.message):
                    self.counters["image_only_cached"]+=1
                    if not (event.message.text or '').strip():
                        event=replace(event,message=replace(event.message,text='[图片]'))
                    # Policy rejects media-only as a reply trigger; the orchestrator
                    # still persists the silent incoming chat event.
                self.on_event(event)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.counters["failed"]+=1
                logger.warning("image_ingress_failed error_type=%s",type(exc).__name__)
                # Do not substitute an older picture after a failed original image intake.
                self.on_event(event)
            finally:
                self.bytes=max(0,self.bytes-size)

    def status(self):
        return {"enabled":self.settings.enabled,"accept_inline":self.settings.accept_inline,
            "queued":self.queue.qsize(),"queued_bytes":self.bytes,
            "running":self.task is not None and not self.task.done(),"counters":dict(self.counters)}
