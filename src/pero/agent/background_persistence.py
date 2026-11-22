"""Bounded background snapshots: atomic disk replacement off the event loop."""
import asyncio,json,logging
from pathlib import Path
from collections import deque
from dataclasses import asdict
from pero.agent.background import BackgroundEntry
log=logging.getLogger(__name__)
def attach(buffer,path):
    path=Path(path);buffer._persistent=True;buffer._idle_seconds=float('inf')
    if path.exists():
        data=json.loads(path.read_text(encoding='utf-8'))
        for group in data:
            buffer._entries[tuple(group['scope'])]=deque([BackgroundEntry(**v) for v in group['entries']][-buffer._max_turns:],maxlen=buffer._max_turns)
    state={'task':None,'version':0}
    async def flush():
        while True:
            version=state['version']
            data=[{'scope':list(k),'entries':[asdict(v) for v in q]} for k,q in buffer._entries.items()]
            text=json.dumps(data,ensure_ascii=False)
            def write():
                path.parent.mkdir(parents=True,exist_ok=True);temp=path.with_suffix('.pending');temp.write_text(text,encoding='utf-8');temp.replace(path)
            try:await asyncio.to_thread(write)
            except OSError:
                log.exception('background persistence failed');await asyncio.sleep(1);continue
            if version==state['version']:break
    def mark():
        state['version']+=1
        if state['task'] is None or state['task'].done():state['task']=asyncio.create_task(flush())
    for name in ('observe','absorb','discard'):
        if not hasattr(buffer,name):continue
        original=getattr(buffer,name)
        def wrapped(*args,_original=original,**kwargs):
            result=_original(*args,**kwargs);mark();return result
        setattr(buffer,name,wrapped)
    async def close():
        if state['task'] is not None:await state['task']
    buffer.flush_persistence=close
