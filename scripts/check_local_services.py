"""Offline regression checks for auto-discovered local service control."""
import asyncio,json,os,sys,tempfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from pero.manager.local_services import LocalServices,discover_local_services

async def main():
    with tempfile.TemporaryDirectory() as directory:
        root=Path(directory)
        (root/'core-state.json').write_text(json.dumps({'running':False}))
        settings=SimpleNamespace(state_dir=root,core_url='http://127.0.0.1:8765')
        with patch.dict(os.environ,{'PERO_LOCAL_CONTROL_DIR':str(root)}):
            bindings,adapter=discover_local_services(settings)
        assert 'pero' in bindings and isinstance(adapter,LocalServices)
        assert not await adapter.running(bindings['pero'])
        await adapter.trigger(bindings['pero'].start_task)
        request=json.loads((root/'core-request.json').read_text())
        assert set(request)=={'request_id','action'} and request['action']=='start'
        (root/'core-state.json').write_text(json.dumps({'running':True}))
        assert await adapter.running(bindings['pero'])
        await adapter.trigger(bindings['pero'].stop_task)
        assert json.loads((root/'core-request.json').read_text())['action']=='stop'
        with patch.dict(os.environ,{'PERO_LOCAL_CONTROL_DIR':''}):
            assert discover_local_services(settings)==({},None)
    print('PASS: auto-discovered core control, actual running state, fixed start/stop request contract, absent-supervisor rejection.')

if __name__=='__main__':asyncio.run(main())
