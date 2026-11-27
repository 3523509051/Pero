"""Automatically discovered controls for owned local launchers only."""
import asyncio
import json
import os
from pathlib import Path
import secrets
import subprocess
from types import SimpleNamespace
from .services import ServiceError


class LocalServices:
    def __init__(self, control):
        self.control=Path(control)

    async def running(self,binding):
        if binding.command_marker=='pero':
            try:return bool(json.loads((self.control/'core-state.json').read_text())['running'])
            except (OSError,ValueError,KeyError):raise ServiceError('local_core_state_unavailable') from None
        command=[binding.process_executable,str(Path(binding.start_task).parent/'scripts/bridge_launcher.py'),'status','--json']
        result=await asyncio.to_thread(subprocess.run,command,capture_output=True,timeout=10,creationflags=0x08000000)
        if result.returncode:raise ServiceError('bridge_probe_failed')
        try:return json.loads(result.stdout.decode('utf-8'))['running'] is True
        except (ValueError,KeyError):raise ServiceError('bridge_probe_invalid') from None

    async def trigger(self,task):
        if task in ('local-core:start','local-core:stop'):
            request={'request_id':secrets.token_hex(12),'action':task.split(':')[1]}
            temporary=self.control/('request-'+request['request_id']+'.tmp')
            try:
                temporary.write_text(json.dumps(request),encoding='utf-8')
                os.replace(temporary,self.control/'core-request.json')
            finally:temporary.unlink(missing_ok=True)
            return
        # task is resolved from trusted on-disk configuration, never request text.
        path=Path(task)
        if path.name not in ('start.cmd','stop.cmd'):raise ServiceError('invalid_local_command')
        python=path.parent/'bridge/.venv/Scripts/python.exe'
        command=[str(python),str(path.parent/'scripts/bridge_launcher.py'),path.stem]
        result=await asyncio.to_thread(subprocess.run,command,capture_output=True,timeout=110,creationflags=0x08000000)
        if result.returncode:raise ServiceError('bridge_control_failed')


def discover_local_services(settings):
    control=os.environ.get('PERO_LOCAL_CONTROL_DIR','')
    if not control:return {},None
    root=Path(control)
    if not root.is_absolute() or not (root/'core-state.json').is_file():return {},None
    bindings={'pero':SimpleNamespace(start_task='local-core:start',stop_task='local-core:stop',
               process_executable='',command_marker='pero',start_timeout_seconds=90,stop_timeout_seconds=150)}
    # Explicit bridge connection setup records an allowlisted checkout root.
    manifest=Path(settings.state_dir)/'local-bridge.json' if settings.state_dir else None
    try:
        bridge=None
        if manifest and manifest.is_file():
            data=json.loads(manifest.read_text(encoding='utf-8'))
            bridge=Path(data['root'])
            if set(data)!={'root'} or not bridge.is_absolute():raise ValueError()
        else:
            # Auto-discover the documented adjacent-checkout layout.
            candidate=Path(__file__).resolve().parents[4]/'webot'
            config=candidate/'bridge/config.json'
            if config.is_file():
                from urllib.parse import urlsplit
                bridge_settings=json.loads(config.read_text(encoding='utf-8-sig'))
                endpoint=urlsplit(bridge_settings.get('astrbot',{}).get('ws_url',''))
                if endpoint.hostname in ('127.0.0.1','localhost','::1'):
                    bridge=candidate
        if bridge is not None:
            python=bridge/'bridge/.venv/Scripts/python.exe'
            entry=bridge/'scripts/bridge_launcher.py'
            if python.is_file() and entry.is_file():
                bindings['bridge']=SimpleNamespace(start_task=str(bridge/'start.cmd'),stop_task=str(bridge/'stop.cmd'),
                    process_executable=str(python),command_marker='bridge',start_timeout_seconds=110,stop_timeout_seconds=45)
    except (OSError,ValueError,KeyError,TypeError):pass
    return bindings,LocalServices(root)
