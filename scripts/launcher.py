"""Windows source-checkout launcher for independent Pero Core and Manager services."""
from __future__ import annotations
import argparse
import json
import os
from pathlib import Path
import secrets
import signal
import socket
import sys
import time
import urllib.request
import webbrowser

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'src'))


class LauncherError(Exception):pass


def atomic_write(path,data):
    path.parent.mkdir(parents=True,exist_ok=True)
    temporary=path.with_name(path.name+'.'+secrets.token_hex(6)+'.tmp')
    try:
        with temporary.open('wb') as file:file.write(data);file.flush();os.fsync(file.fileno())
        for attempt in range(20):
            try:
                os.replace(temporary,path)
                break
            except PermissionError:
                if attempt==19:raise
                time.sleep(.02)
    finally:temporary.unlink(missing_ok=True)


class InstanceLock:
    def __init__(self,path):self.path=path;self.file=None
    def acquire(self):
        import msvcrt
        self.path.parent.mkdir(parents=True,exist_ok=True)
        self.file=self.path.open('a+b');self.file.seek(0,2)
        if self.file.tell()==0:self.file.write(b'0');self.file.flush()
        self.file.seek(0)
        try:msvcrt.locking(self.file.fileno(),msvcrt.LK_NBLCK,1);return True
        except OSError:self.file.close();self.file=None;return False
    def close(self):
        if self.file:
            import msvcrt
            self.file.seek(0);msvcrt.locking(self.file.fileno(),msvcrt.LK_UNLCK,1);self.file.close();self.file=None


def running(state_dir):
    path=ROOT/'data/launcher.lock'
    if not path.exists():return False
    lock=InstanceLock(path)
    if lock.acquire():lock.close();return False
    return True


def load_state(state_dir):
    try:return json.loads((state_dir/'session.json').read_text(encoding='utf-8'))
    except (OSError,ValueError):raise LauncherError('无法读取启动状态；请检查启动窗口。') from None


def management_token(state_dir,*,create=False):
    from pero.manager.local_access import management_token as owned_token, AccessCodeError
    try:
        return owned_token(create=create,legacy_paths=[state_dir/'management-token.dpapi'])
    except AccessCodeError as error:
        raise LauncherError(str(error)) from None


def request(url,path,*,token='',body=None):
    data=json.dumps(body).encode('utf-8') if body is not None else None
    headers={'Authorization':'Bearer '+token} if token else {}
    if data is not None:headers['Content-Type']='application/json'
    req=urllib.request.Request(url+path,data=data,headers=headers)
    with urllib.request.build_opener(urllib.request.ProxyHandler({})).open(req,timeout=2) as response:
        return json.load(response)


def available(host,port):
    try:
        for family,kind,proto,_,address in socket.getaddrinfo(host,port,type=socket.SOCK_STREAM):
            with socket.socket(family,kind,proto) as listener:
                if hasattr(socket,'SO_EXCLUSIVEADDRUSE'):listener.setsockopt(socket.SOL_SOCKET,socket.SO_EXCLUSIVEADDRUSE,1)
                listener.bind(address)
    except OSError:raise LauncherError(f'地址 {host}:{port} 不可用或已被占用；请使用其它端口，不会接管或终止已有进程。') from None


def check(args):
    for module in ('uvicorn','fastapi','httpx','websockets'):
        try:__import__(module)
        except ImportError:raise LauncherError('缺少依赖，请先按 README 安装 requirements-lock.txt。') from None
    configured=args.config or os.environ.get('PERO_CONFIG_FILE')
    path=Path(configured).expanduser() if configured else ROOT/'config/local.json'
    if not path.is_absolute():path=ROOT/path
    if not path.exists():
        if configured:raise LauncherError('指定的配置文件不存在：'+str(path))
        atomic_write(path,(ROOT/'config/default.json').read_bytes())
        print('已从默认配置创建 config/local.json，未覆盖其它配置。',flush=True)
    from pero.core.settings import load_settings
    try:settings=load_settings(path)
    except Exception as exc:raise LauncherError('配置校验失败（'+type(exc).__name__+'），请检查运行配置。') from None
    if not (ROOT/'frontend/dist/index.html').is_file():raise LauncherError('前端尚未构建，请进入 frontend 执行 npm ci 和 npm run build。')
    if not 1<=args.manager_port<=65535:raise LauncherError('控制室端口必须在 1–65535。')
    ports=[settings.service.port,args.manager_port]
    if settings.channels.onebot11.enabled:ports.append(settings.channels.onebot11.port)
    if len(ports)!=len(set(ports)):raise LauncherError('核心、控制室和启用的桥接通道端口不能重复。')
    return path.resolve(),settings


def health_check(url,path,service):
    try:
        result=request(url,path)
        return result.get('ok') is True and result.get('service')==service
    except Exception:return False


def stop_request(args,state_dir):
    if not running(state_dir):print('当前没有本脚本管理的运行实例。');return 0
    state=load_state(state_dir)
    atomic_write(state_dir/'stop-request.json',json.dumps({'run_id':state['run_id']}).encode())
    print('已请求停止，等待现有对话和后台任务收尾……',flush=True)
    deadline=time.monotonic()+args.stop_timeout+35
    while time.monotonic()<deadline:
        if not running(state_dir):print('核心和控制室已停止。');return 0
        time.sleep(0.5)
    raise LauncherError('尚未停止，请查看启动窗口；任务繁忙时不会强制终止。')


def drain(state,token,timeout):
    try:
        request(state['core_url'],'/api/v1/management/drain',token=token,body={'enabled':True})
        deadline=time.monotonic()+timeout
        while time.monotonic()<deadline:
            if not request(state['core_url'],'/api/v1/management/drain',token=token).get('busy',True):return True
            time.sleep(0.5)
    except Exception:
        # Do not force-stop a live, unverified core on a failed management call.
        return False
    try:request(state['core_url'],'/api/v1/management/drain',token=token,body={'enabled':False})
    except Exception:pass
    return False


def start(args,state_dir):
    lock=InstanceLock(ROOT/'data/launcher.lock')
    if not lock.acquire():
        state=load_state(state_dir)
        print('Pero 控制室已经在运行：'+state['manager_url'])
        if not args.no_browser:webbrowser.open(state['manager_url'])
        return 0
    core_job=None;manager_job=None;state=None;core=None;manager=None
    try:
        config,settings=check(args)
        available(settings.service.host,settings.service.port)
        available('127.0.0.1',args.manager_port)
        if settings.channels.onebot11.enabled:available(settings.channels.onebot11.host,settings.channels.onebot11.port)
        token=management_token(state_dir,create=True)
        from windows_job import WindowsJob
        run_id=secrets.token_hex(8);run_dir=state_dir/'runs'/run_id;run_dir.mkdir(parents=True)
        core_url=f'http://127.0.0.1:{settings.service.port}'
        if settings.service.host not in ('127.0.0.1','localhost','0.0.0.0','::','::1'):
            raise LauncherError('本地启动器要求核心可以通过本机回环访问。')
        if settings.service.host in ('::','::1'):core_url=f'http://[::1]:{settings.service.port}'
        if settings.service.host=='localhost':core_url=f'http://localhost:{settings.service.port}'
        manager_url=f'http://127.0.0.1:{args.manager_port}'
        log_dir=ROOT/'logs/launcher'/run_id
        env=dict(os.environ,PERO_MANAGEMENT_TOKEN=token,PERO_MANAGER_URL=manager_url,
                 PYTHONIOENCODING='utf-8',PYTHONUNBUFFERED='1')
        if settings.channels.onebot11.enabled:
            from pero.manager.local_access import onebot_token
            variable=settings.channels.onebot11.token_env
            if variable:env[variable]=onebot_token(create=True,environment=variable)
        env['PYTHONPATH']=str(ROOT/'src')+(os.pathsep+env['PYTHONPATH'] if env.get('PYTHONPATH') else '')
        state={'run_id':run_id,'pid':os.getpid(),'core_pid':None,'manager_pid':None,
               'config':str(config),'core_url':core_url,'manager_url':manager_url,'logs':str(log_dir)}
        last_published=None
        def publish():
            nonlocal last_published
            active=core is not None and core.poll() is None
            state['core_pid']=core.pid if active else None
            signature=(state['core_pid'],state['manager_pid'])
            if signature==last_published:return
            atomic_write(run_dir/'core-state.json',json.dumps({'running':active,'pid':state['core_pid']}).encode())
            atomic_write(state_dir/'session.json',json.dumps(state,ensure_ascii=False,indent=2).encode())
            last_published=signature
        def spawn(component,job,stopfile,childenv):
            command=[sys.executable,str(ROOT/'scripts/launch_child.py'),component,'--config',str(config),
                     '--manager-port',str(args.manager_port),'--core-url',core_url,
                     '--manager-state',str(args.manager_state_dir.resolve()),'--stop-file',str(stopfile),
                     '--log',str(log_dir/(component+'.log'))]
            labels=args.manager_state_dir/'scope-labels.json'
            if component=='manager' and labels.is_file():command+=['--scope-labels',str(labels)]
            return job.spawn(command,cwd=ROOT,env=childenv)
        def start_core():
            nonlocal core,core_job
            if core is not None and core.poll() is None:return
            available(settings.service.host,settings.service.port)
            if settings.channels.onebot11.enabled:available(settings.channels.onebot11.host,settings.channels.onebot11.port)
            if core_job:core_job.close()
            (run_dir/'core-stop').unlink(missing_ok=True)
            core_job=WindowsJob();core=spawn('core',core_job,run_dir/'core-stop',env);publish()
        def stop_core():
            nonlocal core,core_job
            if core is None or core.poll() is not None:publish();return
            if not drain(state,token,args.stop_timeout):
                raise LauncherError('核心仍忙或无法核对，未停止。')
            (run_dir/'core-stop').touch()
            deadline=time.monotonic()+30
            while core.poll() is None and time.monotonic()<deadline:time.sleep(.2)
            if core.poll() is None:raise LauncherError('核心停止未确认，请检查日志。')
            core_job.close();core_job=None;core=None;publish()
        publish()
        start_core()
        manager_job=WindowsJob()
        manager_env={**env,'PERO_LOCAL_CONTROL_DIR':str(run_dir.resolve())}
        manager=spawn('manager',manager_job,run_dir/'manager-stop',manager_env)
        state['manager_pid']=manager.pid;publish()
        deadline=time.monotonic()+args.start_timeout
        while time.monotonic()<deadline:
            if manager.poll() is not None:raise LauncherError('控制室退出，请检查日志。')
            if core.poll() is not None:raise LauncherError('核心启动失败，请检查日志。')
            if health_check(core_url,'/health','pero') and health_check(manager_url,'/manage/v1/health','pero-manager'):break
            time.sleep(.3)
        else:raise LauncherError('启动健康检查超时，请检查日志。')
        print('Pero 核心与控制室已启动；网页服务按钮只控制核心，控制室保持在线。',flush=True)
        if not args.no_browser:webbrowser.open(manager_url)
        requested=False
        def stop_signal(*_):
            nonlocal requested
            requested=True
        signal.signal(signal.SIGINT,stop_signal)
        while True:
            if manager.poll() is not None:raise LauncherError('控制室进程已退出。')
            publish()
            marker=state_dir/'stop-request.json'
            try:requested=requested or (marker.exists() and json.loads(marker.read_text())['run_id']==run_id)
            except (OSError,ValueError,KeyError):pass
            if requested:
                try:stop_core()
                except LauncherError:
                    marker.unlink(missing_ok=True);requested=False;time.sleep(.3);continue
                (run_dir/'manager-stop').touch()
                deadline=time.monotonic()+30
                while manager.poll() is None and time.monotonic()<deadline:time.sleep(.2)
                return 0
            command=run_dir/'core-request.json'
            if command.is_file():
                request_value={}
                try:
                    request_value=json.loads(command.read_text(encoding='utf-8'))
                    command.unlink()
                    if set(request_value)!={'request_id','action'}:raise ValueError()
                    if request_value['action']=='start':start_core()
                    elif request_value['action']=='stop':stop_core()
                    else:raise ValueError()
                    result={'request_id':request_value['request_id'],'state':'accepted'}
                except Exception:
                    command.unlink(missing_ok=True)
                    result={'request_id':request_value.get('request_id'),'state':'failed'}
                publish();atomic_write(run_dir/'core-result.json',json.dumps(result).encode())
            time.sleep(.2)
    finally:
        if core_job:core_job.close()
        if manager_job:manager_job.close()
        if state:
            (state_dir/'session.json').unlink(missing_ok=True)
            (state_dir/'stop-request.json').unlink(missing_ok=True)
        lock.close()


def main(argv=None):
    parser=argparse.ArgumentParser(description='Pero Windows 本地启动器')
    parser.add_argument('action',choices=['start','stop','status','login-code','check'],nargs='?',default='start')
    parser.add_argument('--config',help='配置路径；默认 config/local.json，首次从 default.json 创建')
    parser.add_argument('--manager-port',type=int,default=8766)
    parser.add_argument('--state-dir',type=Path,default=ROOT/'data/launcher')
    parser.add_argument('--manager-state-dir',type=Path,default=ROOT/'data/manager',help='控制室状态目录，默认沿用手动启动目录')
    parser.add_argument('--start-timeout',type=float,default=60)
    parser.add_argument('--stop-timeout',type=float,default=120)
    parser.add_argument('--no-browser',action='store_true')
    args=parser.parse_args(argv)
    if sys.platform!='win32':raise LauncherError('此启动脚本用于 Windows；其它平台请使用 README 的手动启动入口。')
    state_dir=args.state_dir.expanduser().resolve()
    if not (0<args.start_timeout<=600 and 0<args.stop_timeout<=600):raise LauncherError('启动／停止等待时间需在 0–600 秒。')
    if args.action=='start':return start(args,state_dir)
    if args.action=='check':check(args);print('配置、依赖和前端构建检查通过。');return 0
    if args.action=='login-code':
        print(management_token(state_dir));return 0  # Explicit user action only; never a log file.
    if args.action=='stop':return stop_request(args,state_dir)
    if not running(state_dir):print('当前没有本脚本管理的运行实例。');return 0
    state=load_state(state_dir)
    print(json.dumps({'running':True,'core_healthy':health_check(state['core_url'],'/health','pero'),
                     'manager_healthy':health_check(state['manager_url'],'/manage/v1/health','pero-manager'),
                     'config':state['config'],'manager_url':state['manager_url'],'logs':state['logs']},ensure_ascii=False,indent=2))
    return 0


if __name__=='__main__':
    try:raise SystemExit(main())
    except LauncherError as exc:print('启动器：'+str(exc),file=sys.stderr);raise SystemExit(1)
    except KeyboardInterrupt:raise SystemExit(130)
