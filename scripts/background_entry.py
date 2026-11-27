"""Start only independent Pero Core/Manager behind a persistent supervisor."""
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import webbrowser
from launcher import ROOT,running,load_state,health_check,LauncherError

def main(argv=None):
    argv=list(sys.argv[1:] if argv is None else argv)
    state_dir=ROOT/'data/launcher'
    timeout=60.0
    no_browser='--no-browser' in argv
    for i,item in enumerate(argv):
        if item=='--state-dir' and i+1<len(argv):state_dir=Path(argv[i+1]).resolve()
        elif item.startswith('--state-dir='):state_dir=Path(item.split('=',1)[1]).resolve()
        elif item=='--start-timeout' and i+1<len(argv):timeout=float(argv[i+1])
        elif item.startswith('--start-timeout='):timeout=float(item.split('=',1)[1])
    if not 0<timeout<=600:raise LauncherError('启动超时必须在 0–600 秒。')
    child=None
    if not running(state_dir):
        logs=ROOT/'logs/launcher';logs.mkdir(parents=True,exist_ok=True)
        env={**os.environ,'PYTHONIOENCODING':'utf-8'}
        with (logs/'background.log').open('ab',buffering=0) as stream:
            child=subprocess.Popen([sys.executable,str(ROOT/'scripts/launcher.py'),'start','--no-browser',*argv],
                cwd=ROOT,env=env,stdin=subprocess.DEVNULL,stdout=stream,stderr=stream,
                creationflags=subprocess.CREATE_NO_WINDOW|subprocess.CREATE_NEW_PROCESS_GROUP)
    deadline=time.monotonic()+timeout+5
    while time.monotonic()<deadline:
        if child and child.poll() not in (None,0):raise LauncherError('Pero 启动失败，请查看 logs/launcher/background.log。')
        try:
            state=load_state(state_dir)
            if running(state_dir) and health_check(state['core_url'],'/health','pero') and health_check(state['manager_url'],'/manage/v1/health','pero-manager'):
                print('Pero 核心与控制室已在后台启动；未启动 webot。\n控制室：'+state['manager_url'])
                if not no_browser:webbrowser.open(state['manager_url'])
                return 0
        except (OSError,ValueError,LauncherError):pass
        time.sleep(.3)
    raise LauncherError('尚未确认 Pero 启动，请检查 logs/launcher/background.log。')

if __name__=='__main__':
    try:raise SystemExit(main())
    except (LauncherError,ValueError) as error:print('Pero 启动器：'+str(error),file=sys.stderr);raise SystemExit(1)
