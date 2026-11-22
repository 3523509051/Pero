"""Single owned service entry, with file logs and cooperative shutdown."""
import argparse
import asyncio
import logging
from pathlib import Path
import sys


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('component',choices=['core','manager'])
    parser.add_argument('--config',required=True)
    parser.add_argument('--manager-port',type=int,required=True)
    parser.add_argument('--core-url',required=True)
    parser.add_argument('--manager-state',required=True)
    parser.add_argument('--stop-file',type=Path,required=True)
    parser.add_argument('--log',type=Path,required=True)
    args=parser.parse_args()
    args.log.parent.mkdir(parents=True,exist_ok=True)
    stream=args.log.open('a',encoding='utf-8',buffering=1)
    original_stdout,original_stderr=sys.stdout,sys.stderr
    sys.stdout=sys.stderr=stream
    try:
        import uvicorn
        from pero.core.settings import load_settings
        settings=load_settings(args.config)
        logging.basicConfig(level=settings.service.log_level.upper(),format='%(asctime)s %(levelname)s %(name)s: %(message)s',stream=stream)
        if args.component=='core':
            from pero.api.app import create_app
            from pero.runtime import build_runtime
            app=create_app(settings,build_runtime(settings))
            host,port=settings.service.host,settings.service.port
        else:
            from pero.manager.app import create_manager_app
            from pero.manager.settings import ManagerSettings
            app=create_manager_app(ManagerSettings(Path(args.config),core_url=args.core_url,state_dir=Path(args.manager_state)))
            host,port='127.0.0.1',args.manager_port
        server=uvicorn.Server(uvicorn.Config(app,host=host,port=port,log_level=settings.service.log_level))
        async def run():
            async def watch():
                while not args.stop_file.exists():await asyncio.sleep(0.25)
                server.should_exit=True
            task=asyncio.create_task(watch())
            try:await server.serve()
            finally:task.cancel();await asyncio.gather(task,return_exceptions=True)
        asyncio.run(run())
    finally:
        sys.stdout,sys.stderr=original_stdout,original_stderr
        stream.close()


if __name__=='__main__':main()
