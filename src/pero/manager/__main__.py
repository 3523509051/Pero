"""Start locally with python -m pero.manager; does not start Pero or any bridge."""
import argparse
from pathlib import Path
import uvicorn

from pero.core.settings import default_config_path
from .app import create_manager_app
from .settings import ManagerSettings


def main(argv=None):
    parser = argparse.ArgumentParser(description='Pero local manager (F04; authenticated configuration and fixed service operations)')
    parser.add_argument('--host', choices=['127.0.0.1', 'localhost'], default='127.0.0.1')
    parser.add_argument('--port', type=int, default=8766)
    parser.add_argument('--config', type=Path, default=default_config_path())
    parser.add_argument('--scope-labels', type=Path)
    parser.add_argument('--core-url', default='http://127.0.0.1:8765')
    parser.add_argument('--state-dir', type=Path)
    parser.add_argument('--services-file', type=Path)
    args = parser.parse_args(argv)
    if not 1 <= args.port <= 65535:
        parser.error('port must be within 1..65535')
    settings = ManagerSettings(args.config, args.core_url, args.scope_labels, state_dir=args.state_dir, services_path=args.services_file)
    # uvicorn 默认 log_config 用本地时区渲染 asctime；这里接管根 logger 并禁用
    # uvicorn 自己的配置，保证 Manager 日志与核心服务同为北京时间。
    from pero.core.logging_setup import configure

    configure()
    uvicorn.run(create_manager_app(settings), host=args.host, port=args.port,
                log_config=None)


if __name__ == '__main__':
    main()
