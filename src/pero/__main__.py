"""启动入口：``python -m pero``（等价 ``pero`` 命令）。

默认只监听 127.0.0.1:8765，可用配置或命令行参数修改。
Ctrl+C 即停，本阶段不注册计划任务、Windows 服务或容器服务。
"""

from __future__ import annotations

import argparse
import logging
import sys
from typing import Sequence

from pero import PHASE, __version__
from pero.api.app import create_app
from pero.core.settings import ENV_HOST, ENV_PORT, load_settings
from pero.runtime import build_runtime


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="pero",
        description="pero 独立后端（P02：模型客户端与有界工具循环）",
    )
    parser.add_argument("--host", help=f"监听地址，默认取配置或 {ENV_HOST}")
    parser.add_argument("--port", type=int, help=f"监听端口，默认取配置或 {ENV_PORT}")
    parser.add_argument("--config", help="配置文件路径，默认 config/default.json")
    parser.add_argument("--log-level", help="日志级别，默认取配置 service.log_level")
    parser.add_argument("--version", action="store_true", help="打印版本后退出")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)

    if args.version:
        print(f"pero {__version__} ({PHASE})")
        return 0

    settings = load_settings(args.config)
    if args.host:
        settings.service.host = args.host
    if args.port:
        settings.service.port = args.port
    if args.log_level:
        settings.service.log_level = args.log_level.lower()

    from pero.core.logging_setup import configure as configure_logging

    # 日志时间统一北京时间并带 +0800，不依赖服务器本地时区。
    configure_logging(
        getattr(logging, settings.service.log_level.upper(), logging.INFO)
    )

    runtime = build_runtime(settings)
    app = create_app(settings, runtime)

    print(f"pero {__version__} phase={settings.phase}")
    print(f"  listen     http://{settings.service.host}:{settings.service.port}")
    print(f"  config     {settings.config_path or '(内置默认值)'}")
    print(f"  components {len(runtime.components)}  tools {len(runtime.tools)}")
    print(
        f"  model      {settings.thinking.model} via {settings.provider.base_url}"
        f"  key={'set' if runtime.provider.api_key_configured else 'missing'}"
    )
    print(
        f"  limits     model_calls<={settings.runtime.max_model_calls}"
        f" tool_calls<={settings.runtime.max_tool_calls}"
        f" tool_timeout={settings.runtime.tool_timeout_seconds:g}s"
        f" history<={settings.runtime.max_history_messages}"
    )
    print("  endpoints  /health, /api/v1/capabilities, /api/v1/components,"
          " /api/v1/agents, /api/v1/agents/{id}/composition,")
    print("             POST /api/v1/agents/{id}/sessions,"
          " POST|GET /api/v1/agents/{id}/sessions/{sid}[/messages]")
    print("  退出：Ctrl+C")

    import uvicorn

    try:
        uvicorn.run(
            app,
            host=settings.service.host,
            port=settings.service.port,
            log_level=settings.service.log_level,
        )
    except KeyboardInterrupt:  # pragma: no cover - 交互退出
        print("pero 已停止")
    return 0


if __name__ == "__main__":
    sys.exit(main())
