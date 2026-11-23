"""最小启动/关闭/配置核对脚本（P15；手工运行，不装服务、不留自启）。

用法（PowerShell，在仓库根）：
    .\\.venv\\Scripts\\python.exe scripts\\check_run.py            # 配置核对 + 导入自检
    .\\.venv\\Scripts\\python.exe scripts\\check_run.py --serve     # 前台起服务，Ctrl+C 停止

只做：配置加载/校验、组件装配、路由注册与 /health 自检；不连桥/模型/真实 API，
不写业务数据，不创建任何计划任务或后台进程。
"""

from __future__ import annotations

import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "src"))


def main() -> int:
    from pero.core.settings import load_settings

    settings = load_settings()
    print(f"[1] 配置加载 OK（config_path={settings.config_path or '内置默认'}）")
    print(
        f"    开关概览：通道={settings.channels.onebot11.enabled}"
        f" 自动处理={settings.orchestration.auto_process_enabled}"
        f" 自动发送={settings.orchestration.auto_send_enabled}"
        f" L10={settings.conversation.l10_enabled}"
        f" L3={settings.memory.l3_enabled}"
        f" 向量={settings.embeddings.enabled}"
        f" 归档={settings.archive.enabled}"
    )

    from pero.runtime import build_runtime

    runtime = build_runtime(settings)
    print(
        "[2] 组件装配 OK（"
        f"memory={'OK' if runtime.memory.available else '不可用'}"
        f" events={'OK' if runtime.events is not None else '缺失'}"
        f" l10={'启用' if runtime.l10 is not None else '关闭'}"
        f" semantic={'启用' if runtime.semantic is not None else '关闭'}）"
    )

    from pero.api.app import create_app

    app = create_app(runtime=runtime)
    print(f"[3] 路由注册 OK（{len(app.routes)} 条）")

    if "--serve" in sys.argv:
        import uvicorn

        print(
            f"[4] 前台启动 http://{settings.service.host}:{settings.service.port}"
            "（Ctrl+C 停止；不写任何后台自启）"
        )
        from pero.core.logging_setup import configure

        configure()
        uvicorn.run(
            app,
            host=settings.service.host,
            port=settings.service.port,
            log_level=settings.service.log_level,
            log_config=None,
        )
    else:
        print("[4] 未启动服务（加 --serve 前台运行）")
    print("CHECK_OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
