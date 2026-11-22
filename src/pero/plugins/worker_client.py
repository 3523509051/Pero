"""Core 侧独立 worker 客户端：stdio JSON 协议、最小环境、故障即回收。"""

from __future__ import annotations

import asyncio
import json
import os
import uuid
from pathlib import Path
from typing import Any, Optional

from pero.core.settings import repo_root
from pero.plugins.env_manager import safe_worker_env
from pero.plugins.errors import PluginError


class PluginWorkerClient:
    def __init__(
        self,
        *,
        package: Path,
        data_dir: Path,
        python: str,
        timeout_seconds: float,
        quota_mb: int = 32,
    ) -> None:
        self.package = Path(package).resolve()
        self.data_dir = Path(data_dir).resolve()
        self.python = python or os.sys.executable
        self.timeout = float(timeout_seconds)
        self.quota_mb = int(quota_mb)
        self._proc: Optional[asyncio.subprocess.Process] = None
        self._lock = asyncio.Lock()

    async def start(self) -> None:
        self.data_dir.mkdir(parents=True, exist_ok=True)
        env = safe_worker_env(pythonpath=str(repo_root() / "src"))
        try:
            self._proc = await asyncio.create_subprocess_exec(
                self.python, "-m", "pero.plugins.worker",
                "--package", str(self.package),
                "--data-dir", str(self.data_dir),
                "--quota-mb", str(self.quota_mb),
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.DEVNULL,
                env=env,
                cwd=str(self.data_dir),
            )
        except OSError:
            raise PluginError("worker_start_failed", "插件 worker 无法启动") from None

    async def request(self, payload: dict, timeout_override=None) -> dict:
        if self._proc is None or self._proc.stdin is None or self._proc.stdout is None:
            raise PluginError("worker_unavailable", "插件 worker 未启动")
        request_id = uuid.uuid4().hex
        body = {**payload, "request_id": request_id}
        async with self._lock:
            if self._proc is None or self._proc.returncode is not None:
                raise PluginError("worker_unavailable","插件进程已停止")
            try:
                self._proc.stdin.write((json.dumps(body, ensure_ascii=False) + "\n").encode("utf-8"))
                await self._proc.stdin.drain()
                line = await asyncio.wait_for(self._proc.stdout.readline(), timeout=float(timeout_override or self.timeout))
            except asyncio.CancelledError:
                await self.discard()
                raise
            except (BrokenPipeError, ConnectionResetError, asyncio.TimeoutError, OSError, ValueError):
                await self.discard()
                raise PluginError("worker_unavailable", "插件 worker 无响应或超时") from None
            if not line:
                await self.discard()
                raise PluginError("worker_exited", "插件 worker 已退出")
            try:
                response = json.loads(line.decode("utf-8"))
            except (UnicodeDecodeError, ValueError):
                await self.discard()
                raise PluginError("worker_invalid_response", "插件 worker 返回非法数据") from None
            if not isinstance(response, dict) or response.get("request_id") != request_id:
                await self.discard()
                raise PluginError("worker_response_mismatch", "响应与请求不对应")
            if not response.get("ok"):
                error = response.get("error") if isinstance(response.get("error"), dict) else {}
                code = str(error.get("code") or "worker_rejected")
                if code in {"worker_error", "plugin_result_invalid"}:
                    await self.discard()
                raise PluginError(code, str(error.get("message") or "插件 worker 拒绝执行"))
            return response

    async def init(self) -> list:
        response = await self.request({"op": "init"})
        tools = response.get("tools")
        return list(tools) if isinstance(tools, list) else []

    async def call(self, tool: str, arguments: dict, config: dict, context: dict, network_hosts=(),timeout_override=None) -> dict:
        return await self.request({"op": "call", "tool": tool, "arguments": arguments,
                                   "config": config, "context": context, "network_hosts": list(network_hosts)},timeout_override=timeout_override)

    async def discard(self) -> None:
        """超时/断连/错配后清理旧进程，下一次请求必须重建干净状态。"""

        process = self._proc
        self._proc = None
        if process is None:
            return
        try:
            if process.returncode is None:
                process.terminate()
                try:
                    await asyncio.wait_for(process.wait(), timeout=3)
                except asyncio.TimeoutError:
                    process.kill()
                    await asyncio.wait_for(process.wait(), timeout=3)
        except (ProcessLookupError, asyncio.TimeoutError, OSError):
            pass

    async def close(self) -> None:
        await self.discard()
