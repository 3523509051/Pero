"""每插件版本独立依赖环境：固定版本、批准清单、最小环境、失败保留旧版。

这不是权限沙箱：普通 venv 与子进程不能隔离恶意代码。它只保证 worker 不继承
宿主凭据/管理令牌、不修改主程序环境，并且依赖版本可复现。
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import shutil
import sys
from pathlib import Path
from typing import Iterable, Mapping, Optional

from pero.plugins.errors import PluginError

#: 只允许这些已核对的固定版本；未知依赖不安装。
APPROVED = frozenset({"httpx==0.28.1", "Pillow==11.3.0"})
_ENV_ALLOW = (
    "SYSTEMROOT", "WINDIR", "PATH", "TEMP", "TMP", "COMSPEC",
    "NUMBER_OF_PROCESSORS", "OS", "PATHEXT", "LOCALAPPDATA",
)


def safe_worker_env(*, pythonpath: str) -> dict[str, str]:
    """构造不含 PERO_/Key/Token/凭据变量的最小子进程环境。"""

    env: dict[str, str] = {}
    for name in _ENV_ALLOW:
        value = os.environ.get(name)
        if value is None:
            continue
        lowered = name.lower()
        if any(part in lowered for part in ("key", "token", "secret", "password", "credential")):
            continue
        env[name] = value
    env.update({
        "PYTHONPATH": pythonpath,
        "PYTHONNOUSERSITE": "1",
        "PYTHONDONTWRITEBYTECODE": "1",
        "PYTHONIOENCODING": "utf-8",
        "NO_COLOR": "1",
    })
    return env


class PluginEnvironment:
    def __init__(self, root: Path, *, host_python: str = "", timeout_seconds: float = 240.0) -> None:
        self.root = Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self.host_python = host_python or sys.executable
        self.timeout = float(timeout_seconds)
        self._locks: dict[str, asyncio.Lock] = {}

    @staticmethod
    def _venv_python(path: Path) -> Path:
        return path / ("Scripts/python.exe" if os.name == "nt" else "bin/python")

    def _lock(self, key: str) -> asyncio.Lock:
        return self._locks.setdefault(key, asyncio.Lock())

    @staticmethod
    def _normalize(dependencies: Iterable[str]) -> tuple[str, ...]:
        result = tuple(str(item).strip() for item in dependencies if str(item).strip())
        if len(result) > 16 or any(item not in APPROVED for item in result):
            raise PluginError("dependency_not_approved", "插件依赖包含未批准的版本")
        return result

    async def _run(self, *command: str) -> None:
        try:
            process = await asyncio.create_subprocess_exec(
                *command,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL,
                env=safe_worker_env(pythonpath=""),
                cwd=str(self.root),
            )
            code = await asyncio.wait_for(process.wait(), timeout=self.timeout)
        except (OSError, asyncio.TimeoutError):
            raise PluginError("dependency_prepare_failed", "依赖环境准备超时或失败") from None
        if code != 0:
            raise PluginError("dependency_prepare_failed", "依赖环境准备失败")

    async def prepare(self, plugin_id: str, version: str, package: Path,
                      dependencies: Iterable[str]) -> str:
        """准备或复用插件版本环境，返回该环境解释器的绝对路径。"""

        deps = self._normalize(dependencies)
        target = self.root / plugin_id / version
        marker = target / "ready.json"
        lock_file = Path(package) / "requirements.lock.txt"
        digest = hashlib.sha256(lock_file.read_bytes()).hexdigest() if lock_file.is_file() else ""
        expected = {"deps": list(deps), "lock_sha256": digest, "python": self.host_python}
        async with self._lock(f"{plugin_id}:{version}"):
            interpreter = self._venv_python(target)
            if marker.is_file() and interpreter.is_file():
                try:
                    if json.loads(marker.read_text(encoding="utf-8")) == expected:
                        return str(interpreter)
                except (ValueError, OSError):
                    pass
            if target.exists():
                await asyncio.to_thread(shutil.rmtree, target, True)
            try:
                await self._run(self.host_python, "-m", "venv", str(target))
                if deps:
                    python = str(interpreter)
                    if lock_file.is_file() and digest:
                        await self._run(python, "-m", "pip", "install", "--no-input",
                                        "--disable-pip-version-check", "--only-binary=:all:",
                                        "-r", str(lock_file))
                    else:
                        await self._run(python, "-m", "pip", "install", "--no-input",
                                        "--disable-pip-version-check", "--only-binary=:all:",
                                        *deps)
                temporary = marker.with_suffix(".tmp")
                await asyncio.to_thread(temporary.write_text,
                                        json.dumps(expected, ensure_ascii=False, sort_keys=True),
                                        encoding="utf-8")
                await asyncio.to_thread(os.replace, temporary, marker)
            except PluginError:
                await asyncio.to_thread(shutil.rmtree, target, True)
                raise
            return str(interpreter)
