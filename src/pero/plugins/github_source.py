"""GitHub 固定版本来源解析：预览、校验、解压；安装阶段不执行任何包内代码。

外部 CLI（npm `plugins`）的包发现/版本/target 思路作参考；不假定其存在
`--target pero`，不复用其他 Agent 的配置目录，也不维护第二份插件状态。
"""

from __future__ import annotations

import hashlib
import io
import json
import re
import tarfile
import tempfile
from pathlib import Path
from typing import Any, Mapping, Optional

import httpx

from pero.plugins.errors import PluginError
from pero.plugins.manifest import PluginManifest, parse_manifest

_REPO = re.compile(
    r"^https://github\.com/([A-Za-z0-9_.-]{1,80})/([A-Za-z0-9_.-]{1,80})"
    r"(?:/tree/([A-Za-z0-9_./-]{1,120}))?/?$"
)
_SHA = re.compile(r"^[0-9a-f]{40}$")
_SUFFIXES = frozenset({".py", ".json", ".toml", ".txt", ".md", ".png", ".jpg", ".jpeg", ".gif", ".webp"})
_MAX_ARCHIVE = 25 * 1024 * 1024
_MAX_FILE = 2 * 1024 * 1024
_MAX_TOTAL = 12 * 1024 * 1024
_MAX_FILES = 200


def parse_repo(url: str) -> tuple[str, str, str]:
    match = _REPO.fullmatch(str(url or "").strip())
    if match is None:
        raise PluginError("invalid_github_url", "只支持 https://github.com/owner/repo[/tree/ref]")
    owner, repo, ref = match.groups()
    return owner, repo, (ref or "").strip("/")


class GitHubSource:
    def __init__(self, *, timeout_seconds: float = 30.0) -> None:
        self.timeout = float(timeout_seconds)

    async def _client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(timeout=self.timeout, follow_redirects=False,
                                 trust_env=False, headers={"User-Agent": "PeroPluginSource/1.0"})

    async def resolve_commit(self, owner: str, repo: str, ref: str) -> str:
        if not ref or len(ref) > 120 or any(char in ref for char in ("?", "#", "@{")):
            raise PluginError("ref_required", "必须指定固定 tag 或 commit，不接受默认分支")
        url = f"https://api.github.com/repos/{owner}/{repo}/commits/{ref}"
        async with await self._client() as client:
            try:
                response = await client.get(url)
            except httpx.HTTPError:
                raise PluginError("source_unreachable", "GitHub API 不可达") from None
        if response.status_code != 200:
            raise PluginError("ref_not_found", "指定的 tag/commit 不存在")
        try:
            commit = str(response.json().get("sha") or "")
        except ValueError:
            raise PluginError("source_invalid_response", "GitHub 响应不是合法 JSON") from None
        if not _SHA.fullmatch(commit):
            raise PluginError("ref_not_found", "无法解析固定 commit")
        return commit

    async def _download(self, owner: str, repo: str, commit: str) -> bytes:
        url = f"https://codeload.github.com/{owner}/{repo}/tar.gz/{commit}"
        async with await self._client() as client:
            try:
                response = await client.get(url)
            except httpx.HTTPError:
                raise PluginError("source_unreachable", "插件包下载失败") from None
        if response.status_code != 200:
            raise PluginError("source_unreachable", "插件包下载失败")
        data = response.content
        if not data or len(data) > _MAX_ARCHIVE:
            raise PluginError("archive_too_large", "插件归档超过大小上限")
        return data

    @staticmethod
    def _extract(archive: bytes, target: Path) -> tuple[PluginManifest, str, list[dict]]:
        target.mkdir(parents=True, exist_ok=True)
        files: list[dict] = []
        total = 0
        try:
            with tarfile.open(fileobj=io.BytesIO(archive), mode="r:gz") as tar:
                for member in tar.getmembers():
                    if not member.isfile() or member.issym() or member.islnk():
                        continue
                    parts = Path(member.name).parts
                    if len(parts) < 2 or any(part in {"..", ".", ""} for part in parts):
                        continue
                    relative = Path(*parts[1:])
                    if len(parts) > 8 or len(files) >= _MAX_FILES:
                        raise PluginError("archive_too_large", "插件文件数超过上限")
                    license_files = {"LICENSE", "LICENSE.txt", "NOTICE", "COPYING"}
                    if member.size > _MAX_FILE or (relative.suffix.lower() not in _SUFFIXES
                                                   and relative.name not in license_files):
                        continue
                    total += int(member.size)
                    if total > _MAX_TOTAL:
                        raise PluginError("archive_too_large", "插件总量超过上限")
                    stream = tar.extractfile(member)
                    if stream is None:
                        continue
                    data = stream.read(_MAX_FILE + 1)
                    if len(data) != member.size:
                        raise PluginError("archive_invalid", "插件文件内容与声明不符")
                    destination = target / relative
                    destination.parent.mkdir(parents=True, exist_ok=True)
                    destination.write_bytes(data)
                    files.append({"path": relative.as_posix(), "size": len(data),
                                  "sha256": hashlib.sha256(data).hexdigest()})
        except (tarfile.TarError, OSError, ValueError):
            raise PluginError("archive_invalid", "插件归档格式不合法") from None
        manifest_path = target / "plugin.json"
        if not manifest_path.is_file():
            raise PluginError("manifest_not_found", "归档根目录缺少 plugin.json")
        manifest = parse_manifest(json.loads(manifest_path.read_text(encoding="utf-8")))
        return manifest, hashlib.sha256(manifest_path.read_bytes()).hexdigest(), files

    async def preview(self, url: str, ref: str = "") -> dict:
        owner, repo, url_ref = parse_repo(url)
        fixed = str(ref or url_ref)
        commit = await self.resolve_commit(owner, repo, fixed)
        archive = await self._download(owner, repo, commit)
        with tempfile.TemporaryDirectory(prefix="pero-source-preview-") as directory:
            manifest, manifest_sha, files = self._extract(archive, Path(directory))
            self._validate_remote(manifest, Path(directory))
            result = {
                "owner": owner, "repo": repo, "ref": fixed, "commit": commit,
                "package_sha256": hashlib.sha256(archive).hexdigest(),
                "manifest_sha256": manifest_sha,
                "id": manifest.id, "name": manifest.name, "version": manifest.version,
                "license": manifest.license, "description": manifest.description,
                "tools": list(manifest.tools), "capabilities": list(manifest.capabilities),
                "dependencies": list(manifest.dependencies), "network_hosts":list(manifest.network_hosts), "background_tools":list(manifest.background_tools), "runtime": manifest.runtime,
                "config_schema": dict(manifest.config_schema),
                "files": files,
                "install_requires": "确认后下载执行；默认停用，启用才启动 worker",
            }
            return result

    @staticmethod
    def _validate_remote(manifest: PluginManifest, package: Path) -> None:
        if manifest.runtime != "worker":
            raise PluginError("remote_host_runtime_denied", "远程插件必须在独立 worker 中运行")
        if not (package / "requirements.lock.txt").is_file():
            raise PluginError("dependency_lock_missing", "远程插件缺少依赖锁定文件")
        if not (package / "LICENSE").is_file():
            raise PluginError("license_missing", "远程插件缺少许可证文件")
        if "network.video_read" in manifest.capabilities and not manifest.dependencies:
            raise PluginError("dependency_declaration_missing", "网络能力插件必须声明固定依赖")

    async def fetch(self, url: str, commit: str, *, expected_package_sha256: str) -> tuple[Path, PluginManifest, dict]:
        if not _SHA.fullmatch(commit):
            raise PluginError("invalid_commit", "commit 格式不合法")
        owner, repo, _ref = parse_repo(url)
        archive = await self._download(owner, repo, commit)
        digest = hashlib.sha256(archive).hexdigest()
        if digest != str(expected_package_sha256).strip().lower():
            raise PluginError("package_hash_mismatch", "插件包摘要与预览不一致")
        root = Path(tempfile.mkdtemp(prefix="pero-source-install-"))
        manifest, manifest_sha, files = self._extract(archive, root)
        self._validate_remote(manifest, root)
        preview = {"commit": commit, "package_sha256": digest,
                   "manifest_sha256": manifest_sha, "files": files}
        return root, manifest, preview
