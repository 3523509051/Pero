"""Pero-owned, Windows-user-encrypted local control-room access code."""
import os
from pathlib import Path
import secrets

from .vault import DPAPI, VaultError

ROOT = Path(__file__).resolve().parents[3]


class AccessCodeError(RuntimeError):
    pass


def management_token(*, create=False, legacy_paths=(), directory=None,
                     _name='management-token.dpapi', _environment='PERO_MANAGEMENT_TOKEN'):
    """Viewing never creates a credential; launchers request create explicitly.

    Optional legacy files permit migration without giving any bridge responsibility
    for generating or owning Pero's management credential.
    """
    folder = Path(directory) if directory is not None else ROOT / 'data/manager'
    target = folder / _name
    override = os.environ.get(_environment, '').strip()
    try:
        if override and create:
            value = override
        elif target.is_file():
            return DPAPI.transform(target.read_bytes(), decrypt=True).decode('utf-8')
        else:
            value = None
            for candidate in [*legacy_paths, ROOT / 'data/launcher' / _name]:
                candidate = Path(candidate)
                if candidate.is_file():
                    value = DPAPI.transform(candidate.read_bytes(), decrypt=True).decode('utf-8')
                    break
            if not create:
                if value:
                    return value
                raise AccessCodeError('尚未生成本机凭据，请先启动 Pero 或完成连接配置。')
            if not value:
                value = secrets.token_urlsafe(32)
        protected = DPAPI.transform(value.encode('utf-8'))
        folder.mkdir(parents=True, exist_ok=True)
        if override:
            temporary = folder / ('access-' + secrets.token_hex(8) + '.tmp')
            try:
                with temporary.open('xb') as stream:
                    stream.write(protected); stream.flush(); os.fsync(stream.fileno())
                os.replace(temporary, target)
            finally:
                temporary.unlink(missing_ok=True)
        else:
            try:
                with target.open('xb') as stream:
                    stream.write(protected); stream.flush(); os.fsync(stream.fileno())
            except FileExistsError:
                return DPAPI.transform(target.read_bytes(), decrypt=True).decode('utf-8')
        return value
    except (VaultError, UnicodeError, OSError) as error:
        raise AccessCodeError('无法读取当前 Windows 用户加密的管理访问码；请使用原用户检查，未输出凭据。') from error


def onebot_token(*, create=False, legacy_paths=(), environment='PERO_ONEBOT_TOKEN',directory=None):
    return management_token(create=create,legacy_paths=legacy_paths,directory=directory,
                            _name='onebot-token.dpapi',_environment=environment)
