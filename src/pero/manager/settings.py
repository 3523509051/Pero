"""Independent read-only manager configuration; no runtime or credentials loaded."""
from dataclasses import dataclass
from pathlib import Path
from ipaddress import ip_address
from urllib.parse import urlsplit


@dataclass(frozen=True)
class ManagerSettings:
    config_path: Path
    core_url: str = 'http://127.0.0.1:8765'
    labels_path: Path | None = None
    timeout_seconds: float = 3.0
    state_dir: Path | None = None
    services_path: Path | None = None
    allowed_hosts: tuple[str, ...] = ('localhost', '127.0.0.1', '[::1]', '::1')
    allowed_clients: tuple[str, ...] = ()

    def __post_init__(self):
        if not self.allowed_hosts or any(not host or '*' in host or '/' in host for host in self.allowed_hosts):
            raise ValueError('Explicit manager hosts are required')
        for address in self.allowed_clients:
            ip_address(address)
        url = urlsplit(self.core_url)
        if (url.scheme != 'http' or url.hostname not in {'127.0.0.1', 'localhost', '::1'}
                or url.username or url.password or url.query or url.fragment
                or url.path not in {'', '/'}):
            raise ValueError('F02 core_url must be a loopback HTTP origin')
        if not 0 < self.timeout_seconds <= 30:
            raise ValueError('timeout_seconds must be within 0..30')
