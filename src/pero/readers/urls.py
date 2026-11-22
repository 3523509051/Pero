"""Parsed-host routing and public HTTP URL validation."""
from __future__ import annotations

import asyncio
import ipaddress
import socket
from urllib.parse import urlsplit, urlunsplit


def route_reader(url: str) -> str:
    host = (urlsplit(url).hostname or "").lower().rstrip(".")
    if host in {"bilibili.com", "b23.tv"} or host.endswith(".bilibili.com"):
        return "bilibili"
    if host == "mp.weixin.qq.com":
        return "wechat"
    return "deepseek"


async def validate_public_url(url: str) -> str:
    if not isinstance(url, str) or not url.strip() or len(url) > 8192:
        raise ValueError("invalid_url")
    parts = urlsplit(url.strip())
    if (parts.scheme not in {"http", "https"} or not parts.hostname
        or parts.username is not None or parts.password is not None):
        raise ValueError("invalid_url")
    host = parts.hostname.encode("idna").decode("ascii").lower().rstrip(".")
    port = parts.port or (443 if parts.scheme == "https" else 80)
    addresses = await asyncio.wait_for(
        asyncio.get_running_loop().getaddrinfo(host, port, type=socket.SOCK_STREAM), 5.0)
    if not addresses or any(not ipaddress.ip_address(item[4][0]).is_global for item in addresses):
        raise ValueError("non_public_url")
    netloc = f"[{host}]" if ":" in host else host
    if parts.port is not None:
        netloc += f":{port}"
    return urlunsplit((parts.scheme, netloc, parts.path or "/", parts.query, ""))
