"""Read-only HTTPS service access for explicitly approved public origins."""
from urllib.parse import urlsplit,urljoin
import asyncio
import ipaddress
import socket

from pero.plugins.errors import PluginError


class PublicNetwork:
    def __init__(self, hosts=(), *, transport=None):
        self.hosts=frozenset(hosts)
        self.transport=transport

    async def read(self,url,*,max_bytes=2*1024*1024,timeout=15,redirect_metadata=False):
        import httpx
        try:
            parts=urlsplit(url)
            host=parts.hostname
            if parts.scheme!='https' or not host or host not in self.hosts or parts.username or parts.password or parts.port not in (None,443):
                raise ValueError('origin')
        except (ValueError,TypeError):
            raise PluginError('network_origin_denied','此服务没有获准读取') from None
        if not isinstance(max_bytes,int) or not 1<=max_bytes<=8*1024*1024 or not 0<timeout<=30:
            raise PluginError('network_limits_invalid','网络预算不合法')
        # Approved service names are explicitly configured by the administrator.
        # Reject non-public resolution; this facade is not an OS egress sandbox.
        try:
            records=await asyncio.get_running_loop().getaddrinfo(host,443,type=socket.SOCK_STREAM)
            if not records or not all(ipaddress.ip_address(row[4][0]).is_global for row in records):
                raise ValueError('address')
        except (OSError,ValueError):
            raise PluginError('network_origin_denied','无法验证公开服务地址') from None
        try:
            async with httpx.AsyncClient(timeout=timeout,trust_env=False,follow_redirects=False,transport=self.transport) as client:
                async with client.stream('GET',url,headers={'User-Agent':'PeroPlugin/1.0'}) as response:
                    if 300<=response.status_code<400 and redirect_metadata:
                        location=urljoin(url,response.headers.get('location',''))
                        target=urlsplit(location)
                        if target.scheme!='https' or target.hostname not in self.hosts or target.username or target.password or target.port not in (None,443):raise PluginError('network_origin_denied','重定向目标未获授权')
                        return {'url':url,'content_type':'','bytes':b'','status_code':response.status_code,'location':location}
                    if response.status_code!=200:raise PluginError('network_status',f'HTTP {response.status_code}')
                    body=bytearray()
                    async for block in response.aiter_bytes():
                        if len(body)+len(block)>max_bytes:raise PluginError('network_too_large','响应超过大小限制')
                        body.extend(block)
                    return {'content_type':response.headers.get('content-type',''),'bytes':bytes(body),'url':str(response.request.url)}
        except httpx.HTTPError:
            raise PluginError('network_unavailable','公开服务读取失败') from None
