"""Bounded read-only transport. No arbitrary URL, commands, retries or model calls."""
import httpx


class CoreUnavailable(Exception):
    def __init__(self, code, upstream_status=None):
        self.code, self.upstream_status = code, upstream_status
        super().__init__(code)


class CoreReader:
    def __init__(self, settings, transport=None):
        self.client = httpx.AsyncClient(base_url=settings.core_url.rstrip('/'),
            timeout=settings.timeout_seconds, follow_redirects=False, trust_env=False,
            transport=transport)

    async def close(self):
        await self.client.aclose()

    async def read(self, path, params=None):
        if (not (path == '/health' or path.startswith('/api/v1/')) or '?' in path
                or any(part in {'.', '..'} for part in path.split('/'))):
            raise ValueError('unsupported internal read path')
        try:
            response = await self.client.get(path, params=params)
        except httpx.TimeoutException:
            raise CoreUnavailable('core_timeout') from None
        except httpx.HTTPError:
            raise CoreUnavailable('core_unreachable') from None
        if response.status_code != 200:
            raise CoreUnavailable('core_http_error', response.status_code)
        try:
            data = response.json()
        except ValueError:
            raise CoreUnavailable('core_invalid_response', response.status_code) from None
        if not isinstance(data, dict):
            raise CoreUnavailable('core_invalid_response', response.status_code)
        return data

    async def management(self, method, suffix, payload=None):
        import os
        from pero.configuration.auth import TOKEN_ENV
        token = os.environ.get(TOKEN_ENV, '').strip()
        if not token:
            raise CoreUnavailable('management_not_configured')
        if (method, suffix) not in {('GET','/config-state'),('GET','/tools'),('GET','/models'),('PUT','/config-snapshot'),('POST','/drain'),('GET','/drain'),('POST','/models/check')}:
            raise ValueError('unsupported management operation')
        try:
            response = await self.client.request(method, '/api/v1/management' + suffix,
                json=payload, headers={'Authorization':'Bearer ' + token})
        except httpx.HTTPError:
            raise CoreUnavailable('core_unreachable') from None
        if response.status_code != 200:
            raise CoreUnavailable('configuration_apply_rejected' if response.status_code==422 else 'management_http_error', response.status_code)
        try:
            result = response.json()
            if not isinstance(result, dict):raise ValueError()
        except ValueError:
            raise CoreUnavailable('management_invalid_response') from None
        return result

    async def status(self):
        try:
            health = await self.read('/health')
            if health.get('service') != 'pero' or health.get('ok') is not True:
                return {'reachable': True, 'ready': False, 'state': 'unexpected_service'}
        except CoreUnavailable as exc:
            return {'reachable': exc.upstream_status is not None, 'ready': False, 'state': exc.code,
                    'upstream_status': exc.upstream_status}
        core = {'reachable': True, 'draining':bool(health.get('draining',False)), 'state': 'http_ready', 'version': health.get('version'),
                'loop_ready': bool(health.get('loop_ready')),
                'model_configured': bool(health.get('model_configured')),
                'ready': bool(health.get('loop_ready') and health.get('model_configured')),
                'readiness_basis': 'loop_and_credential_presence_only',
                'model_remote_connectivity': 'not_verified', 'process_state': 'unknown'}
        bridge = {'connected': None, 'state': 'unknown'}
        try:
            channels = await self.read('/api/v1/channels')
            items = channels.get('channels')
            if not isinstance(items, list) or any(not isinstance(c, dict) for c in items):
                raise CoreUnavailable('core_invalid_response', 200)
            bridge = {'connected': any(c.get('connected') is True for c in items),
                      'state': 'connected' if any(c.get('connected') is True for c in items)
                               else 'not_connected',
                      'channels': [{k: c.get(k) for k in ('channel_id', 'enabled', 'listening', 'connected', 'connections')}
                                   for c in items]}
        except CoreUnavailable as exc:
            bridge = {'connected': None, 'state': exc.code}
        return {'pero': core, 'bridge': bridge}
