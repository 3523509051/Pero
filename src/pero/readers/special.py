"""Bilibili public HTML and WeChat #js_content readers.

Adapted from bridge/safe_browser.py::TextParser and
bridge/wechat_article.py::_ArticleParser/read_wechat_article. No browser,
login, video download, subtitle extraction or additional model call.
"""
from __future__ import annotations

import asyncio
import re
from html.parser import HTMLParser
from urllib.parse import urljoin, urlsplit

import httpx

from pero.readers.urls import route_reader, validate_public_url


class _ReadError(Exception):
    def __init__(self, code):
        self.code = code


_HIDDEN = {'script', 'style', 'noscript', 'iframe', 'svg', 'template'}
_VOID = {'area', 'base', 'br', 'col', 'embed', 'hr', 'img', 'input', 'link', 'meta', 'param', 'source', 'track', 'wbr'}
_BLOCKS = {'p', 'div', 'br', 'li', 'h1', 'h2', 'h3', 'section', 'article', 'main'}


def _clean(text):
    text = re.sub(r'[ \t\u00a0]+', ' ', text)
    return re.sub(r'\n{3,}', '\n\n', text).strip()


class _DocumentParser(HTMLParser):
    """Text-only extraction; scripts/styles/media never become readable text."""
    def __init__(self, wechat=False):
        super().__init__(convert_charrefs=True)
        self.wechat = wechat
        self.stack = []
        self.parts = []
        self.article_parts = []
        self.main_parts = []
        self.wechat_parts = []
        self.title_parts = []
        self.wechat_title_parts = []
        self.content_found = False

    def _flags(self):
        hidden = any(item[1] for item in self.stack)
        return hidden, any(item[2] for item in self.stack), any(item[3] for item in self.stack), any(item[4] for item in self.stack)

    def _append(self, value):
        hidden, article, main, wechat = self._flags()
        if hidden:
            return
        self.parts.append(value)
        if article:
            self.article_parts.append(value)
        if main:
            self.main_parts.append(value)
        if wechat:
            self.wechat_parts.append(value)

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        is_wechat = attrs.get('id') == 'js_content'
        self.content_found = self.content_found or is_wechat
        if tag not in _VOID:
            self.stack.append((tag, tag in _HIDDEN, tag == 'article', tag == 'main', is_wechat,
                               tag == 'h1' and 'rich_media_title' in (attrs.get('class') or '').split()))
        if tag in _BLOCKS:
            self._append('\n')

    def handle_startendtag(self, tag, attrs):
        self.handle_starttag(tag, attrs)
        if tag not in _VOID:
            self.handle_endtag(tag)

    def handle_endtag(self, tag):
        if tag in _BLOCKS:
            self._append('\n')
        for index in range(len(self.stack)-1, -1, -1):
            if self.stack[index][0] == tag:
                del self.stack[index:]
                break

    def handle_data(self, data):
        hidden = self._flags()[0]
        if hidden:
            return
        if any(item[0] == 'title' for item in self.stack):
            self.title_parts.append(data)
        if any(item[5] for item in self.stack):
            self.wechat_title_parts.append(data)
        self._append(data)

    def result(self):
        title = re.sub(r'\s+', ' ', ''.join(self.wechat_title_parts or self.title_parts)).strip()
        if self.wechat:
            content = _clean(''.join(self.wechat_parts))
        else:
            content = _clean(''.join(self.article_parts or self.main_parts or self.parts))
        return title, content


class SpecialReaders:
    """A bounded HTTP-only reader for the two retained sites.

    `max_response_bytes` limits decoded HTML transfer, not final text. Full responses are
    never persisted. Final title/content and error codes are locally bounded.
    """
    def __init__(self, *, max_chars=1800, max_response_bytes=524288, timeout=8.0):
        self._max_bytes = max(int(max_response_bytes), 1024)
        self._max_chars = max(int(max_chars), 0)
        self._timeout = min(max(float(timeout), 1.0), 20.0)
        self._client = None

    def _http(self):
        if self._client is None:
            self._client = httpx.AsyncClient(
                timeout=self._timeout, follow_redirects=False, trust_env=False,
                limits=httpx.Limits(max_connections=4, max_keepalive_connections=2))
        return self._client

    async def aclose(self):
        client, self._client = self._client, None
        if client is not None:
            await client.aclose()

    async def _fetch(self, url, backend):
        current = url
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,text/plain;q=0.8',
            'Accept-Language': 'zh-CN,zh;q=0.9',
        }
        if backend == 'wechat':
            headers['Referer'] = 'https://mp.weixin.qq.com/'
        for _ in range(5):
            current = await validate_public_url(current)
            if route_reader(current) != backend:
                raise _ReadError('unsupported_redirect')
            async with self._http().stream('GET', current, headers=headers) as response:
                if response.status_code in {301, 302, 303, 307, 308}:
                    location = response.headers.get('location')
                    if not location:
                        raise _ReadError('redirect_missing_location')
                    current = urljoin(current, location)
                    # Each target is checked before any request, not only after redirection.
                    continue
                if response.status_code != 200:
                    raise _ReadError(f'http_{response.status_code}')
                mime = response.headers.get('content-type', '').split(';', 1)[0].strip().lower()
                if mime not in {'text/html', 'application/xhtml+xml', 'text/plain'}:
                    raise _ReadError('unsupported_content_type')
                if response.headers.get('content-disposition', '').lower().startswith('attachment'):
                    raise _ReadError('attachment_not_allowed')
                declared = response.headers.get('content-length', '')
                if declared.isdigit() and int(declared) > self._max_bytes:
                    raise _ReadError('response_too_large')
                chunks, size = [], 0
                async for chunk in response.aiter_bytes(chunk_size=16384):
                    size += len(chunk)
                    if size > self._max_bytes:
                        raise _ReadError('response_too_large')
                    chunks.append(chunk)
                match = re.search(r'charset=["\']?([\w-]+)', response.headers.get('content-type', ''), re.I)
                encoding = match.group(1) if match else 'utf-8'
                try:
                    page = b''.join(chunks).decode(encoding, errors='replace')
                except LookupError:
                    page = b''.join(chunks).decode('utf-8', errors='replace')
                return current, page, mime
        raise _ReadError('too_many_redirects')

    async def read(self, url: str) -> dict:
        try:
            backend = route_reader(url)
        except (ValueError, TypeError):
            return {'ok': False, 'backend': 'unknown', 'url': '', 'title': '', 'content': '', 'truncated': False, 'error_code': 'invalid_url'}
        result = {'ok': False, 'backend': backend, 'url': url, 'title': '', 'content': '', 'truncated': False}
        if backend not in {'bilibili', 'wechat'}:
            result['error_code'] = 'unsupported_site'
            return result
        try:
            final, page, mime = await asyncio.wait_for(self._fetch(url, backend), timeout=self._timeout)
            result['url'] = final
            if backend == 'wechat' and ('环境异常' in page or 'wappoc_appmsgcaptcha' in urlsplit(final).path):
                raise _ReadError('wechat_verification_required')
            parser = _DocumentParser(wechat=backend == 'wechat')
            parser.feed(page)
            title, content = parser.result()
            result['title'] = title[:300]
            if backend == 'wechat' and not parser.content_found:
                raise _ReadError('wechat_body_unavailable')
            if mime == 'text/plain' and backend == 'bilibili':
                content = page.strip()
            if not content:
                raise _ReadError('body_unavailable')
            if backend == 'bilibili' and len(content) < 20:
                # A title alone is not a video/article transcript or readable page.
                raise _ReadError('body_unavailable')
            result.update(ok=True, content=content if self._max_chars == 0 else content[:self._max_chars],
                          truncated=self._max_chars > 0 and len(content)>self._max_chars)
        except asyncio.CancelledError:
            raise
        except _ReadError as exc:
            result['error_code'] = exc.code
        except (asyncio.TimeoutError, httpx.TimeoutException):
            result['error_code'] = 'timeout'
        except httpx.HTTPError:
            result['error_code'] = 'transport_error'
        except ValueError:
            result['error_code'] = 'invalid_or_nonpublic_url'
        except Exception:
            result['error_code'] = 'reader_error'
        return result
