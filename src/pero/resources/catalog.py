"""Scoped original-byte catalog. Never parses document content or stores extracted text."""
from __future__ import annotations

import asyncio
import hashlib
import math
import os
import shutil
import re
import sqlite3
import tempfile
import threading
import time
import uuid
from pathlib import Path


#: 已知二进制文档（Files API 按文档解析；内容是容器/二进制，不做文本嗅探）。
BINARY_DOC_EXTENSIONS = frozenset({
    '.pdf', '.doc', '.docx', '.ppt', '.pptx', '.xls', '.xlsx',
    # 电子书：Plain Text / Spreadsheets / Ebooks 走 user_data，见 files.py。
    '.epub', '.mobi',
})

#: 文本/源码后缀：允许接收，但必须通过有界内容嗅探（防二进制改名 .txt）。
TEXTUAL_EXTENSIONS = frozenset({
    '.md', '.markdown', '.txt', '.text', '.rst', '.c', '.h', '.cc', '.cpp',
    '.cxx', '.hpp', '.hh', '.hxx', '.py', '.pyi', '.js', '.jsx', '.ts', '.tsx',
    '.java', '.kt', '.go', '.rs', '.cs', '.php', '.rb', '.swift', '.lua',
    '.json', '.jsonc', '.yaml', '.yml', '.toml', '.ini', '.cfg', '.conf',
    '.xml', '.html', '.htm', '.css', '.scss', '.less', '.sql', '.csv', '.tsv',
    '.log', '.tex', '.cmake', '.mk',
    # 文本脚本只作为数据读取，绝不执行。
    '.sh', '.bash', '.ps1', '.bat', '.cmd', '.zsh', '.fish',
    '.diff', '.patch', '.env', '.properties', '.gradle', '.makefile', '.gitignore',
})

#: 二进制附件（压缩包/可执行/未知二进制容器）：只登记原件，不解析、不执行、
#: 不解压；供应商不支持读取时由 API 受控报错。不再因扩展名提前拒收。
BINARY_ATTACHMENT_EXTENSIONS = frozenset({
    '.zip', '.rar', '.7z', '.tar', '.gz', '.tgz', '.bz2', '.xz', '.zst',
    '.lz4', '.cab', '.iso', '.apk', '.jar', '.war',
    '.exe', '.com', '.msi', '.dll', '.so', '.dylib', '.scr', '.bin', '.dat',
})

ALLOWED_EXTENSIONS = (
    BINARY_DOC_EXTENSIONS | TEXTUAL_EXTENSIONS | BINARY_ATTACHMENT_EXTENSIONS
)

#: 文本嗅探读取上限（有界；不为识别类型读全文）。
TEXT_SNIFF_BYTES = 8192

#: 允许图片原件写入/复用 Files 云映射的唯一用途：明确按文档解析图片。
#: 直接视觉输入仍走既有图片流程，不因本白名单自动切换到 Files。
IMAGE_FILE_API_PURPOSES = frozenset({'text_recognition', 'rich_layout_recognition'})


def looks_like_textual(data) -> bool:
    """有界判定内容是否像文本：UTF-8 严格优先，其次 GB18030 且控制字符极少。

    只读前 `TEXT_SNIFF_BYTES` 字节；含 NUL 直接判否（二进制容器/可执行文件）。
    允许"读取上限截断多字节字符"造成的尾部解码错误，不把正常中文误判。
    """

    if not isinstance(data, (bytes, bytearray)) or not data:
        return False
    head = bytes(data[:TEXT_SNIFF_BYTES])
    if b'\x00' in head:
        return False
    try:
        head.decode('utf-8')
        return True
    except UnicodeDecodeError as exc:
        if exc.start >= len(head) - 3:
            try:
                head[:exc.start].decode('utf-8')
                return True
            except UnicodeDecodeError:
                pass
    try:
        text = head.decode('gb18030')
    except UnicodeDecodeError:
        return False
    control = sum(1 for ch in text if ord(ch) < 32 and ch not in '\t\r\n')
    printable = sum(1 for ch in text if ch.isprintable() or ch in '\t\r\n')
    return printable >= max(1, int(len(text) * 0.95)) and control <= 1
IMAGE_EXTENSIONS = {
    '.jpg': 'image/jpeg', '.jpeg': 'image/jpeg', '.png': 'image/png',
    '.gif': 'image/gif', '.webp': 'image/webp',
    # BMP/TIFF 不进直接视觉路径，但可作为文件按文档解析读取。
    '.bmp': 'image/bmp', '.tif': 'image/tiff', '.tiff': 'image/tiff',
}


def sniff_image_mime(data):
    if data.startswith(b'\xff\xd8\xff'):
        return 'image/jpeg'
    if data.startswith(b'\x89PNG\r\n\x1a\n'):
        return 'image/png'
    if data[:6] in {b'GIF87a', b'GIF89a'}:
        return 'image/gif'
    if len(data) >= 12 and data[:4] == b'RIFF' and data[8:12] == b'WEBP':
        return 'image/webp'
    if data.startswith(b'BM'):
        return 'image/bmp'
    if data[:4] in {b'II*\x00', b'MM\x00*'}:
        return 'image/tiff'
    return None


image_mime = sniff_image_mime


_BAD_NAME = re.compile(r'[\\/:*?"<>|\x00-\x1f\x7f]')
_RESERVED = frozenset({'CON', 'PRN', 'AUX', 'NUL'} | {f'{prefix}{i}' for prefix in ('COM', 'LPT') for i in range(1, 10)})


class ResourceCatalogError(RuntimeError):
    def __init__(self, code):
        self.code = code
        super().__init__(code)


def _bounded_int(value, low, high, code):
    if isinstance(value, bool) or not isinstance(value, int) or not low <= value <= high:
        raise ResourceCatalogError(code)
    return value


def _scope(agent_id, conversation_id):
    if not isinstance(agent_id, str) or not agent_id.strip() or len(agent_id) > 128:
        raise ResourceCatalogError('invalid_agent')
    if not isinstance(conversation_id, str) or len(conversation_id) > 1024:
        raise ResourceCatalogError('invalid_conversation')
    parts = conversation_id.split(':')
    http_scope = len(parts) == 3 and parts[0] == 'http' and parts[1] == agent_id
    channel_scope = len(parts) == 4 and parts[2] in {'group', 'private'}
    if (not (http_scope or channel_scope)
        or any(not part or part != part.strip() for part in parts)):
        raise ResourceCatalogError('invalid_conversation')
    if any(ord(char) < 32 for char in agent_id + conversation_id):
        raise ResourceCatalogError('invalid_scope')
    return agent_id, conversation_id


def _name(name, *, allow_images=False, allow_textual=False):
    if (not isinstance(name, str) or not name or name != name.strip() or len(name) > 180
        or name.startswith('.') or name.endswith('.') or _BAD_NAME.search(name)
        or name.split('.', 1)[0].rstrip(' .').upper() in _RESERVED):
        raise ResourceCatalogError('invalid_filename')
    suffix = Path(name).suffix.lower()
    known = ALLOWED_EXTENSIONS | (set(IMAGE_EXTENSIONS) if allow_images else set())
    if suffix in known:
        return name
    if allow_textual:
        # 无扩展名/未知后缀：不在文件名阶段拒绝，交给 ingest 的有界内容嗅探。
        return name
    raise ResourceCatalogError('unsupported_file_type')


def _label(value, code):
    if not isinstance(value, str) or not value or len(value) > 128 or any(ord(char) < 32 for char in value):
        raise ResourceCatalogError(code)
    return value


class ResourceCatalog:
    """One process owns the catalog; all SQLite/blob IO runs in to_thread under one lock.

    Constructor has no IO. The first operation creates a separate database and blob
    directory. Immutable resource versions are scoped by Agent and stable conversation.
    """
    def __init__(self, root: Path, max_bytes=104_857_600, max_total_bytes=None, *, allow_images=False, max_image_bytes=10_000_000):
        self._root = Path(root)
        self._max_bytes = _bounded_int(max_bytes, 1, 536_870_912, 'invalid_file_limit')
        # None = 不设项目配额；仍受磁盘容量与明确 IO 错误约束。
        self._max_total_bytes = (
            None
            if max_total_bytes is None
            else _bounded_int(int(max_total_bytes), self._max_bytes, 10_000_000_000, 'invalid_total_limit')
        )
        self._allow_images = bool(allow_images)
        self._max_image_bytes = _bounded_int(max_image_bytes, 1, 100_000_000, 'invalid_image_limit')
        self._lock = threading.RLock()
        self._conn = None
        self._closed = False

    @property
    def initialized(self):
        return self._conn is not None

    def _open(self):
        if self._closed:
            raise ResourceCatalogError('catalog_closed')
        if self._conn is not None:
            return self._conn
        self._root.mkdir(parents=True, exist_ok=True)
        blobs = self._root / 'blobs'
        blobs.mkdir(exist_ok=True)
        if blobs.is_symlink():
            raise ResourceCatalogError('invalid_blob_directory')
        db = self._root / 'catalog.sqlite3'
        if db.is_symlink():
            raise ResourceCatalogError('invalid_catalog_file')
        conn = sqlite3.connect(str(db), check_same_thread=False, timeout=5)
        conn.row_factory = sqlite3.Row
        try:
            conn.execute('PRAGMA journal_mode=WAL')
            conn.execute('PRAGMA foreign_keys=ON')
            conn.execute('PRAGMA synchronous=FULL')
            version = conn.execute('PRAGMA user_version').fetchone()[0]
            if version not in {0, 1, 2}:
                raise ResourceCatalogError('unsupported_catalog_schema')
            conn.execute('BEGIN IMMEDIATE')
            conn.execute("""CREATE TABLE IF NOT EXISTS resource (
                resource_id TEXT PRIMARY KEY, agent_id TEXT NOT NULL, conversation_id TEXT NOT NULL,
                name TEXT NOT NULL, sha256 TEXT NOT NULL, size INTEGER NOT NULL, version INTEGER NOT NULL,
                kind TEXT NOT NULL, blob TEXT NOT NULL, event_id TEXT NOT NULL, created_at REAL NOT NULL,
                UNIQUE(agent_id,conversation_id,name,version))""")
            conn.execute("""CREATE INDEX IF NOT EXISTS resource_scope ON resource(agent_id,conversation_id,created_at)""")
            conn.execute("""CREATE TABLE IF NOT EXISTS resource_event (
                agent_id TEXT NOT NULL, conversation_id TEXT NOT NULL, event_id TEXT NOT NULL,
                resource_id TEXT NOT NULL REFERENCES resource(resource_id),
                PRIMARY KEY(agent_id,conversation_id,event_id))""")
            conn.execute("""CREATE TABLE IF NOT EXISTS cloud_mapping (
                resource_id TEXT NOT NULL REFERENCES resource(resource_id), version INTEGER NOT NULL,
                sha256 TEXT NOT NULL, purpose TEXT NOT NULL, provider_namespace TEXT NOT NULL,
                file_id TEXT NOT NULL, expires_at REAL NOT NULL, created_at REAL NOT NULL,
                PRIMARY KEY(resource_id,version,sha256,purpose,provider_namespace))""")
            columns = {row[1] for row in conn.execute('PRAGMA table_info(resource)')}
            if 'mime' not in columns:
                conn.execute('ALTER TABLE resource ADD COLUMN mime TEXT')
            # R02 增量迁移：provider_bytes 是供应商对象大小（转换后），
            # 与原件的 size 语义不同；旧记录为 NULL 仍可正常复用。
            mapping_columns = {row[1] for row in conn.execute('PRAGMA table_info(cloud_mapping)')}
            if 'provider_bytes' not in mapping_columns:
                conn.execute('ALTER TABLE cloud_mapping ADD COLUMN provider_bytes INTEGER')
            conn.execute("""CREATE TABLE IF NOT EXISTS image_source (
                agent_id TEXT NOT NULL, conversation_id TEXT NOT NULL, event_id TEXT NOT NULL,
                resource_id TEXT NOT NULL REFERENCES resource(resource_id), sender_ref TEXT NOT NULL,
                observed_at REAL NOT NULL,
                PRIMARY KEY(agent_id,conversation_id,event_id,resource_id))""")
            conn.execute('CREATE INDEX IF NOT EXISTS image_source_recent ON image_source(agent_id,conversation_id,observed_at DESC)')
            conn.execute('PRAGMA user_version=2')
            conn.commit()
        except BaseException:
            conn.rollback()
            conn.close()
            raise
        self._conn = conn
        return conn

    async def _run(self, method, *args, **kwargs):
        def execute():
            with self._lock:
                try:
                    return method(*args, **kwargs)
                except ResourceCatalogError:
                    raise
                except (OSError, sqlite3.Error) as exc:
                    if self._conn is not None:
                        self._conn.rollback()
                    raise ResourceCatalogError('catalog_io_error') from exc
        return await asyncio.to_thread(execute)

    @staticmethod
    def _metadata(row, *, duplicate=False):
        return {key: row[key] for key in ('resource_id', 'name', 'sha256', 'size', 'version', 'kind', 'mime', 'created_at')} | {'duplicate': duplicate}

    def _row(self, agent_id, conversation_id, resource_id):
        _scope(agent_id, conversation_id)
        if not isinstance(resource_id, str) or not re.fullmatch(r'r-[0-9a-f]{32}', resource_id):
            raise ResourceCatalogError('invalid_resource_id')
        return self._open().execute('SELECT * FROM resource WHERE agent_id=? AND conversation_id=? AND resource_id=?',
                                    (agent_id, conversation_id, resource_id)).fetchone()

    def _blob_path(self, row):
        if not re.fullmatch(r'[0-9a-f]{32}\.blob', row['blob']):
            raise ResourceCatalogError('invalid_blob_reference')
        path = self._root / 'blobs' / row['blob']
        if path.is_symlink() or path.resolve().parent != (self._root / 'blobs').resolve():
            raise ResourceCatalogError('invalid_blob_reference')
        return path

    def _read(self, row):
        path = self._blob_path(row)
        cap = self._max_image_bytes if row['kind'] == 'image' else self._max_bytes
        if not path.is_file() or path.stat().st_size != row['size'] or row['size'] > cap:
            raise ResourceCatalogError('resource_integrity_error')
        with path.open('rb') as stream:
            data = stream.read(cap + 1)
        if len(data) != row['size'] or hashlib.sha256(data).hexdigest() != row['sha256']:
            raise ResourceCatalogError('resource_integrity_error')
        return data

    async def ingest(self, *, agent_id, conversation_id, name, data: bytes, event_id=''):
        _scope(agent_id, conversation_id)
        _name(name, allow_images=self._allow_images, allow_textual=True)
        if not isinstance(data, bytes) or not data:
            raise ResourceCatalogError('empty_or_invalid_bytes')
        mime = sniff_image_mime(data)
        image_extension = IMAGE_EXTENSIONS.get(Path(name).suffix.lower())
        if mime or image_extension:
            if not self._allow_images:
                raise ResourceCatalogError('images_disabled')
            if mime is None or mime != image_extension:
                raise ResourceCatalogError('image_type_mismatch')
            kind, cap = 'image', self._max_image_bytes
        else:
            # 已知二进制文档（PDF/Office/电子书）按容器接收；其余后缀——包括
            # 文本/源码后缀与无扩展名/未知后缀——都必须通过有界文本嗅探，
            # 防止把二进制改名 .txt 放行。
            if Path(name).suffix.lower() not in (
                BINARY_DOC_EXTENSIONS | BINARY_ATTACHMENT_EXTENSIONS
            ) and not looks_like_textual(data):
                raise ResourceCatalogError('content_not_textual')
            kind, cap = 'document', self._max_bytes
        if len(data) > cap:
            raise ResourceCatalogError('image_too_large' if kind == 'image' else 'file_too_large')
        if not isinstance(event_id, str) or len(event_id) > 512 or any(ord(char) < 32 for char in event_id):
            raise ResourceCatalogError('invalid_event_id')
        return await self._run(self._ingest, agent_id, conversation_id, name, data, event_id, kind, mime)

    def _ingest(self, agent_id, conversation_id, name, data, event_id, kind, mime):
        conn = self._open()
        digest = hashlib.sha256(data).hexdigest()
        if event_id:
            existing = conn.execute('SELECT r.* FROM resource_event e JOIN resource r ON r.resource_id=e.resource_id '
                'WHERE e.agent_id=? AND e.conversation_id=? AND e.event_id=?', (agent_id, conversation_id, event_id)).fetchone()
            if existing is not None:
                if existing['name'] != name or existing['sha256'] != digest:
                    raise ResourceCatalogError('source_conflict')
                self._read(existing)
                return self._metadata(existing, duplicate=True)
        existing = conn.execute('SELECT * FROM resource WHERE agent_id=? AND conversation_id=? AND name=? AND sha256=? '
            'ORDER BY version DESC LIMIT 1', (agent_id, conversation_id, name, digest)).fetchone()
        if existing is not None:
            self._read(existing)
            if event_id:
                conn.execute('INSERT INTO resource_event VALUES(?,?,?,?)', (agent_id, conversation_id, event_id, existing['resource_id']))
                conn.commit()
            return self._metadata(existing, duplicate=True)
        # Include orphaned blobs after interrupted writes in quota; never delete unknown originals.
        if self._max_total_bytes is not None:
            total = sum(path.stat().st_size for path in (self._root / 'blobs').iterdir() if path.is_file())
            if total + len(data) > self._max_total_bytes:
                raise ResourceCatalogError('storage_quota_exceeded')
        version = conn.execute('SELECT COALESCE(MAX(version),0)+1 FROM resource WHERE agent_id=? AND conversation_id=? AND name=?',
                               (agent_id, conversation_id, name)).fetchone()[0]
        resource_id = 'r-' + uuid.uuid4().hex
        blob = uuid.uuid4().hex + '.blob'
        target = self._root / 'blobs' / blob
        descriptor, temporary = tempfile.mkstemp(prefix='upload-', suffix='.tmp', dir=self._root / 'blobs')
        try:
            with os.fdopen(descriptor, 'wb') as stream:
                stream.write(data)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, target)
            conn.execute('INSERT INTO resource(resource_id,agent_id,conversation_id,name,sha256,size,version,kind,blob,event_id,created_at,mime) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)',
                         (resource_id, agent_id, conversation_id, name, digest, len(data), version, kind, blob, event_id, time.time(), mime))
            if event_id:
                conn.execute('INSERT INTO resource_event VALUES(?,?,?,?)', (agent_id, conversation_id, event_id, resource_id))
            conn.commit()
        finally:
            # Only our uncommitted temporary file; original/version blobs are never deleted.
            if os.path.exists(temporary):
                os.unlink(temporary)
        row = self._row(agent_id, conversation_id, resource_id)
        return self._metadata(row)

    async def ingest_file(
        self, *, agent_id, conversation_id, name, source_path, sha256, size, event_id=''
    ):
        """从**已落盘的临时文件**登记原件（分块补传用，不把整文件读进内存）。

        调用方负责逐块写临时文件并累计 SHA256；这里做有界校验
        （size / sha256 / 类型 / 内容嗅探 / 配额）后分块复制进 blobs，
        源临时文件由调用方清理。任何校验失败都不留下可读记录。
        """

        _scope(agent_id, conversation_id)
        _name(name, allow_images=self._allow_images, allow_textual=True)
        digest = str(sha256 or '').strip().lower()
        if len(digest) != 64 or any(char not in '0123456789abcdef' for char in digest):
            raise ResourceCatalogError('invalid_sha256')
        if isinstance(size, bool) or not isinstance(size, int) or size <= 0:
            raise ResourceCatalogError('invalid_size')
        source = Path(source_path)

        def work():
            try:
                actual_size = source.stat().st_size
            except OSError:
                raise ResourceCatalogError('source_unreadable') from None
            if actual_size != size:
                raise ResourceCatalogError('source_size_mismatch')
            try:
                with source.open('rb') as stream:
                    magic = stream.read(64)
            except OSError:
                raise ResourceCatalogError('source_unreadable') from None
            mime = sniff_image_mime(magic)
            image_extension = IMAGE_EXTENSIONS.get(Path(name).suffix.lower())
            if mime or image_extension:
                if not self._allow_images:
                    raise ResourceCatalogError('images_disabled')
                if mime is None or mime != image_extension:
                    raise ResourceCatalogError('image_type_mismatch')
                kind, cap = 'image', self._max_image_bytes
            else:
                if Path(name).suffix.lower() not in (
                    BINARY_DOC_EXTENSIONS | BINARY_ATTACHMENT_EXTENSIONS
                ):
                    try:
                        with source.open('rb') as stream:
                            probe = stream.read(TEXT_SNIFF_BYTES)
                    except OSError:
                        raise ResourceCatalogError('source_unreadable') from None
                    if not looks_like_textual(probe):
                        raise ResourceCatalogError('content_not_textual')
                kind, cap = 'document', self._max_bytes
            if size > cap:
                raise ResourceCatalogError(
                    'image_too_large' if kind == 'image' else 'file_too_large'
                )
            return self._register_from_path(
                agent_id, conversation_id, name, source, digest, size,
                event_id, kind, mime,
            )

        return await self._run(work)

    def _register_from_path(
        self, agent_id, conversation_id, name, source, digest, size,
        event_id, kind, mime,
    ):
        conn = self._open()
        if event_id:
            existing = conn.execute(
                'SELECT r.* FROM resource_event e JOIN resource r ON r.resource_id=e.resource_id '
                'WHERE e.agent_id=? AND e.conversation_id=? AND e.event_id=?',
                (agent_id, conversation_id, event_id)).fetchone()
            if existing is not None:
                if existing['name'] != name or existing['sha256'] != digest:
                    raise ResourceCatalogError('source_conflict')
                self._verify_stream(existing)
                return self._metadata(existing, duplicate=True)
        existing = conn.execute(
            'SELECT * FROM resource WHERE agent_id=? AND conversation_id=? AND name=? AND sha256=? '
            'ORDER BY version DESC LIMIT 1',
            (agent_id, conversation_id, name, digest)).fetchone()
        if existing is not None:
            self._verify_stream(existing)
            if event_id:
                conn.execute(
                    'INSERT INTO resource_event VALUES(?,?,?,?)',
                    (agent_id, conversation_id, event_id, existing['resource_id']))
                conn.commit()
            return self._metadata(existing, duplicate=True)
        if self._max_total_bytes is not None:
            total = sum(
                path.stat().st_size
                for path in (self._root / 'blobs').iterdir()
                if path.is_file()
            )
            if total + size > self._max_total_bytes:
                raise ResourceCatalogError('storage_quota_exceeded')
        version = conn.execute(
            'SELECT COALESCE(MAX(version),0)+1 FROM resource WHERE agent_id=? AND conversation_id=? AND name=?',
            (agent_id, conversation_id, name)).fetchone()[0]
        resource_id = 'r-' + uuid.uuid4().hex
        blob = uuid.uuid4().hex + '.blob'
        target = self._root / 'blobs' / blob
        descriptor, temporary = tempfile.mkstemp(
            prefix='upload-', suffix='.tmp', dir=self._root / 'blobs')
        try:
            with os.fdopen(descriptor, 'wb') as out, source.open('rb') as stream:
                shutil.copyfileobj(stream, out, 1024 * 1024)
                out.flush()
                os.fsync(out.fileno())
            os.replace(temporary, target)
            conn.execute(
                'INSERT INTO resource(resource_id,agent_id,conversation_id,name,sha256,size,version,kind,blob,event_id,created_at,mime) '
                'VALUES(?,?,?,?,?,?,?,?,?,?,?,?)',
                (resource_id, agent_id, conversation_id, name, digest, size,
                 version, kind, blob, event_id, time.time(), mime))
            if event_id:
                conn.execute(
                    'INSERT INTO resource_event VALUES(?,?,?,?)',
                    (agent_id, conversation_id, event_id, resource_id))
            conn.commit()
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)
        return self._metadata(self._row(agent_id, conversation_id, resource_id))

    def _verify_stream(self, row, chunk=1024 * 1024):
        """分块校验原件（不整文件进内存），返回可读路径。"""

        path = self._blob_path(row)
        cap = self._max_image_bytes if row['kind'] == 'image' else self._max_bytes
        try:
            if not path.is_file() or path.stat().st_size != row['size'] or row['size'] > cap:
                raise ResourceCatalogError('resource_integrity_error')
        except OSError:
            raise ResourceCatalogError('resource_integrity_error') from None
        digest = hashlib.sha256()
        total = 0
        with path.open('rb') as stream:
            while True:
                block = stream.read(chunk)
                if not block:
                    break
                total += len(block)
                if total > cap:
                    raise ResourceCatalogError('resource_integrity_error')
                digest.update(block)
        if total != row['size'] or digest.hexdigest() != row['sha256']:
            raise ResourceCatalogError('resource_integrity_error')
        return path

    async def open_blob(self, *, agent_id, conversation_id, resource_id):
        """校验后返回原件路径（内部使用，供流式上传；不得回显给调用方/前端）。"""

        def work():
            row = self._row(agent_id, conversation_id, resource_id)
            if row is None:
                raise ResourceCatalogError('resource_not_found')
            return self._verify_stream(row)

        return await self._run(work)

    async def get(self, *, agent_id, conversation_id, resource_id):
        def get():
            row = self._row(agent_id, conversation_id, resource_id)
            return self._metadata(row) if row is not None else None
        return await self._run(get)

    async def list(self, *, agent_id, conversation_id, limit=50, offset=0, kind=None):
        _scope(agent_id, conversation_id)
        _bounded_int(limit, 1, 100, 'invalid_limit')
        _bounded_int(offset, 0, 1_000_000, 'invalid_offset')
        if kind not in (None,'','image','document'):raise ResourceCatalogError('invalid_resource_kind')
        def listing():
            conn = self._open()
            where = (agent_id, conversation_id,kind or '',kind or '')
            rows = conn.execute("SELECT * FROM resource WHERE agent_id=? AND conversation_id=? AND (?='' OR kind=?) ORDER BY created_at DESC,resource_id DESC LIMIT ? OFFSET ?",
                                (*where, limit, offset)).fetchall()
            total = conn.execute("SELECT COUNT(*) FROM resource WHERE agent_id=? AND conversation_id=? AND (?='' OR kind=?)", where).fetchone()[0]
            return {'items': [self._metadata(row) for row in rows], 'total': total, 'limit': limit, 'offset': offset}
        return await self._run(listing)

    async def read_bytes(self, *, agent_id, conversation_id, resource_id):
        def read():
            row = self._row(agent_id, conversation_id, resource_id)
            if row is None:
                raise ResourceCatalogError('resource_not_found')
            return self._read(row)
        return await self._run(read)

    async def lookup_mapping(self, *, agent_id, conversation_id, resource_id, purpose, provider_namespace):
        _label(purpose, 'invalid_purpose')
        _label(provider_namespace, 'invalid_provider_namespace')
        def lookup():
            row = self._row(agent_id, conversation_id, resource_id)
            if row is None:
                raise ResourceCatalogError('resource_not_found')
            if row['kind'] == 'image' and purpose not in IMAGE_FILE_API_PURPOSES:
                raise ResourceCatalogError('image_not_file_api')
            # No cloud reuse if local bytes no longer match immutable version.
            # 分块校验：大文件不整份读进内存。
            self._verify_stream(row)
            mapping = self._open().execute('SELECT resource_id,version,sha256,purpose,provider_namespace,file_id,expires_at,provider_bytes FROM cloud_mapping '
                'WHERE resource_id=? AND version=? AND sha256=? AND purpose=? AND provider_namespace=?',
                (resource_id, row['version'], row['sha256'], purpose, provider_namespace)).fetchone()
            if mapping is None or mapping['expires_at'] <= time.time():
                return None
            return dict(mapping)
        return await self._run(lookup)

    async def cache_mapping(self, *, agent_id, conversation_id, resource_id, purpose, provider_namespace, file_id, expires_at, provider_bytes=None):
        _label(purpose, 'invalid_purpose')
        _label(provider_namespace, 'invalid_provider_namespace')
        if not isinstance(file_id, str) or not file_id or len(file_id) > 512 or any(ord(char) < 32 for char in file_id):
            raise ResourceCatalogError('invalid_file_id')
        if isinstance(expires_at, bool) or not isinstance(expires_at, (int, float)) or not math.isfinite(expires_at) or expires_at <= time.time():
            raise ResourceCatalogError('invalid_mapping_expiry')
        # provider_bytes 是供应商对象大小（可能因转换而变化），只要求类型/非负；
        # 不与原件 size 比较，也不能证明正文非空。
        if provider_bytes is not None:
            if isinstance(provider_bytes, bool) or not isinstance(provider_bytes, int) or provider_bytes < 0:
                raise ResourceCatalogError('invalid_provider_bytes')
        def cache():
            row = self._row(agent_id, conversation_id, resource_id)
            if row is None:
                raise ResourceCatalogError('resource_not_found')
            if row['kind'] == 'image' and purpose not in IMAGE_FILE_API_PURPOSES:
                raise ResourceCatalogError('image_not_file_api')
            self._verify_stream(row)
            conn = self._open()
            conn.execute('INSERT INTO cloud_mapping(resource_id,version,sha256,purpose,provider_namespace,file_id,expires_at,created_at,provider_bytes) '
                'VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(resource_id,version,sha256,purpose,provider_namespace) '
                'DO UPDATE SET file_id=excluded.file_id,expires_at=excluded.expires_at,created_at=excluded.created_at,'
                'provider_bytes=excluded.provider_bytes',
                (resource_id, row['version'], row['sha256'], purpose, provider_namespace, file_id, float(expires_at), time.time(), provider_bytes))
            conn.commit()
            return {'resource_id': resource_id, 'version': row['version'], 'sha256': row['sha256'],
                    'purpose': purpose, 'provider_namespace': provider_namespace, 'file_id': file_id,
                    'expires_at': float(expires_at), 'provider_bytes': provider_bytes}
        return await self._run(cache)

    async def invalidate_mapping(self, *, agent_id, conversation_id, resource_id, purpose=None, provider_namespace=None):
        if purpose is not None:
            _label(purpose, 'invalid_purpose')
        if provider_namespace is not None:
            _label(provider_namespace, 'invalid_provider_namespace')
        def invalidate():
            row = self._row(agent_id, conversation_id, resource_id)
            if row is None:
                raise ResourceCatalogError('resource_not_found')
            sql, params = 'DELETE FROM cloud_mapping WHERE resource_id=? AND version=? AND sha256=?', [resource_id, row['version'], row['sha256']]
            if purpose is not None:
                sql += ' AND purpose=?'; params.append(purpose)
            if provider_namespace is not None:
                sql += ' AND provider_namespace=?'; params.append(provider_namespace)
            cursor = self._open().execute(sql, params)
            self._open().commit()
            return cursor.rowcount
        return await self._run(invalidate)

    async def register_image_source(self, *, agent_id, conversation_id, resource_id, event_id, sender_ref, observed_at, sender_known=True):
        """登记图片来源（谁/何时发的，供引用回溯与同发送者补挂）。

        P13-R02：作者未知是**合法的来源状态**（引用原图查到了、但桥元数据
        里没有作者 wxid）。`sender_known=False` 时 `sender_ref` 必须为
        None/空，落库存**空串哨兵**（列 NOT NULL，不做 schema 迁移；空串
        被 `_label` 禁止作为真实身份，因此无歧义）——不是"某个空 wxid 的
        真实身份"，不能伪造稳定身份；空串不会命中 `sender_ref=?` 的真实
        wxid 精确查询，因此**不参与同发送者补挂的身份匹配**。默认
        `sender_known=True` 保持原有非空校验（普通接收/HTTP 上传路径行为
        不变，未被放松）。
        """

        _scope(agent_id, conversation_id)
        if not isinstance(event_id, str) or not event_id or len(event_id) > 512 or any(ord(c) < 32 for c in event_id):
            raise ResourceCatalogError('invalid_event_id')
        if not isinstance(sender_known, bool):
            raise ResourceCatalogError('invalid_sender')
        if sender_known:
            _label(sender_ref, 'invalid_sender')
        else:
            if sender_ref not in (None, ''):
                # 显式未知却不给空值：调用方语义矛盾，拒绝而不是猜。
                raise ResourceCatalogError('invalid_sender')
            sender_ref = ''
        if isinstance(observed_at, bool) or not isinstance(observed_at, (int, float)) or not math.isfinite(observed_at) or observed_at <= 0:
            raise ResourceCatalogError('invalid_observed_at')
        def register():
            row = self._row(agent_id, conversation_id, resource_id)
            if row is None or row['kind'] != 'image':
                raise ResourceCatalogError('image_not_found')
            conn = self._open()
            conn.execute('INSERT INTO image_source VALUES(?,?,?,?,?,?) ON CONFLICT(agent_id,conversation_id,event_id,resource_id) DO NOTHING',
                         (agent_id, conversation_id, event_id, resource_id, sender_ref, float(observed_at)))
            conn.commit()
            source = conn.execute('SELECT sender_ref,observed_at FROM image_source WHERE agent_id=? AND conversation_id=? AND event_id=? AND resource_id=?',
                                  (agent_id, conversation_id, event_id, resource_id)).fetchone()
            if source['sender_ref'] != sender_ref:
                raise ResourceCatalogError('image_source_conflict')
            # Retransmission keeps the original observation time instead of extending carry TTL.
            return self._metadata(row) | {'event_id': event_id, 'sender_ref': sender_ref, 'observed_at': float(source['observed_at'])}
        return await self._run(register)

    async def recent_images(self, *, agent_id, conversation_id, limit=5, offset=0, source_event_id=None, sender_ref=None, after_ts=None):
        _scope(agent_id, conversation_id)
        _bounded_int(limit, 1, 101, 'invalid_limit')
        _bounded_int(offset, 0, 10000, 'invalid_offset')
        if source_event_id is not None and (not isinstance(source_event_id, str) or len(source_event_id) > 512):
            raise ResourceCatalogError('invalid_event_id')
        if sender_ref is not None:
            _label(sender_ref, 'invalid_sender')
        if after_ts is not None and (isinstance(after_ts,bool) or not isinstance(after_ts,(int,float)) or not math.isfinite(after_ts)):
            raise ResourceCatalogError('invalid_after_ts')
        def query():
            clauses, params = ['s.agent_id=?', 's.conversation_id=?', "r.kind='image'"], [agent_id, conversation_id]
            if source_event_id is not None:
                clauses.append('s.event_id=?');params.append(source_event_id)
            if sender_ref is not None:
                clauses.append('s.sender_ref=?');params.append(sender_ref)
            if after_ts is not None:
                clauses.append('s.observed_at>=?');params.append(float(after_ts))
            rows = self._open().execute('SELECT r.*,s.event_id AS source_event_id,s.sender_ref,s.observed_at FROM image_source s '
                'JOIN resource r ON r.resource_id=s.resource_id WHERE '+' AND '.join(clauses)+' ORDER BY s.observed_at DESC,s.event_id DESC,r.resource_id DESC LIMIT ? OFFSET ?',
                [*params,limit,offset]).fetchall()
            return [self._metadata(row) | {'event_id':row['source_event_id'],'sender_ref':row['sender_ref'],'observed_at':row['observed_at']} for row in rows]
        return await self._run(query)

    async def aclose(self):
        def close():
            self._closed = True
            conn, self._conn = self._conn, None
            if conn is not None:
                conn.close()
        await self._run(close)
