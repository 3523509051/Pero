"""群级文档知识库：原件、版本、片段、关键词检索与任务记录。

边界（方案第 1 节）：
- 独立 SQLite + 独立原件目录，不与聊天资源库、L4、L20 混用；
- 只收录用户明确加入的文档；不自动收录群附件；
- 全文只在知识库内，检索片段只作为本轮临时上下文；
- 关闭知识库不删除数据；删除文档不影响聊天资源与其他记忆。

首版真实能力：
- 纯文本类解析（UTF-8/UTF-16/GB18030；txt/md/csv/tsv/json/log/yaml/ini/toml/html）；
- 按段落切分 + 中文子串关键词检索（LIKE，真实命中正文）；
- PDF/DOCX/PPTX/XLSX/EPUB 等未接入解析器的格式：保存原件并标记
  `parse_unavailable`，不伪称已索引；
- 向量检索未实现（不写假向量），`retrieval_mode` 只支持 `lexical`。
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
import threading
import time
import uuid
import asyncio,math
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

#: 可确定性解析的纯文本类扩展名。
TEXT_EXTENSIONS = frozenset({
    ".txt", ".text", ".md", ".markdown", ".csv", ".tsv", ".json", ".jsonc",
    ".log", ".yaml", ".yml", ".ini", ".cfg", ".conf", ".toml", ".xml",
    ".html", ".htm", ".rst", ".tex", ".sql", ".py", ".js", ".ts", ".java",
    ".go", ".rs", ".c", ".h", ".cpp", ".hpp", ".css",
})

#: 已知但未接入解析器的格式（保存原件，明确标记解析不可用）。
UNSUPPORTED_PARSE_EXTENSIONS = frozenset({
    ".pdf", ".doc", ".docx", ".ppt", ".pptx", ".xls", ".xlsx", ".epub",
    ".mobi", ".rtf", ".odt", ".ods", ".odp", ".zip", ".rar", ".7z",
})

MAX_UPLOAD_BYTES = 104_857_600
MAX_TEXT_CHARS = 2_000_000
MAX_TITLE_CHARS = 200
MAX_CHUNK_CHARS_LIMIT = 4000
CONTAINER_EXTENSIONS=frozenset({'.pdf','.docx','.pptx','.xlsx','.epub'})
_SERVICES={}
def knowledge_for(settings):
    root=settings.memory.data_root()/'knowledge'
    key=str(root.resolve())
    if key not in _SERVICES:_SERVICES[key]=KnowledgeService(root,settings.embeddings)
    return _SERVICES[key]

def parse_file(filename,data):
    ext=Path(filename).suffix.lower()
    if ext in CONTAINER_EXTENSIONS:
        from .parsers import parse_container
        try:return parse_container(filename,data)
        except ImportError:raise KnowledgeError('document_parser_unavailable') from None
        except Exception:raise KnowledgeError('document_parse_failed') from None
    if ext not in TEXT_EXTENSIONS:raise KnowledgeError('document_parser_unavailable')
    text=decode_text(data)
    if ext in {'.html','.htm','.xml'}:text=strip_html(text)
    if not text.strip() or len(text)>MAX_TEXT_CHARS:raise KnowledgeError('empty_or_oversized_text')
    return text

_SCHEMA = """
CREATE TABLE IF NOT EXISTS kb_config (
    agent_id TEXT NOT NULL,
    conversation_id TEXT NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 0,
    automatic_retrieval INTEGER NOT NULL DEFAULT 0,
    retrieval_mode TEXT NOT NULL DEFAULT 'lexical',
    top_k INTEGER NOT NULL DEFAULT 5,
    context_char_budget INTEGER NOT NULL DEFAULT 6000,
    chunk_size INTEGER NOT NULL DEFAULT 800,
    chunk_overlap INTEGER NOT NULL DEFAULT 100,
    embedding_profile TEXT NOT NULL DEFAULT '',
    revision INTEGER NOT NULL DEFAULT 1,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (agent_id, conversation_id)
);
CREATE TABLE IF NOT EXISTS kb_document (
    doc_id TEXT PRIMARY KEY,
    agent_id TEXT NOT NULL,
    conversation_id TEXT NOT NULL,
    title TEXT NOT NULL,
    category TEXT NOT NULL DEFAULT '',
    tags TEXT NOT NULL DEFAULT '[]',
    source_type TEXT NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 1,
    current_version TEXT,
    deleted_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS kb_document_scope
    ON kb_document(agent_id, conversation_id, deleted_at, updated_at DESC);
CREATE TABLE IF NOT EXISTS kb_version (
    version_id TEXT PRIMARY KEY,
    doc_id TEXT NOT NULL,
    agent_id TEXT NOT NULL,
    conversation_id TEXT NOT NULL,
    filename TEXT NOT NULL,
    mime TEXT NOT NULL DEFAULT '',
    size INTEGER NOT NULL DEFAULT 0,
    sha256 TEXT NOT NULL DEFAULT '',
    blob TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL,
    parse_error TEXT NOT NULL DEFAULT '',
    index_mode TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS kb_version_doc ON kb_version(doc_id, created_at DESC);
CREATE TABLE IF NOT EXISTS kb_chunk (
    chunk_id TEXT PRIMARY KEY,
    doc_id TEXT NOT NULL,
    version_id TEXT NOT NULL,
    agent_id TEXT NOT NULL,
    conversation_id TEXT NOT NULL,
    ordinal INTEGER NOT NULL,
    heading TEXT NOT NULL DEFAULT '',
    text TEXT NOT NULL,
    char_start INTEGER NOT NULL DEFAULT 0,
    char_end INTEGER NOT NULL DEFAULT 0,
    sha256 TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS kb_chunk_version ON kb_chunk(version_id, ordinal);
CREATE TABLE IF NOT EXISTS kb_job (
    job_id TEXT PRIMARY KEY,
    agent_id TEXT NOT NULL,
    conversation_id TEXT NOT NULL,
    doc_id TEXT NOT NULL,
    version_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    state TEXT NOT NULL,
    progress INTEGER NOT NULL DEFAULT 0,
    error_code TEXT NOT NULL DEFAULT '',
    attempts INTEGER NOT NULL DEFAULT 0,
    next_allowed_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS kb_job_scope ON kb_job(agent_id, conversation_id, state);
CREATE TABLE IF NOT EXISTS kb_vector (
    chunk_id TEXT PRIMARY KEY,
    provider TEXT NOT NULL,
    model TEXT NOT NULL,
    dim INTEGER NOT NULL,
    text_hash TEXT NOT NULL,
    vector BLOB NOT NULL,
    updated_at TEXT NOT NULL
);
"""


class KnowledgeError(Exception):
    def __init__(self, code: str, detail: str = "") -> None:
        self.code = code
        self.detail = detail or code
        super().__init__(code)


def _now() -> str:
    """知识库任务时间统一北京时间（带 +08:00），不用服务器本地时区。"""

    from pero.core.timebase import now_iso

    return now_iso()


def _digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def decode_text(data: bytes) -> str:
    for encoding in ("utf-8", "utf-16", "gb18030"):
        try:
            return data.decode(encoding)
        except UnicodeDecodeError:
            continue
    raise KnowledgeError("unsupported_encoding", "无法以 UTF-8/UTF-16/GB18030 解码")


def strip_html(text: str) -> str:
    text = re.sub(r"(?is)<(script|style).*?</\1>", " ", text)
    text = re.sub(r"(?s)<[^>]+>", " ", text)
    return re.sub(r"[ \t]+", " ", text)


def split_chunks(text: str, *, chunk_size: int, overlap: int) -> List[Dict[str, Any]]:
    """按自然段聚合切分；保留 Markdown 标题作为 heading。"""

    size = max(200, min(int(chunk_size), MAX_CHUNK_CHARS_LIMIT))
    gap = max(0, min(int(overlap), size // 2))
    paragraphs: List[Tuple[str, str, int]] = []
    heading = ""
    cursor = 0
    for raw in re.split(r"\n\s*\n", text):
        block = raw.strip("\n")
        if not block.strip():
            cursor += len(raw) + 2
            continue
        match = re.match(r"^(#{1,6})\s+(.+)$", block.strip())
        if match and "\n" not in block.strip():
            heading = match.group(2).strip()[:120]
        paragraphs.append((block, heading, cursor))
        cursor += len(raw) + 2
    chunks: List[Dict[str, Any]] = []
    buffer: List[Tuple[str, str, int]] = []
    length = 0
    start = 0

    def flush() -> None:
        nonlocal buffer, length, start
        if not buffer:
            return
        body = "\n\n".join(item[0] for item in buffer).strip()
        if body:
            chunks.append({
                "text": body, "heading": buffer[0][1],
                "char_start": start, "char_end": start + len(body),
            })
        tail = body[-gap:] if gap and len(body) > gap else ""
        buffer = []
        length = 0
        if tail:
            buffer.append((tail, "", 0))
            length = len(tail)
        start = chunks[-1]["char_end"] if chunks else 0

    for block, head, offset in paragraphs:
        if length and length + len(block) + 2 > size:
            flush()
            start = offset
        if not buffer:
            start = offset
        buffer.append((block, head, offset))
        length += len(block) + 2
        if length >= size:
            flush()
    flush()
    return [item for item in chunks if item["text"].strip()]


class KnowledgeService:
    """单进程串行访问的群级知识库（与主记忆库完全分离）。"""

    def __init__(self, root: Path, embedding_settings=None) -> None:
        self._root = Path(root)
        self._root.mkdir(parents=True, exist_ok=True)
        (self._root / "blobs").mkdir(parents=True, exist_ok=True)
        self._db = self._root / "knowledge.sqlite3"
        self._lock = threading.RLock()
        from pero.memory.semantic import EmbeddingClient
        self._embedding=EmbeddingClient(embedding_settings) if embedding_settings else None
        self._vector_locks={}
        from types import SimpleNamespace
        self.settings=SimpleNamespace(enabled=True)
        with self._lock, self._connect() as conn:
            conn.executescript(_SCHEMA)
            conn.execute('CREATE TABLE IF NOT EXISTS kb_embedding(agent_id TEXT NOT NULL,conversation_id TEXT NOT NULL,base_url TEXT NOT NULL,model TEXT NOT NULL,dimension INTEGER NOT NULL,credential_ref TEXT NOT NULL,PRIMARY KEY(agent_id,conversation_id))')
            conn.commit()

    @contextmanager
    def _connect(self):
        conn = sqlite3.connect(self._db, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=OFF")
        try:
            with conn:yield conn
        finally:conn.close()

    # ------------------------------------------------------------------ #
    # 配置
    # ------------------------------------------------------------------ #

    def get_config(self, *, agent_id: str, conversation_id: str) -> Dict[str, Any]:
        with self._lock, self._connect() as conn:
            emb=conn.execute('SELECT base_url,model,dimension,credential_ref FROM kb_embedding WHERE agent_id=? AND conversation_id=?',(agent_id,conversation_id)).fetchone()
            profile=dict(emb) if emb else None
            row = conn.execute(
                "SELECT * FROM kb_config WHERE agent_id=? AND conversation_id=?",
                (agent_id, conversation_id),
            ).fetchone()
            if row is None:
                return {
                    "agent_id": agent_id, "conversation_id": conversation_id,
                    "enabled": False, "automatic_retrieval": False,
                    "retrieval_mode": "lexical", "top_k": 5,
                    "context_char_budget": 6000, "chunk_size": 800,
                    "chunk_overlap": 100, "embedding_profile": "",
                    "revision": 0,'embedding':profile,
                }
            item = dict(row)
            item["enabled"] = bool(item["enabled"])
            item["automatic_retrieval"] = bool(item["automatic_retrieval"])
            item['embedding']=profile
            return item

    def patch_config(self, *, agent_id: str, conversation_id: str,
                     values: Dict[str, Any], revision: Optional[int] = None) -> Dict[str, Any]:
        current = self.get_config(agent_id=agent_id, conversation_id=conversation_id)
        if revision is not None and int(revision) != int(current.get("revision") or 0):
            raise KnowledgeError("revision_conflict", "配置已被其他页面修改，请刷新后重试")
        allowed = {
            "enabled", "automatic_retrieval", "retrieval_mode", "top_k",
            "context_char_budget", "chunk_size", "chunk_overlap", "embedding_profile",
        }
        updates = {key: values[key] for key in values if key in allowed}
        embedding=values.get('embedding') if 'embedding' in values else None
        if embedding is not None:
            from urllib.parse import urlsplit
            if not isinstance(embedding,dict) or set(embedding)!={'base_url','model','dimension','credential_ref'}:raise KnowledgeError('invalid_embedding_config')
            url=urlsplit(str(embedding['base_url']));ref=str(embedding['credential_ref'])
            from .ollama import is_local_ollama
            local=is_local_ollama(str(embedding['base_url']))
            remote=url.scheme=='https' and url.hostname and not (url.username or url.password or url.query or url.fragment) and re.fullmatch('cred-[0-9a-f]{32}',ref)
            if not (local or remote) or not str(embedding['model']).strip() or not 64<=int(embedding['dimension'])<=4096:raise KnowledgeError('invalid_embedding_config')
            if local:embedding={**embedding,'credential_ref':''}
        if "retrieval_mode" in updates and updates["retrieval_mode"] not in {'lexical','hybrid'}:
            raise KnowledgeError("vector_not_configured",
                                 "仅支持 lexical 或 hybrid")
        for key in ("top_k",):
            if key in updates:
                value = int(updates[key])
                if not 1 <= value <= 20:
                    raise KnowledgeError("invalid_top_k", "top_k 必须在 1..20")
                updates[key] = value
        for key in ("context_char_budget", "chunk_size", "chunk_overlap"):
            if key in updates:
                value = int(updates[key])
                if value < (0 if key=='chunk_overlap' else 1) or (key=='chunk_size' and not 200<=value<=4000) or (key=='context_char_budget' and value>20000):
                    raise KnowledgeError("invalid_budget", f"{key} 必须为正整数")
                updates[key] = value
        if int(updates.get("chunk_overlap", current["chunk_overlap"])) >= int(
            updates.get("chunk_size", current["chunk_size"])
        ):
            raise KnowledgeError("invalid_chunking", "chunk_overlap 必须小于 chunk_size")
        now = _now()
        with self._lock, self._connect() as conn:
            conn.execute('BEGIN IMMEDIATE')
            latest=conn.execute('SELECT revision FROM kb_config WHERE agent_id=? AND conversation_id=?',(agent_id,conversation_id)).fetchone()
            if int(latest[0] if latest else 0)!=int(current['revision']):raise KnowledgeError('revision_conflict')
            if current["revision"] == 0:
                conn.execute(
                    "INSERT INTO kb_config(agent_id,conversation_id,enabled,automatic_retrieval,"
                    "retrieval_mode,top_k,context_char_budget,chunk_size,chunk_overlap,"
                    "embedding_profile,revision,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,1,?)",
                    (agent_id, conversation_id,
                     1 if updates.get("enabled", False) else 0,
                     1 if updates.get("automatic_retrieval", False) else 0,
                     str(updates.get("retrieval_mode", "lexical")),
                     int(updates.get("top_k", 5)),
                     int(updates.get("context_char_budget", 6000)),
                     int(updates.get("chunk_size", 800)),
                     int(updates.get("chunk_overlap", 100)),
                     str(updates.get("embedding_profile", ""))[:80], now),
                )
            else:
                sets = ",".join(f"{key}=?" for key in updates)
                params = []
                for key, value in updates.items():
                    params.append(int(value) if key in {
                        "enabled", "automatic_retrieval", "top_k",
                        "context_char_budget", "chunk_size", "chunk_overlap",
                    } else str(value))
                params.extend([now, agent_id, conversation_id])
                if sets:
                    conn.execute(
                        f"UPDATE kb_config SET {sets},revision=revision+1,updated_at=? "
                        "WHERE agent_id=? AND conversation_id=?",
                        params,
                    )
                elif 'embedding' in values:conn.execute('UPDATE kb_config SET revision=revision+1,updated_at=? WHERE agent_id=? AND conversation_id=?',(now,agent_id,conversation_id))
            if 'embedding' in values:
                if embedding is None:conn.execute('DELETE FROM kb_embedding WHERE agent_id=? AND conversation_id=?',(agent_id,conversation_id))
                else:conn.execute('INSERT OR REPLACE INTO kb_embedding VALUES(?,?,?,?,?,?)',(agent_id,conversation_id,str(embedding['base_url']).rstrip('/'),str(embedding['model']),int(embedding['dimension']),str(embedding['credential_ref'])))
            conn.commit()
        return self.get_config(agent_id=agent_id, conversation_id=conversation_id)

    # ------------------------------------------------------------------ #
    # 文档写入
    # ------------------------------------------------------------------ #

    def add_text(self, *, agent_id: str, conversation_id: str, title: str,
                 content: str, category: str = "", tags: Sequence[str] = (),
                 filename: str = "") -> Dict[str, Any]:
        title = str(title or "").strip()
        if not title or len(title) > MAX_TITLE_CHARS:
            raise KnowledgeError("invalid_title", "标题不能为空且不超过 200 字")
        text = str(content or "")
        if not text.strip():
            raise KnowledgeError("empty_content", "正文不能为空")
        if len(text) > MAX_TEXT_CHARS:
            raise KnowledgeError("content_too_large", "正文超过 2,000,000 字符上限")
        data = text.encode("utf-8")
        return self._ingest(
            agent_id=agent_id, conversation_id=conversation_id, title=title,
            category=category, tags=tags, filename=filename or (title + ".txt"),
            data=data, mime="text/plain", source_type="text",
            predecoded=text,
        )

    def add_file(self, *, agent_id: str, conversation_id: str, title: str,
                 filename: str, data: bytes, category: str = "",
                 tags: Sequence[str] = ()) -> Dict[str, Any]:
        if not isinstance(data, (bytes, bytearray)) or not data:
            raise KnowledgeError("empty_content", "文件为空")
        if len(data) > MAX_UPLOAD_BYTES:
            raise KnowledgeError("file_too_large", "原件超过 100 MiB 上限")
        return self._ingest(
            agent_id=agent_id, conversation_id=conversation_id,
            title=str(title or filename or "未命名文档").strip()[:MAX_TITLE_CHARS],
            category=category, tags=tags, filename=filename,
            data=bytes(data), mime="", source_type="upload", predecoded=None,
        )

    def add_from_resource(self, *, agent_id: str, conversation_id: str,
                          title: str, filename: str, data: bytes,
                          category: str = "", tags: Sequence[str] = ()) -> Dict[str, Any]:
        if not isinstance(data, (bytes, bytearray)) or not data:
            raise KnowledgeError("empty_content", "资源为空")
        if len(data) > MAX_UPLOAD_BYTES:
            raise KnowledgeError("file_too_large", "原件超过 100 MiB 上限")
        return self._ingest(
            agent_id=agent_id, conversation_id=conversation_id,
            title=str(title or filename or "群文件").strip()[:MAX_TITLE_CHARS],
            category=category, tags=tags, filename=filename,
            data=bytes(data), mime="", source_type="resource", predecoded=None,
        )

    def _ingest(self, *, agent_id: str, conversation_id: str, title: str,
                category: str, tags: Sequence[str], filename: str, data: bytes,
                mime: str, source_type: str, predecoded: Optional[str]) -> Dict[str, Any]:
        digest = _digest(data)
        ext = Path(filename).suffix.lower()
        now = _now()
        with self._lock, self._connect() as conn:
            duplicate = conn.execute(
                "SELECT d.doc_id,d.title FROM kb_document d JOIN kb_version v ON v.doc_id=d.doc_id "
                "WHERE d.agent_id=? AND d.conversation_id=? AND d.deleted_at IS NULL "
                "AND v.sha256=? LIMIT 1",
                (agent_id, conversation_id, digest),
            ).fetchone()
            if duplicate is not None:
                raise KnowledgeError("duplicate_document",
                                     f"同群已存在相同内容的文档：{duplicate['title']}")
            version_id = "kv-" + uuid.uuid4().hex
            doc_id = "kd-" + uuid.uuid4().hex
            blob_name = ""
            if data:
                blob_name = version_id + ".bin"
                (self._root / "blobs" / blob_name).write_bytes(data)
            parsed = predecoded
            status = "indexed"
            error = ""
            if parsed is None:
                try:parsed=parse_file(filename,data)
                except KnowledgeError as exc:
                    status='parse_unavailable' if exc.code=='document_parser_unavailable' else 'parse_failed';error=exc.code
            module = conn.execute(
                "SELECT chunk_size,chunk_overlap FROM kb_config WHERE agent_id=? AND conversation_id=?",
                (agent_id, conversation_id),
            ).fetchone()
            chunk_size = int(module[0]) if module else 800
            chunk_overlap = int(module[1]) if module else 100
            conn.execute(
                "INSERT INTO kb_document(doc_id,agent_id,conversation_id,title,category,tags,"
                "source_type,enabled,current_version,deleted_at,created_at,updated_at) "
                "VALUES(?,?,?,?,?,?,?,1,NULL,NULL,?,?)",
                (doc_id, agent_id, conversation_id, title,
                 str(category or "")[:60],
                 json.dumps([str(tag)[:40] for tag in list(tags)[:10]], ensure_ascii=False),
                 source_type, now, now),
            )
            conn.execute(
                "INSERT INTO kb_version(version_id,doc_id,agent_id,conversation_id,filename,"
                "mime,size,sha256,blob,status,parse_error,index_mode,created_at) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (version_id, doc_id, agent_id, conversation_id, filename[:200], mime,
                 len(data), digest, blob_name, status, error, "", now),
            )
            chunks = 0
            if parsed is not None:
                chunks = self._index_chunks(
                    conn, agent_id=agent_id, conversation_id=conversation_id,
                    doc_id=doc_id, version_id=version_id, text=parsed,
                    chunk_size=chunk_size, chunk_overlap=chunk_overlap,
                )
                conn.execute(
                    "UPDATE kb_version SET index_mode='lexical' WHERE version_id=?",
                    (version_id,),
                )
                conn.execute(
                    "UPDATE kb_document SET current_version=?,updated_at=? WHERE doc_id=?",
                    (version_id, now, doc_id),
                )
            conn.execute(
                "INSERT INTO kb_job(job_id,agent_id,conversation_id,doc_id,version_id,kind,state,"
                "progress,error_code,attempts,next_allowed_at,created_at,updated_at) "
                "VALUES(?,?,?,?,?,'index',?,?,?,1,NULL,?,?)",
                ("kj-" + uuid.uuid4().hex, agent_id, conversation_id, doc_id, version_id,
                 "done" if status == "indexed" else "failed",
                 100 if status == "indexed" else 0, error, now, now),
            )
            conn.commit()
        return {
            "doc_id": doc_id, "version_id": version_id, "status": status,
            "chunks": chunks, "sha256": digest, "parse_error": error,
        }

    def _index_chunks(self, conn: sqlite3.Connection, *, agent_id: str,
                      conversation_id: str, doc_id: str, version_id: str,
                      text: str, chunk_size: int, chunk_overlap: int) -> int:
        if not text.strip() or len(text)>MAX_TEXT_CHARS:raise KnowledgeError('empty_or_oversized_text')
        step=chunk_size-min(chunk_overlap,chunk_size//2)
        parts=[{'text':text[i:i+chunk_size],'heading':'','char_start':i,'char_end':min(i+chunk_size,len(text))} for i in range(0,len(text),step)]
        for ordinal, item in enumerate(parts):
            conn.execute(
                "INSERT INTO kb_chunk(chunk_id,doc_id,version_id,agent_id,conversation_id,"
                "ordinal,heading,text,char_start,char_end,sha256) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                ("kc-" + uuid.uuid4().hex, doc_id, version_id, agent_id,
                 conversation_id, ordinal, item["heading"], item["text"],
                 item["char_start"], item["char_end"],
                 _digest(item["text"].encode("utf-8"))),
            )
        return len(parts)

    def add_version(self, *, agent_id: str, conversation_id: str, doc_id: str,
                    filename: str = "", data: Optional[bytes] = None,
                    content: str = "") -> Dict[str, Any]:
        """追加新版本：索引成功后才原子切换；失败保留上一可用版本。"""

        doc = self.get_document(agent_id=agent_id, conversation_id=conversation_id,
                                doc_id=doc_id)
        payload = bytes(data) if data is not None else str(content or "").encode("utf-8")
        if not payload:
            raise KnowledgeError("empty_content", "新版本内容为空")
        if len(payload) > MAX_UPLOAD_BYTES:
            raise KnowledgeError("file_too_large", "原件超过 100 MiB 上限")
        name = filename or (doc["title"] + ".txt")
        ext = Path(name).suffix.lower()
        digest = _digest(payload)
        version_id = "kv-" + uuid.uuid4().hex
        now = _now()
        status = "indexed"
        error = ""
        text: Optional[str] = None
        try:text=parse_file(name,payload)
        except KnowledgeError as exc:status='parse_unavailable' if exc.code=='document_parser_unavailable' else 'parse_failed';error=exc.code
        with self._lock, self._connect() as conn:
            duplicate = conn.execute(
                "SELECT version_id FROM kb_version WHERE doc_id=? AND sha256=?",
                (doc_id, digest),
            ).fetchone()
            if duplicate is not None:
                raise KnowledgeError("duplicate_version", "该文档已存在相同内容的版本")
            blob = version_id + ".bin"
            (self._root / "blobs" / blob).write_bytes(payload)
            conn.execute(
                "INSERT INTO kb_version(version_id,doc_id,agent_id,conversation_id,filename,"
                "mime,size,sha256,blob,status,parse_error,index_mode,created_at) "
                "VALUES(?,?,?,?,?,'',?,?,?,?,?,'',?)",
                (version_id, doc_id, agent_id, conversation_id, name[:200],
                 len(payload), digest, blob, status, error, now),
            )
            chunks = 0
            switched = False
            if text is not None:
                cfg = conn.execute(
                    "SELECT chunk_size,chunk_overlap FROM kb_config WHERE agent_id=? AND conversation_id=?",
                    (agent_id, conversation_id),
                ).fetchone()
                chunks = self._index_chunks(
                    conn, agent_id=agent_id, conversation_id=conversation_id,
                    doc_id=doc_id, version_id=version_id, text=text,
                    chunk_size=int(cfg[0]) if cfg else 800,
                    chunk_overlap=int(cfg[1]) if cfg else 100,
                )
                conn.execute(
                    "UPDATE kb_version SET status='indexed',index_mode='lexical' WHERE version_id=?",
                    (version_id,),
                )
                # 只有索引成功才切换；失败保持旧版本可检索。
                conn.execute(
                    "UPDATE kb_document SET current_version=?,updated_at=? WHERE doc_id=?",
                    (version_id, now, doc_id),
                )
                switched = True
            conn.execute(
                "INSERT INTO kb_job(job_id,agent_id,conversation_id,doc_id,version_id,kind,state,"
                "progress,error_code,attempts,next_allowed_at,created_at,updated_at) "
                "VALUES(?,?,?,?,?,'index',?,?,?,1,NULL,?,?)",
                ("kj-" + uuid.uuid4().hex, agent_id, conversation_id, doc_id, version_id,
                 "done" if switched else "failed", 100 if switched else 0,
                 "" if switched else error, now, now),
            )
            conn.commit()
        return {"doc_id": doc_id, "version_id": version_id, "status": status,
                "chunks": chunks, "switched": switched, "parse_error": error}

    # ------------------------------------------------------------------ #
    # 查询与维护
    # ------------------------------------------------------------------ #

    def list_documents(self, *, agent_id: str, conversation_id: str,
                       query: str = "", category: str = "", status: str = "",
                       limit: int = 20, offset: int = 0) -> Dict[str, Any]:
        limit = max(1, min(int(limit), 100))
        offset = max(0, int(offset))
        clauses = ["d.agent_id=?", "d.conversation_id=?", "d.deleted_at IS NULL"]
        params: List[Any] = [agent_id, conversation_id]
        if query:
            clauses.append("d.title LIKE ?")
            params.append(f"%{query}%")
        if category:
            clauses.append("d.category=?")
            params.append(category)
        if status:
            clauses.append(
                "COALESCE((SELECT v.status FROM kb_version v WHERE v.version_id=d.current_version),"
                "'pending')=?"
            )
            params.append(status)
        where = " AND ".join(clauses)
        with self._lock, self._connect() as conn:
            rows = conn.execute(
                "SELECT d.*, (SELECT v.status FROM kb_version v WHERE v.version_id=d.current_version) "
                "AS current_status, (SELECT v.filename FROM kb_version v WHERE v.version_id=d.current_version) "
                "AS current_filename, (SELECT COUNT(*) FROM kb_chunk c WHERE c.version_id=d.current_version) "
                "AS chunk_count FROM kb_document d WHERE " + where +
                " ORDER BY d.updated_at DESC LIMIT ? OFFSET ?",
                [*params, limit, offset],
            ).fetchall()
            total = int(conn.execute(
                "SELECT COUNT(*) FROM kb_document d WHERE " + where, params
            ).fetchone()[0])
        items = []
        for row in rows:
            item = dict(row)
            item["tags"] = json.loads(item.get("tags") or "[]")
            item["enabled"] = bool(item["enabled"])
            items.append(item)
        return {"items": items, "total": total, "limit": limit, "offset": offset}

    def get_document(self, *, agent_id: str, conversation_id: str,
                     doc_id: str) -> Dict[str, Any]:
        with self._lock, self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM kb_document WHERE doc_id=? AND agent_id=? AND conversation_id=? "
                "AND deleted_at IS NULL",
                (doc_id, agent_id, conversation_id),
            ).fetchone()
            if row is None:
                raise KnowledgeError("document_not_found", "文档不存在或不属于本群")
            item = dict(row)
            item["tags"] = json.loads(item.get("tags") or "[]")
            item["enabled"] = bool(item["enabled"])
            versions = conn.execute(
                "SELECT version_id,filename,size,sha256,status,parse_error,index_mode,created_at "
                "FROM kb_version WHERE doc_id=? ORDER BY created_at DESC",
                (doc_id,),
            ).fetchall()
            item["versions"] = [dict(version) for version in versions]
            item["current_status"] = next(
                (version["status"] for version in versions
                 if version["version_id"] == item["current_version"]), "pending")
        return item

    def list_chunks(self, *, agent_id: str, conversation_id: str, doc_id: str,
                    limit: int = 20, offset: int = 0) -> Dict[str, Any]:
        limit = max(1, min(int(limit), 50))
        offset = max(0, int(offset))
        with self._lock, self._connect() as conn:
            doc = conn.execute(
                "SELECT current_version FROM kb_document WHERE doc_id=? AND agent_id=? "
                "AND conversation_id=? AND deleted_at IS NULL",
                (doc_id, agent_id, conversation_id),
            ).fetchone()
            if doc is None:
                raise KnowledgeError("document_not_found", "文档不存在或不属于本群")
            version = doc[0]
            if not version:
                return {"items": [], "total": 0}
            rows = conn.execute(
                "SELECT chunk_id,ordinal,heading,text,char_start,char_end FROM kb_chunk "
                "WHERE version_id=? ORDER BY ordinal LIMIT ? OFFSET ?",
                (version, limit, offset),
            ).fetchall()
            total = int(conn.execute(
                "SELECT COUNT(*) FROM kb_chunk WHERE version_id=?", (version,)
            ).fetchone()[0])
        return {"items": [dict(row) for row in rows], "total": total}

    def patch_document(self, *, agent_id: str, conversation_id: str, doc_id: str,
                       values: Dict[str, Any]) -> Dict[str, Any]:
        allowed = {"title", "category", "tags", "enabled"}
        updates = {key: values[key] for key in values if key in allowed}
        if not updates:
            return self.get_document(agent_id=agent_id, conversation_id=conversation_id,
                                     doc_id=doc_id)
        params: List[Any] = []
        sets: List[str] = []
        for key, value in updates.items():
            if key == "tags":
                sets.append("tags=?")
                params.append(json.dumps([str(tag)[:40] for tag in list(value)[:10]],
                                         ensure_ascii=False))
            elif key == "enabled":
                sets.append("enabled=?")
                params.append(1 if value else 0)
            else:
                sets.append(f"{key}=?")
                params.append(str(value)[:MAX_TITLE_CHARS])
        params.extend([_now(), doc_id, agent_id, conversation_id])
        with self._lock, self._connect() as conn:
            cursor = conn.execute(
                f"UPDATE kb_document SET {','.join(sets)},updated_at=? "
                "WHERE doc_id=? AND agent_id=? AND conversation_id=? AND deleted_at IS NULL",
                params,
            )
            conn.commit()
            if cursor.rowcount != 1:
                raise KnowledgeError("document_not_found", "文档不存在或不属于本群")
        return self.get_document(agent_id=agent_id, conversation_id=conversation_id,
                                 doc_id=doc_id)

    def delete_document(self, *, agent_id: str, conversation_id: str,
                        doc_id: str) -> Dict[str, Any]:
        with self._lock, self._connect() as conn:
            cursor = conn.execute(
                "UPDATE kb_document SET deleted_at=?,enabled=0,updated_at=? "
                "WHERE doc_id=? AND agent_id=? AND conversation_id=? AND deleted_at IS NULL",
                (_now(), _now(), doc_id, agent_id, conversation_id),
            )
            conn.commit()
            if cursor.rowcount != 1:
                raise KnowledgeError("document_not_found", "文档不存在或不属于本群")
        return {"doc_id": doc_id, "deleted": True, "soft_delete": True}

    def reindex(self, *, agent_id: str, conversation_id: str, doc_id: str) -> Dict[str, Any]:
        doc = self.get_document(agent_id=agent_id, conversation_id=conversation_id, doc_id=doc_id)
        version = doc.get("current_version")
        if not version:
            raise KnowledgeError("no_indexable_version", "该文档没有可索引的版本")
        with self._lock, self._connect() as conn:
            row = conn.execute(
                "SELECT filename,blob,status FROM kb_version WHERE version_id=?",
                (version,),
            ).fetchone()
            if row is None:
                raise KnowledgeError("version_not_found", "版本不存在")
            if row["status"] == "parse_unavailable":
                raise KnowledgeError("document_parser_unavailable",
                                     "该格式未接入解析器，重新索引不会产生正文")
            ext = Path(row["filename"]).suffix.lower()
            data = (self._root / "blobs" / row["blob"]).read_bytes() if row["blob"] else b""
            text = parse_file(row['filename'],data)
            conn.execute('BEGIN IMMEDIATE')
            conn.execute('DELETE FROM kb_vector WHERE chunk_id IN (SELECT chunk_id FROM kb_chunk WHERE version_id=?)',(version,))
            conn.execute("DELETE FROM kb_chunk WHERE version_id=?", (version,))
            cfg = conn.execute(
                "SELECT chunk_size,chunk_overlap FROM kb_config WHERE agent_id=? AND conversation_id=?",
                (agent_id, conversation_id),
            ).fetchone()
            chunks = self._index_chunks(
                conn, agent_id=agent_id, conversation_id=conversation_id,
                doc_id=doc_id, version_id=version, text=text,
                chunk_size=int(cfg[0]) if cfg else 800,
                chunk_overlap=int(cfg[1]) if cfg else 100,
            )
            now = _now()
            conn.execute(
                "UPDATE kb_version SET status='indexed',parse_error='',index_mode='lexical' "
                "WHERE version_id=?", (version,),
            )
            conn.execute("UPDATE kb_document SET current_version=?,updated_at=? WHERE doc_id=?",
                         (version, now, doc_id))
            conn.execute(
                "INSERT INTO kb_job(job_id,agent_id,conversation_id,doc_id,version_id,kind,state,"
                "progress,error_code,attempts,next_allowed_at,created_at,updated_at) "
                "VALUES(?,?,?,?,?,'reindex','done',100,'',1,NULL,?,?)",
                ("kj-" + uuid.uuid4().hex, agent_id, conversation_id, doc_id, version, now, now),
            )
            conn.commit()
        return {"doc_id": doc_id, "chunks": chunks, "state": "done", "mode": "lexical"}

    def list_jobs(self, *, agent_id: str, conversation_id: str, limit: int = 20) -> Dict[str, Any]:
        with self._lock, self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM kb_job WHERE agent_id=? AND conversation_id=? "
                "ORDER BY created_at DESC LIMIT ?",
                (agent_id, conversation_id, max(1, min(int(limit), 100))),
            ).fetchall()
        return {"items": [dict(row) for row in rows]}

    def queue_reindex(self,*,agent_id,conversation_id,doc_id):
        doc=self.get_document(agent_id=agent_id,conversation_id=conversation_id,doc_id=doc_id)
        version=doc.get('current_version')
        if not version:raise KnowledgeError('version_not_found')
        with self._lock,self._connect() as conn:
            old=conn.execute("SELECT job_id,state FROM kb_job WHERE agent_id=? AND conversation_id=? AND doc_id=? AND kind='reindex_async' AND state IN ('pending','running')",(agent_id,conversation_id,doc_id)).fetchone()
            if old:return dict(old)
            job='kj-'+uuid.uuid4().hex;now=_now()
            conn.execute("INSERT INTO kb_job(job_id,agent_id,conversation_id,doc_id,version_id,kind,state,progress,attempts,created_at,updated_at) VALUES(?,?,?,?,?,'reindex_async','pending',0,0,?,?)",(job,agent_id,conversation_id,doc_id,version,now,now))
            return {'job_id':job,'state':'pending'}

    def _claim_job(self):
        with self._lock,self._connect() as conn:
            row=conn.execute("SELECT * FROM kb_job WHERE kind='reindex_async' AND state='pending' ORDER BY created_at LIMIT 1").fetchone()
            if not row:return None
            conn.execute("UPDATE kb_job SET state='running',attempts=attempts+1,updated_at=? WHERE job_id=?",(_now(),row['job_id']))
            return dict(row)

    def _settle_job(self,job,state,error=''):
        with self._lock,self._connect() as conn:conn.execute('UPDATE kb_job SET state=?,progress=CASE WHEN ?=\'done\' THEN 100 ELSE progress END,error_code=?,updated_at=? WHERE job_id=?',(state,state,error,_now(),job))

    async def worker(self):
        # An interrupted expensive operation requires an explicit retry.
        def recover():
            with self._lock,self._connect() as conn:conn.execute("UPDATE kb_job SET state='failed',error_code='interrupted' WHERE kind='reindex_async' AND state='running'")
        await asyncio.to_thread(recover)
        while True:
            job=await asyncio.to_thread(self._claim_job)
            if not job:await asyncio.sleep(2);continue
            args={k:job[k] for k in ('agent_id','conversation_id','doc_id')}
            try:
                doc=await asyncio.to_thread(self.get_document,**args)
                if doc['current_version']!=job['version_id']:raise KnowledgeError('version_changed')
                cfg=await asyncio.to_thread(self.get_config,agent_id=job['agent_id'],conversation_id=job['conversation_id'])
                if int(job['progress'])<50:
                    await asyncio.to_thread(self.reindex,**args)
                    def mark_lexical():
                        with self._lock,self._connect() as conn:conn.execute('UPDATE kb_job SET progress=50 WHERE job_id=?',(job['job_id'],))
                    await asyncio.to_thread(mark_lexical)
                if cfg['retrieval_mode']=='hybrid':
                    result=await self.build_vectors(**args)
                    if result['state']!='done':raise KnowledgeError('vector_'+result['state'])
                await asyncio.to_thread(self._settle_job,job['job_id'],'done')
            except asyncio.CancelledError:
                await asyncio.shield(asyncio.to_thread(self._settle_job,job['job_id'],'failed','interrupted'));raise
            except Exception as e:await asyncio.to_thread(self._settle_job,job['job_id'],'failed',getattr(e,'code',type(e).__name__))

    def retry_job(self, *, agent_id: str, conversation_id: str, job_id: str) -> Dict[str, Any]:
        with self._lock, self._connect() as conn:
            row = conn.execute(
                "SELECT doc_id,state,kind FROM kb_job WHERE job_id=? AND agent_id=? AND conversation_id=?",
                (job_id, agent_id, conversation_id),
            ).fetchone()
            if row is None:
                raise KnowledgeError("job_not_found", "任务不存在或不属于本群")
            if row["state"] == "done":
                raise KnowledgeError("job_already_done", "该任务已完成，无需重试")
            if row['kind']=='reindex_async':
                if row['state']!='failed':raise KnowledgeError('job_not_failed')
                conn.execute("UPDATE kb_job SET state='pending',error_code='',updated_at=? WHERE job_id=?",(_now(),job_id))
                return {'job_id':job_id,'state':'pending'}
        result = self.reindex(agent_id=agent_id, conversation_id=conversation_id,
                              doc_id=row["doc_id"])
        return {"job_id": job_id, "state": result["state"], "chunks": result["chunks"]}

    def status(self, *, agent_id: str, conversation_id: str) -> Dict[str, Any]:
        config = self.get_config(agent_id=agent_id, conversation_id=conversation_id)
        with self._lock, self._connect() as conn:
            documents = int(conn.execute(
                "SELECT COUNT(*) FROM kb_document WHERE agent_id=? AND conversation_id=? "
                "AND deleted_at IS NULL", (agent_id, conversation_id)).fetchone()[0])
            enabled = int(conn.execute(
                "SELECT COUNT(*) FROM kb_document WHERE agent_id=? AND conversation_id=? "
                "AND deleted_at IS NULL AND enabled=1", (agent_id, conversation_id)).fetchone()[0])
            searchable = int(conn.execute(
                "SELECT COUNT(DISTINCT v.version_id) FROM kb_version v JOIN kb_document d "
                "ON d.doc_id=v.doc_id AND d.current_version=v.version_id "
                "WHERE d.agent_id=? AND d.conversation_id=? AND d.deleted_at IS NULL "
                "AND d.enabled=1 AND v.status='indexed'",
                (agent_id, conversation_id)).fetchone()[0])
            chunks = int(conn.execute(
                "SELECT COUNT(*) FROM kb_chunk c JOIN kb_document d ON d.doc_id=c.doc_id "
                "AND d.current_version=c.version_id WHERE d.agent_id=? AND d.conversation_id=? "
                "AND d.deleted_at IS NULL", (agent_id, conversation_id)).fetchone()[0])
            failed = int(conn.execute(
                "SELECT COUNT(*) FROM kb_job WHERE agent_id=? AND conversation_id=? AND state='failed'",
                (agent_id, conversation_id)).fetchone()[0])
            pending = int(conn.execute(
                "SELECT COUNT(*) FROM kb_job WHERE agent_id=? AND conversation_id=? "
                "AND state IN ('pending','running')", (agent_id, conversation_id)).fetchone()[0])
        return {
            "enabled": config["enabled"], "automatic_retrieval": config["automatic_retrieval"],
            "retrieval_mode": config['retrieval_mode'], "documents": documents, "enabled_documents": enabled,
            "searchable_versions": searchable, "chunks": chunks,
            "failed_jobs": failed, "pending_jobs": pending,
            "vector_configured": bool(config.get('embedding') or (self._embedding and self._embedding.configured)),
            "supported_formats": sorted(TEXT_EXTENSIONS|CONTAINER_EXTENSIONS),
            "parse_unavailable_formats": sorted(UNSUPPORTED_PARSE_EXTENSIONS-CONTAINER_EXTENSIONS),
        }

    # ------------------------------------------------------------------ #
    # 检索（关键词，真实命中正文）
    # ------------------------------------------------------------------ #

    def search(self, *, agent_id: str, conversation_id: str, query: str,
               limit: int = 5, category: str = "") -> Dict[str, Any]:
        config = self.get_config(agent_id=agent_id, conversation_id=conversation_id)
        if not config["enabled"]:
            return {"state": "disabled", "items": [], "mode": "lexical"}
        terms = _query_terms(query)
        if not terms:
            return {"state": "no_hits", "items": [], "mode": "lexical"}
        top_k = max(1, min(int(limit or config["top_k"]), 20))
        clauses = [
            "d.agent_id=?", "d.conversation_id=?", "d.deleted_at IS NULL", "d.enabled=1",
            "c.version_id=d.current_version",
        ]
        params: List[Any] = [agent_id, conversation_id]
        if category:
            clauses.append("d.category=?")
            params.append(category)
        likes = " OR ".join(["c.text LIKE ?"] * len(terms))
        clauses.append(f"({likes})")
        params.extend([f"%{term}%" for term in terms])
        with self._lock, self._connect() as conn:
            rows = conn.execute(
                "SELECT c.chunk_id,c.doc_id,c.version_id,c.heading,c.text,c.ordinal,"
                "d.title,d.category,v.filename,v.created_at FROM kb_chunk c "
                "JOIN kb_document d ON d.doc_id=c.doc_id "
                "JOIN kb_version v ON v.version_id=c.version_id "
                "WHERE " + " AND ".join(clauses) + " LIMIT 200",
                params,
            ).fetchall()
        scored: List[Tuple[int, Dict[str, Any]]] = []
        for row in rows:
            text = str(row["text"] or "")
            score = sum(text.count(term) for term in terms)
            if score <= 0:
                continue
            snippet = _snippet(text, terms)
            scored.append((score, {
                "chunk_id": row["chunk_id"], "doc_id": row["doc_id"],
                "version_id": row["version_id"], "title": row["title"],
                "category": row["category"], "heading": row["heading"],
                "ordinal": row["ordinal"], "filename": row["filename"],
                "updated_at": row["created_at"], "snippet": snippet,
                "score": score, "mode": "lexical",
            }))
        scored.sort(key=lambda item: item[0], reverse=True)
        items = [item for _score, item in scored[:top_k]]
        return {
            "state": "ok" if items else "no_hits", "items": items,
            "mode": "lexical", "top_k": top_k,
            "vector_configured": False,
        }

    def _vector_rows(self,agent_id,conversation_id,category='',doc_id=''):
        with self._lock,self._connect() as conn:
            return [dict(r) for r in conn.execute("SELECT c.*,d.title,d.category,v.filename,x.vector,x.provider,x.model,x.dim,x.text_hash FROM kb_chunk c JOIN kb_document d ON d.doc_id=c.doc_id AND d.current_version=c.version_id JOIN kb_version v ON v.version_id=c.version_id LEFT JOIN kb_vector x ON x.chunk_id=c.chunk_id WHERE d.agent_id=? AND d.conversation_id=? AND d.enabled=1 AND d.deleted_at IS NULL AND (?='' OR d.category=?) AND (?='' OR d.doc_id=?) ORDER BY c.ordinal,c.chunk_id LIMIT 500",(agent_id,conversation_id,category,category,doc_id,doc_id)).fetchall()]

    async def build_vectors(self,*,agent_id,conversation_id,doc_id):
        client=await self._embedding_for(agent_id,conversation_id)
        if not client or not client.configured:return {'state':'not_configured'}
        key=(agent_id,conversation_id,doc_id);lock=self._vector_locks.setdefault(key,asyncio.Lock())
        if lock.locked():return {'state':'running'}
        async with lock:
            profile=client.provider_model
            rows=await asyncio.to_thread(self._vector_rows,agent_id,conversation_id,'',doc_id)
            rows=[r for r in rows if r['doc_id']==doc_id and (r['provider']!=profile or r['text_hash']!=r['sha256'] or r['dim']!=getattr(client._settings,'dimension',r['dim']))]
            written=0
            try:
                for i in range(0,len(rows),8):
                    batch=rows[i:i+8];vectors=await client.embed([x['text'] for x in batch])
                    def commit():
                        with self._lock,self._connect() as conn:
                            for row,vec in zip(batch,vectors):
                                conn.execute('INSERT OR REPLACE INTO kb_vector VALUES(?,?,?,?,?,?,?)',(row['chunk_id'],profile,client._settings.model,len(vec),row['sha256'],json.dumps(vec),_now()))
                    await asyncio.to_thread(commit);written+=len(batch)
            except Exception:return {'state':'failed','written':written}
            return {'state':'done','written':written}

    async def search_async(self,*,agent_id,conversation_id,query,limit=5,category=''):
        limit=max(1,min(int(limit),20));query=str(query)[:500]
        args={'agent_id':agent_id,'conversation_id':conversation_id,'query':query,'limit':limit,'category':category}
        lexical=await asyncio.to_thread(self.search,**args)
        cfg=await asyncio.to_thread(self.get_config,agent_id=agent_id,conversation_id=conversation_id)
        if lexical['state']=='disabled' or cfg['retrieval_mode']!='hybrid':return lexical
        client=await self._embedding_for(agent_id,conversation_id)
        if not client or not client.configured:return dict(lexical,degraded='embedding_not_configured')
        rows=await asyncio.to_thread(self._vector_rows,agent_id,conversation_id,category)
        rows=[r for r in rows if r['provider']==client.provider_model and r['text_hash']==r['sha256'] and r['vector'] and r['dim']==getattr(client._settings,'dimension',r['dim'])]
        if not rows:return dict(lexical,degraded='vector_index_missing')
        try:q=(await client.embed([query]))[0]
        except Exception:return dict(lexical,degraded='embedding_failed')
        scored=[]
        for row in rows:
            v=json.loads(row['vector'])
            if len(v)!=len(q):continue
            denom=math.sqrt(sum(x*x for x in v)*sum(x*x for x in q))
            if denom:scored.append((sum(a*b for a,b in zip(v,q))/denom,row))
        merged={x['chunk_id']:x for x in lexical['items']}
        for score,r in sorted(scored,key=lambda x:x[0],reverse=True)[:limit]:
            if score<=0:continue
            merged.setdefault(r['chunk_id'],{k:r[k] for k in ('chunk_id','doc_id','version_id','heading','ordinal','title','category','filename')}|{'snippet':r['text'][:250],'score':score,'mode':'vector'})
        items=list(merged.values())[:limit]
        return dict(lexical,items=items,state='ok' if items else 'no_hits',mode='hybrid',vector_configured=True)

    async def _embedding_for(self,agent_id,conversation_id):
        cfg=await asyncio.to_thread(self.get_config,agent_id=agent_id,conversation_id=conversation_id);profile=cfg.get('embedding')
        if not profile:return self._embedding
        from .ollama import OllamaEmbedding,is_local_ollama
        if is_local_ollama(profile['base_url']):return OllamaEmbedding(profile['base_url'],profile['model'],profile['dimension'])
        from pero.core.settings import EmbeddingsSettings
        from pero.memory.semantic import EmbeddingClient
        import httpx
        token=os.environ.get('PERO_MANAGEMENT_TOKEN','').strip()
        if not token:return None
        try:
            async with httpx.AsyncClient(trust_env=False,follow_redirects=False,timeout=5) as http:
                r=await http.get('http://127.0.0.1:8766/manage/internal/credentials/'+profile['credential_ref'],headers={'Authorization':'Bearer '+token});r.raise_for_status();key=r.json()['value']
            c=EmbeddingClient(EmbeddingsSettings(enabled=True,base_url=profile['base_url'],model=profile['model'],dimension=profile['dimension'],index_max_chars=4000));c._key=lambda:key;return c
        except Exception:return None


def _query_terms(query: str) -> List[str]:
    text = str(query or "").strip()
    if not text:
        return []
    tokens = re.findall(r"[\u4e00-\u9fff]{2,12}|[A-Za-z0-9_.-]{2,24}", text)
    stop = {"什么", "怎么", "哪些", "多少", "这个", "那个", "我们", "可以", "请问"}
    suffixes = ("在哪里", "在哪儿", "在哪", "是什么", "是啥", "怎么弄",
                "怎么办", "有哪些", "吗", "呢", "呀", "啊")
    terms: List[str] = []

    def add(value: str) -> None:
        if value and value not in stop and value not in terms and len(terms) < 10:
            terms.append(value)

    for token in tokens:
        if re.fullmatch(r"[\u4e00-\u9fff]+", token):
            body = token
            for suffix in suffixes:
                if body.endswith(suffix) and len(body) > len(suffix):
                    body = body[: -len(suffix)]
                    break
            if len(body) <= 4:
                add(body)
            else:
                # 中文长串用滑窗，保证"服务器地址在哪"能命中"服务器地址是…"
                for size in (4, 3):
                    for index in range(0, min(len(body) - size + 1, 6)):
                        add(body[index:index + size])
        else:
            add(token)
    if not terms and len(text) >= 2:
        add(text[:8])
    return terms


def _snippet(text: str, terms: Sequence[str], span: int = 160) -> str:
    for term in terms:
        index = text.find(term)
        if index >= 0:
            start = max(0, index - span // 3)
            return text[start:start + span].replace("\n", " ")
    return text[:span].replace("\n", " ")
