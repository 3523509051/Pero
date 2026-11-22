"""表情资产存储（P12）：独立新库，元数据由 Pero 负责。

边界：
- 独立 sqlite（默认 `data/sticker-assets.sqlite3`），schema v1，不与主记忆库混表；
- 稳定资产 ID `sa-<hex32>`，**不复用**旧桥 st_xxxx 或图片缓存数字 ID；
- 原图只引用 Pero 资源目录（resource_id/version/sha256），不接受磁盘路径；
- 默认 Agent+会话隔离，检索/修改全部按作用域过滤；跨群共享首版不做；
- 事务写入失败如实抛错，调用方不能声称已入库；资源原件不因标签修改删除。
"""

from __future__ import annotations

import json
import re
import sqlite3
import time
import uuid
from contextlib import closing
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

_ASSET_ID = re.compile(r"sa-[0-9a-f]{32}")
_MAX_NAME = 60
_MAX_TEXT = 200
_MAX_TAGS = 8

SCHEMA_VERSION = 1


class StickerStoreError(Exception):
    """受控表情存储错误；`code` 不携带路径或上游文本。"""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


def valid_asset_id(value: Any) -> Optional[str]:
    """校验资产 ID 形态；不合法返回 None（调用方不得模糊匹配兜底）。"""

    if isinstance(value, str) and _ASSET_ID.fullmatch(value.strip()):
        return value.strip()
    return None


def _clip_text(value: Any, limit: int) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    return text[:limit]


def _normalize_tags(tags: Sequence[str]) -> List[str]:
    result: List[str] = []
    for tag in tags or ():
        text = _clip_text(tag, 30)
        if text and text not in result:
            result.append(text)
        if len(result) >= _MAX_TAGS:
            break
    return result


class StickerAssetStore:
    """按 `(agent_id, conversation_id)` 隔离的表情资产元数据表。"""

    def __init__(self, path: Path) -> None:
        self.path = Path(path)

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.path), timeout=3)
        conn.row_factory = sqlite3.Row
        return conn

    def initialize(self) -> None:
        """建库/升级（幂等；只增不改，不删库重建）。"""

        self.path.parent.mkdir(parents=True, exist_ok=True)
        with closing(self._connect()) as conn, conn:
            version = conn.execute("PRAGMA user_version").fetchone()[0]
            if version not in (0, SCHEMA_VERSION):
                raise StickerStoreError("unsupported_schema")
            conn.execute(
                "CREATE TABLE IF NOT EXISTS sticker_asset ("
                " asset_id TEXT PRIMARY KEY,"
                " agent_id TEXT NOT NULL,"
                " conversation_id TEXT NOT NULL,"
                " name TEXT NOT NULL,"
                " emotion TEXT NOT NULL DEFAULT '',"
                " scene TEXT NOT NULL DEFAULT '',"
                " tags_json TEXT NOT NULL DEFAULT '[]',"
                " description TEXT NOT NULL DEFAULT '',"
                " source_resource_id TEXT NOT NULL,"
                " source_version INTEGER NOT NULL,"
                " source_sha256 TEXT NOT NULL,"
                " source_event_id TEXT NOT NULL DEFAULT '',"
                " status TEXT NOT NULL DEFAULT 'active',"
                " created_at REAL NOT NULL,"
                " updated_at REAL NOT NULL,"
                " UNIQUE(agent_id, conversation_id, name))"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS sticker_scope "
                "ON sticker_asset(agent_id, conversation_id, created_at)"
            )
            conn.execute(f"PRAGMA user_version={SCHEMA_VERSION}")

    # ------------------------------------------------------------------ #
    # 写入
    # ------------------------------------------------------------------ #

    def save(
        self,
        *,
        agent_id: str,
        conversation_id: str,
        name: str,
        source_resource_id: str,
        source_version: int,
        source_sha256: str,
        source_event_id: str = "",
        emotion: str = "",
        scene: str = "",
        tags: Sequence[str] = (),
        description: str = "",
    ) -> Dict[str, Any]:
        """入库（幂等语义：同作用域同名直接返回既有资产，不覆盖）。

        返回资产元数据；唯一键冲突（同作用域同名）复用原资产而不是报错。
        """

        clean_name = _clip_text(name, _MAX_NAME)
        if not clean_name:
            raise StickerStoreError("name_required")
        now = time.time()
        with closing(self._connect()) as conn, conn:
            conn.execute("BEGIN IMMEDIATE")
            existing = conn.execute(
                "SELECT * FROM sticker_asset "
                "WHERE agent_id=? AND conversation_id=? AND name=?",
                (agent_id, conversation_id, clean_name),
            ).fetchone()
            if existing is not None:
                return self._metadata(existing)
            asset_id = "sa-" + uuid.uuid4().hex
            conn.execute(
                "INSERT INTO sticker_asset ("
                " asset_id, agent_id, conversation_id, name, emotion, scene,"
                " tags_json, description, source_resource_id, source_version,"
                " source_sha256, source_event_id, status, created_at, updated_at"
                ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    asset_id,
                    agent_id,
                    conversation_id,
                    clean_name,
                    _clip_text(emotion, 40),
                    _clip_text(scene, 60),
                    json.dumps(_normalize_tags(tags), ensure_ascii=False),
                    _clip_text(description, _MAX_TEXT),
                    source_resource_id,
                    int(source_version),
                    source_sha256,
                    _clip_text(source_event_id, 120),
                    "active",
                    now,
                    now,
                ),
            )
            row = conn.execute(
                "SELECT * FROM sticker_asset WHERE asset_id=?", (asset_id,)
            ).fetchone()
        return self._metadata(row)

    def rename(
        self,
        *,
        agent_id: str,
        conversation_id: str,
        asset_id: str,
        name: Optional[str] = None,
        emotion: Optional[str] = None,
        scene: Optional[str] = None,
        add_tags: Sequence[str] = (),
        remove_tags: Sequence[str] = (),
        description: Optional[str] = None,
    ) -> Dict[str, Any]:
        """只改当前作用域内资产的元数据；同名冲突明确失败，不覆盖他人资产。"""

        checked = valid_asset_id(asset_id)
        if checked is None:
            raise StickerStoreError("invalid_asset_id")
        updates: List[tuple] = []
        if name is not None:
            clean = _clip_text(name, _MAX_NAME)
            if not clean:
                raise StickerStoreError("name_required")
            updates.append(("name", clean))
        if emotion is not None:
            updates.append(("emotion", _clip_text(emotion, 40)))
        if scene is not None:
            updates.append(("scene", _clip_text(scene, 60)))
        if description is not None:
            updates.append(("description", _clip_text(description, _MAX_TEXT)))
        with closing(self._connect()) as conn, conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT * FROM sticker_asset WHERE asset_id=? AND agent_id=? "
                "AND conversation_id=?",
                (checked, agent_id, conversation_id),
            ).fetchone()
            if row is None:
                raise StickerStoreError("sticker_not_found")
            if any(field == "name" and value == row["name"] for field, value in updates):
                updates = [u for u in updates if u[0] != "name"]
            if updates:
                for field, value in updates:
                    if field == "name":
                        clash = conn.execute(
                            "SELECT 1 FROM sticker_asset WHERE agent_id=? AND "
                            "conversation_id=? AND name=? AND asset_id<>?",
                            (agent_id, conversation_id, value, checked),
                        ).fetchone()
                        if clash is not None:
                            raise StickerStoreError("name_conflict")
                sets = ", ".join(f"{field}=?" for field, _ in updates)
                conn.execute(
                    f"UPDATE sticker_asset SET {sets}, updated_at=? WHERE asset_id=?",
                    (*[value for _, value in updates], time.time(), checked),
                )
            tags = _normalize_tags(
                [t for t in json.loads(row["tags_json"]) if t not in set(remove_tags)]
                + list(add_tags)
            )
            conn.execute(
                "UPDATE sticker_asset SET tags_json=?, updated_at=? WHERE asset_id=?",
                (json.dumps(tags, ensure_ascii=False), time.time(), checked),
            )
            fresh = conn.execute(
                "SELECT * FROM sticker_asset WHERE asset_id=?", (checked,)
            ).fetchone()
        return self._metadata(fresh)

    def set_status(
        self, *, agent_id: str, conversation_id: str, asset_id: str, status: str
    ) -> None:
        if status not in ("active", "disabled"):
            raise StickerStoreError("invalid_status")
        checked = valid_asset_id(asset_id)
        if checked is None:
            raise StickerStoreError("invalid_asset_id")
        with closing(self._connect()) as conn, conn:
            cur = conn.execute(
                "UPDATE sticker_asset SET status=?, updated_at=? WHERE asset_id=? "
                "AND agent_id=? AND conversation_id=?",
                (status, time.time(), checked, agent_id, conversation_id),
            )
            if cur.rowcount == 0:
                raise StickerStoreError("sticker_not_found")

    # ------------------------------------------------------------------ #
    # 检索（有界、只元数据）
    # ------------------------------------------------------------------ #

    def find(
        self,
        *,
        agent_id: str,
        conversation_id: str,
        asset_id: Optional[str] = None,
        query: str = "",
        emotion: str = "",
        scene: str = "",
        limit: int = 20,
        offset: int = 0,
        include_disabled: bool = False,
    ) -> List[Dict[str, Any]]:
        """按作用域有界检索；按 ID 精确查时只给同作用域资产。

        排序稳定（created_at DESC, asset_id），`offset` 真实分页（配合
        `count_filtered` 计算 total/has_more）；禁用/跨作用域资产不混入。
        """

        limit = max(1, min(int(limit), 100))
        offset = max(0, min(int(offset), 10_000))
        clauses = ["agent_id=?", "conversation_id=?"]
        params: List[Any] = [agent_id, conversation_id]
        if not include_disabled:
            clauses.append("status='active'")
        if asset_id is not None:
            checked = valid_asset_id(asset_id)
            if checked is None:
                return []
            clauses.append("asset_id=?")
            params.append(checked)
        text = _clip_text(query, 60)
        if text:
            clauses.append("(name LIKE ? OR description LIKE ? OR tags_json LIKE ?)")
            like = f"%{text}%"
            params.extend([like, like, like])
        emo = _clip_text(emotion, 40)
        if emo:
            clauses.append("emotion LIKE ?")
            params.append(f"%{emo}%")
        scen = _clip_text(scene, 60)
        if scen:
            clauses.append("scene LIKE ?")
            params.append(f"%{scen}%")
        where = " WHERE " + " AND ".join(clauses)
        sql = (
            "SELECT * FROM sticker_asset" + where +
            " ORDER BY created_at DESC, asset_id LIMIT ? OFFSET ?"
        )
        query_params = [*params, limit, offset]
        with closing(self._connect()) as conn:
            rows = conn.execute(sql, query_params).fetchall()
        return [self._metadata(row) for row in rows]

    def count_filtered(
        self,
        *,
        agent_id: str,
        conversation_id: str,
        asset_id: Optional[str] = None,
        query: str = "",
        emotion: str = "",
        scene: str = "",
        include_disabled: bool = False,
    ) -> int:
        """按与 find 相同的筛选条件计数（total/has_more 用），不带分页。"""

        clauses = ["agent_id=?", "conversation_id=?"]
        params: List[Any] = [agent_id, conversation_id]
        if not include_disabled:
            clauses.append("status='active'")
        if asset_id is not None:
            checked = valid_asset_id(asset_id)
            if checked is None:
                return 0
            clauses.append("asset_id=?")
            params.append(checked)
        text = _clip_text(query, 60)
        if text:
            clauses.append("(name LIKE ? OR description LIKE ? OR tags_json LIKE ?)")
            like = f"%{text}%"
            params.extend([like, like, like])
        emo = _clip_text(emotion, 40)
        if emo:
            clauses.append("emotion LIKE ?")
            params.append(f"%{emo}%")
        scen = _clip_text(scene, 60)
        if scen:
            clauses.append("scene LIKE ?")
            params.append(f"%{scen}%")
        with closing(self._connect()) as conn:
            row = conn.execute(
                "SELECT COUNT(*) FROM sticker_asset WHERE " + " AND ".join(clauses),
                params,
            ).fetchone()
        return int(row[0])

    def get_by_name(
        self, *, agent_id: str, conversation_id: str, name: str
    ) -> Optional[Dict[str, Any]]:
        """同作用域同名精确查找（保存幂等复用用）。"""

        clean = _clip_text(name, _MAX_NAME)
        if not clean:
            return None
        with closing(self._connect()) as conn:
            row = conn.execute(
                "SELECT * FROM sticker_asset WHERE agent_id=? AND conversation_id=? AND name=?",
                (agent_id, conversation_id, clean),
            ).fetchone()
        return self._metadata(row) if row is not None else None

    def count(self, *, agent_id: str, conversation_id: str) -> int:
        with closing(self._connect()) as conn:
            row = conn.execute(
                "SELECT COUNT(*) FROM sticker_asset WHERE agent_id=? AND conversation_id=?",
                (agent_id, conversation_id),
            ).fetchone()
        return int(row[0])

    # ------------------------------------------------------------------ #

    @staticmethod
    def _metadata(row: sqlite3.Row) -> Dict[str, Any]:
        return {
            "asset_id": str(row["asset_id"]),
            "name": str(row["name"]),
            "emotion": str(row["emotion"]),
            "scene": str(row["scene"]),
            "tags": list(json.loads(row["tags_json"])),
            "description": str(row["description"]),
            "source_resource_id": str(row["source_resource_id"]),
            "source_version": int(row["source_version"]),
            "source_sha256": str(row["source_sha256"]),
            "status": str(row["status"]),
            "created_at": float(row["created_at"]),
            "updated_at": float(row["updated_at"]),
        }
