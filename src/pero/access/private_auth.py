"""私聊授权认证器：检查授权状态、主人身份、跨群访问权限。"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Optional


class PrivateChatAuthenticator:
    """私聊授权检查与权限管理。
    
    职责：
    - 检查是否有有效私聊授权
    - 判断是否为实例主人
    - 获取主人可访问的群列表
    - 更新授权状态（暂停/恢复/撤销）
    """

    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path), timeout=5)
        conn.row_factory = sqlite3.Row
        return conn

    def is_authorized(
        self, channel_id: str, account_id: str, user_ref: str
    ) -> Optional[dict]:
        """检查是否有有效私聊授权。
        
        返回:
            None (无授权) 或 {grant_id, conversation_id, agent_id, status, ...}
        """
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT grant_id, conversation_id, agent_id, status, 
                       identity_id, status_revision
                FROM private_chat_grant
                WHERE channel_id=? AND account_id=? AND user_ref=?
            """,
                (channel_id, account_id, user_ref),
            ).fetchone()

            if row and row["status"] == "active":
                return dict(row)
            return None

    def is_owner(self, identity_id: str) -> bool:
        """判断指定身份是否为实例主人。"""
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT 1 FROM instance_owner WHERE identity_id=?
            """,
                (identity_id,),
            ).fetchone()
            return bool(row)

    def is_owner_by_ref(
        self, channel_id: str, account_id: str, user_ref: str
    ) -> bool:
        """通过通道引用判断是否为主人。"""
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT 1 FROM instance_owner 
                WHERE channel_id=? AND account_id=? AND user_ref=?
            """,
                (channel_id, account_id, user_ref),
            ).fetchone()
            return bool(row)

    def get_owner_info(self) -> Optional[dict]:
        """获取主人信息。"""
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT o.*, i.wxid,
                       (SELECT alias FROM identity_alias 
                        WHERE identity_id=i.identity_id 
                        AND alias_type='display_name'
                        ORDER BY observed_at DESC LIMIT 1) as current_name
                FROM instance_owner o
                LEFT JOIN identity i ON o.identity_id=i.identity_id
            """
            ).fetchone()

            if row:
                return dict(row)
            return None

    def get_owner_accessible_groups(self) -> list[str]:
        """获取主人可访问的群conversation_id列表。"""
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT conversation_id FROM owner_group_access
                WHERE enabled=1
            """
            ).fetchall()
            return [row["conversation_id"] for row in rows]

    def get_owner_access_revision(self) -> int:
        """获取跨群访问配置的revision（用于缓存失效）。"""
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT MAX(access_revision) as max_rev 
                FROM owner_group_access
            """
            ).fetchone()
            return row["max_rev"] if row and row["max_rev"] else 0

    def list_grants(self, status_filter: Optional[str] = None) -> list[dict]:
        """列出所有私聊授权。"""
        with self._connect() as conn:
            if status_filter:
                rows = conn.execute(
                    """
                    SELECT g.*, i.wxid,
                           (SELECT alias FROM identity_alias 
                            WHERE identity_id=i.identity_id 
                            AND alias_type='display_name'
                            ORDER BY observed_at DESC LIMIT 1) as display_name,
                           (SELECT 1 FROM instance_owner 
                            WHERE identity_id=g.identity_id) as is_owner
                    FROM private_chat_grant g
                    LEFT JOIN identity i ON g.identity_id=i.identity_id
                    WHERE g.status=?
                    ORDER BY g.created_at DESC
                """,
                    (status_filter,),
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT g.*, i.wxid,
                           (SELECT alias FROM identity_alias 
                            WHERE identity_id=i.identity_id 
                            AND alias_type='display_name'
                            ORDER BY observed_at DESC LIMIT 1) as display_name,
                           (SELECT 1 FROM instance_owner 
                            WHERE identity_id=g.identity_id) as is_owner
                    FROM private_chat_grant g
                    LEFT JOIN identity i ON g.identity_id=i.identity_id
                    ORDER BY g.created_at DESC
                """
                ).fetchall()

            return [dict(row) for row in rows]

    def update_grant_status(
        self, grant_id: str, new_status: str, actor_ref: str = "admin"
    ) -> bool:
        """更新授权状态（active/paused/revoked）。
        
        Returns:
            True if updated, False if not found
        """
        from datetime import datetime, timezone

        if new_status not in ("active", "paused", "revoked"):
            raise ValueError(f"Invalid status: {new_status}")

        with self._connect() as conn:
            cursor = conn.execute(
                """
                UPDATE private_chat_grant 
                SET status=?, updated_at=?, status_revision=status_revision+1
                WHERE grant_id=?
            """,
                (new_status, datetime.now(timezone.utc).isoformat(), grant_id),
            )
            conn.commit()
            return cursor.rowcount > 0

    def update_owner_group_access(
        self, conversation_ids: list[str], actor_ref: str = "admin"
    ) -> int:
        """更新主人可访问的群（全量替换）。
        
        Returns:
            Number of groups now accessible
        """
        from datetime import datetime, timezone

        now = datetime.now(timezone.utc).isoformat()

        with self._connect() as conn:
            # 清空现有配置
            conn.execute("DELETE FROM owner_group_access")

            # 插入新配置
            for cid in conversation_ids:
                conn.execute(
                    """
                    INSERT INTO owner_group_access
                    (conversation_id, enabled, granted_at, updated_at, access_revision)
                    VALUES (?,1,?,?,1)
                """,
                    (cid, now, now),
                )

            conn.commit()
            return len(conversation_ids)

    def get_grant_by_conversation(self, conversation_id: str) -> Optional[dict]:
        """通过conversation_id获取grant信息。"""
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT * FROM private_chat_grant
                WHERE conversation_id=? AND status='active'
            """,
                (conversation_id,),
            ).fetchone()

            if row:
                return dict(row)
            return None


__all__ = ["PrivateChatAuthenticator"]
