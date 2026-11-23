"""访问控制审计日志。"""

from __future__ import annotations

import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path


class AccessAuditLogger:
    """记录授权相关操作的审计日志。
    
    记录内容：
    - 开通码创建/兑换/撤销
    - 授权状态变更
    - 主人转移
    - 跨群访问配置变更
    
    不记录：
    - 码明文
    - 聊天内容
    - API凭据
    """

    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path), timeout=5)
        conn.row_factory = sqlite3.Row
        return conn

    def log(
        self,
        operation: str,
        actor_type: str,
        actor_ref: str,
        target_type: str,
        target_id: str,
        result: str = "success",
        error_code: str = "",
    ) -> str:
        """记录一条审计日志。
        
        Args:
            operation: create_token/redeem/revoke_token/update_grant/transfer_owner/update_group_access
            actor_type: admin/system/bridge/user
            actor_ref: 执行者标识（用户名/身份ID/系统组件）
            target_type: token/grant/owner/group_access
            target_id: 目标对象ID
            result: success/failed/blocked
            error_code: 失败时的错误代码
            
        Returns:
            log_id
        """
        log_id = uuid.uuid4().hex
        from pero.core.timebase import now_iso

        now = now_iso()

        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO access_audit_log
                (log_id, operation, actor_type, actor_ref, target_type, 
                 target_id, result, error_code, created_at)
                VALUES (?,?,?,?,?,?,?,?,?)
            """,
                (
                    log_id,
                    operation,
                    actor_type,
                    actor_ref,
                    target_type,
                    target_id,
                    result,
                    error_code,
                    now,
                ),
            )
            conn.commit()

        return log_id

    def query_logs(
        self,
        target_type: str = None,
        target_id: str = None,
        operation: str = None,
        limit: int = 100,
    ) -> list[dict]:
        """查询审计日志。"""
        with self._connect() as conn:
            query = "SELECT * FROM access_audit_log WHERE 1=1"
            params = []

            if target_type:
                query += " AND target_type=?"
                params.append(target_type)

            if target_id:
                query += " AND target_id=?"
                params.append(target_id)

            if operation:
                query += " AND operation=?"
                params.append(operation)

            query += " ORDER BY created_at DESC LIMIT ?"
            params.append(limit)

            rows = conn.execute(query, params).fetchall()
            return [dict(row) for row in rows]


__all__ = ["AccessAuditLogger"]
