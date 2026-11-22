"""一次性开通码生成与兑换服务。"""

from __future__ import annotations

import hashlib
import secrets
import sqlite3
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional


class ActivationCodeService:
    """管理一次性开通码的生成、兑换和撤销。
    
    规则：
    - 明文码只在生成时返回一次，后续只存储SHA256摘要
    - 一码一用，兑换后标记used_by_identity
    - 首个成功兑换者原子性绑定为instance_owner
    - 已消费码再次发送返回已有授权状态（幂等）
    """

    PREFIX = "PERO-"
    CODE_LENGTH = 24

    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path), timeout=5)
        conn.row_factory = sqlite3.Row
        return conn

    @staticmethod
    def generate_code() -> tuple[str, str]:
        """生成码明文和摘要，返回 (code, hash)。"""
        random_part = secrets.token_urlsafe(18)[: ActivationCodeService.CODE_LENGTH]
        code = f"{ActivationCodeService.PREFIX}{random_part}"
        code_hash = hashlib.sha256(code.encode()).hexdigest()
        return code, code_hash

    def create_token(
        self,
        channel_id: str,
        account_id: str,
        label: str = "",
        hours_valid: int = 24,
    ) -> dict:
        """创建新token，返回 {token_id, code (仅此一次), expires_at}。"""
        code, code_hash = self.generate_code()
        token_id = uuid.uuid4().hex
        now = datetime.now(timezone.utc)
        expires = now + timedelta(hours=hours_valid)

        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO activation_token 
                (token_id, token_hash, channel_id, account_id, label,
                 created_at, expires_at)
                VALUES (?,?,?,?,?,?,?)
            """,
                (
                    token_id,
                    code_hash,
                    channel_id,
                    account_id,
                    label,
                    now.isoformat(),
                    expires.isoformat(),
                ),
            )
            conn.commit()

        return {
            "token_id": token_id,
            "code": code,  # 明文仅此处返回
            "expires_at": expires.isoformat(),
        }

    def redeem_code(
        self,
        code: str,
        channel_id: str,
        account_id: str,
        user_ref: str,
        user_wxid: Optional[str],
        identity_id: str,
        agent_id: str,
        conversation_id: str,
    ) -> dict:
        """Redeem code with atomic authorization and possible owner binding.
        
        Rules:
        - First success redeemer becomes owner (atomic)
        - Consumed code by same user returns current grant status (idempotent)
        - Consumed code by different user rejected
        - Revoked status never auto-restored
        - user_wxid must be trustworthy for owner binding
        
        Returns: {success, error?, grant_id, is_owner, is_first_grant, role_info}
        """
        # Validate user_wxid for owner binding
        if not user_wxid or not isinstance(user_wxid, str) or len(user_wxid) < 2:
            return {
                "success": False,
                "error": "WeChat ID must be provided for private chat authorization",
            }
        
        code_hash = hashlib.sha256(code.encode()).hexdigest()

        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")

            try:
                # 1. 检查token有效性
                row = conn.execute(
                    """
                    SELECT token_id, expires_at, used_by_identity, revoked
                    FROM activation_token WHERE token_hash=?
                """,
                    (code_hash,),
                ).fetchone()

                if not row:
                    conn.rollback()
                    return {"success": False, "error": "开通码无效"}

                token_id, expires_at, used_by, revoked = row

                if revoked:
                    conn.rollback()
                    return {"success": False, "error": "开通码已撤销"}

                if datetime.fromisoformat(expires_at) < datetime.now(timezone.utc):
                    conn.rollback()
                    return {"success": False, "error": "开通码已过期"}

                # 2. 幂等检查：已被他人使用
                if used_by and used_by != identity_id:
                    conn.rollback()
                    return {"success": False, "error": "开通码已被他人使用"}

                # 3. 幂等检查：当前用户已有授权
                existing = conn.execute(
                    """
                    SELECT grant_id, status FROM private_chat_grant
                    WHERE channel_id=? AND account_id=? AND user_ref=?
                """,
                    (channel_id, account_id, user_ref),
                ).fetchone()

                is_first_grant = False
                if existing:
                    grant_id = existing[0]
                    current_status = existing[1]
                    # 已有授权：只返回当前状态，不修改 revoked/paused 状态
                    # 恢复需要管理员明确操作或新的有效码
                else:
                    grant_id = uuid.uuid4().hex
                    now = datetime.now(timezone.utc).isoformat()
                    conn.execute(
                        """
                        INSERT INTO private_chat_grant
                        (grant_id, channel_id, account_id, user_ref, user_wxid,
                         identity_id, agent_id, conversation_id, status,
                         granted_via, token_id, created_at, updated_at)
                        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                        (
                            grant_id,
                            channel_id,
                            account_id,
                            user_ref,
                            user_wxid,
                            identity_id,
                            agent_id,
                            conversation_id,
                            "active",
                            "activation_code",
                            token_id,
                            now,
                            now,
                        ),
                    )
                    is_first_grant = True

                # 4. 消费token (标记已使用)
                if not used_by:
                    conn.execute(
                        """
                        UPDATE activation_token
                        SET used_by_identity=?, used_at=?
                        WHERE token_id=?
                    """,
                        (identity_id, datetime.now(timezone.utc).isoformat(), token_id),
                    )

                # 5. 尝试绑定主人 (首次成功者)
                owner_exists = conn.execute(
                    "SELECT 1 FROM instance_owner LIMIT 1"
                ).fetchone()

                is_owner = False
                if not owner_exists:
                    now = datetime.now(timezone.utc).isoformat()
                    try:
                        # Get display_name
                        display_row = conn.execute(
                            """
                            SELECT alias FROM identity_alias
                            WHERE identity_id=? AND alias_type='display_name'
                            ORDER BY observed_at DESC LIMIT 1
                        """,
                            (identity_id,),
                        ).fetchone()
                        display_name = display_row[0] if display_row else None

                        conn.execute(
                            """
                            INSERT INTO instance_owner
                            (owner_id, channel_id, account_id, user_ref, user_wxid,
                             identity_id, grant_id, display_name, bound_at, updated_at)
                            VALUES ('singleton',?,?,?,?,?,?,?,?,?)
                        """,
                            (
                                channel_id,
                                account_id,
                                user_ref,
                                user_wxid,
                                identity_id,
                                grant_id,
                                display_name,
                                now,
                                now,
                            ),
                        )
                        is_owner = True
                    except sqlite3.IntegrityError as ie:
                        # Concurrent owner binding: only one can succeed (CHECK constraint)
                        # Re-check if owner was just created by other process
                        owner_now = conn.execute(
                            "SELECT 1 FROM instance_owner LIMIT 1"
                        ).fetchone()
                        if not owner_now:
                            # This is NOT a concurrent case - re-raise
                            raise ie
                        # Else: concurrent, silently continue (another process won)

                conn.commit()

                # Build role_info based on actual status
                if is_owner:
                    role_info = "You are the unique instance owner"
                elif is_first_grant:
                    role_info = "Private chat access enabled"
                else:
                    # Existing grant - return actual status
                    current_status = conn.execute(
                        "SELECT status FROM private_chat_grant WHERE grant_id=?",
                        (grant_id,),
                    ).fetchone()[0]
                    
                    if current_status == "active":
                        role_info = "Private chat access still active"
                    elif current_status == "paused":
                        role_info = "Private chat access paused by admin"
                    elif current_status == "revoked":
                        role_info = "Private chat access revoked"
                    else:
                        role_info = f"Status: {current_status}"

                return {
                    "success": True,
                    "grant_id": grant_id,
                    "is_owner": is_owner,
                    "is_first_grant": is_first_grant,
                    "role_info": role_info,
                }

            except Exception as e:
                conn.rollback()
                return {"success": False, "error": f"兑换失败: {str(e)}"}

    def revoke_token(self, token_id: str) -> bool:
        """撤销token（管理操作）。"""
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE activation_token 
                SET revoked=1, revoked_at=?
                WHERE token_id=?
            """,
                (datetime.now(timezone.utc).isoformat(), token_id),
            )
            conn.commit()
        return True

    def list_tokens(self, limit: int = 100) -> list[dict]:
        """列出token（不含明文）。"""
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT token_id, label, channel_id, account_id, created_at, 
                       expires_at, used_by_identity, used_at, revoked, revoked_at
                FROM activation_token 
                ORDER BY created_at DESC 
                LIMIT ?
            """,
                (limit,),
            ).fetchall()
            return [dict(row) for row in rows]


__all__ = ["ActivationCodeService"]
