"""受控插件错误：错误码固定，消息不携带路径、凭据或远端错误体。"""

from __future__ import annotations


class PluginError(Exception):
    """插件安装、注册、执行和生命周期的唯一对外错误类型。"""

    def __init__(self, code: str, message: str = "") -> None:
        self.code = str(code)[:64]
        self.message = str(message or code)[:200]
        super().__init__(self.code)

    def public(self) -> dict:
        return {"code": self.code, "message": self.message}
