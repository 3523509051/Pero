"""pero：独立 Agent 后端。

P05 起增加独立 SQLite 持久记忆：入站原文、身份别名、L2 轮次与 L20 工具证据。
P06 支持明确保存与检索，P07 增加默认关闭的 L2 总结归档，P11 增加会话行为与高德工具，
P12 增加表情资产与文字/图片投递适配（默认关闭），P13 增加桥上下文扩展与文件补传（默认关闭）。
仍不含 L3 人物画像、语义检索、前端与部署。
"""

from __future__ import annotations

__version__ = "0.13.0"

#: 当前实施阶段编号，直接出现在 /health 与 /api/v1/capabilities 中。
PHASE = "P13"

__all__ = ["__version__", "PHASE"]
