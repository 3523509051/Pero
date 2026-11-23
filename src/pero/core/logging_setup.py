"""日志默认格式：时间一律显示北京时间并带明确时区偏移。

第三方库自己打印的原始诊断时间不做正则替换（保持原语义）；
这里只负责本项目 logger 的 `asctime`。
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

#: 默认格式：时间 + 级别 + logger + 消息。
DEFAULT_FORMAT = "%(asctime)s %(levelname)s %(name)s: %(message)s"


class BeijingFormatter(logging.Formatter):
    """把 `asctime` 渲染成北京时间（带 +0800），不受服务器本地时区影响。"""

    def formatTime(self, record, datefmt=None):  # noqa: N802 - 标准接口名
        from pero.core.timebase import to_beijing

        stamp = datetime.fromtimestamp(record.created, timezone.utc)
        converted = to_beijing(stamp) or stamp
        return converted.strftime(datefmt or "%Y-%m-%d %H:%M:%S%z")


def configure(level: int = logging.INFO, fmt: str = DEFAULT_FORMAT) -> None:
    """配置根 logger：北京时间格式，替换已有 handlers（重复调用安全）。"""

    handler = logging.StreamHandler()
    handler.setFormatter(BeijingFormatter(fmt))
    logging.basicConfig(level=level, handlers=[handler], force=True)


__all__ = ["BeijingFormatter", "DEFAULT_FORMAT", "configure"]
