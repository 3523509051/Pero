"""关键词切分与打分（本阶段检索的确定性实现）。

目的：中文没有空格，不能把整句当成"完全相同字符串"去比较。这里的策略是：

- 英文/数字按词切分，取长度 ≥2 的小写词；
- 中文按**连续汉字串切成二元组（bigram）**，单个汉字时保留该字；
- 查询与文本都做小写化，SQL 侧用参数化 `LIKE %token%` 做"任一命中"过滤，
  再在 Python 里按命中 token 数打分排序。

不引入 FTS/向量库、不联网、不做同义词扩展；词表完全来自用户输入本身。
"""

from __future__ import annotations

import re
from typing import List, Sequence, Tuple

#: 连续汉字/日文假名（作为二元组切分单位）。
_CJK_RUN = re.compile(r"[\u3400-\u4dbf\u4e00-\u9fff\u3040-\u30ff]+")
#: 英文/数字词。
_WORD = re.compile(r"[0-9A-Za-z_]+")
#: 中文标点与常见分隔符。
_SEPARATORS = re.compile(
    r"[\s,，、。！？；：.!?;:\"'“”‘’（）()\[\]【】<>《》/\\|+*=~`@#$%^&]+"
)

MAX_TOKENS = 8


def normalize(text: str) -> str:
    """小写化并压缩空白，用于打分比较（不改动原文）。"""

    return " ".join((text or "").lower().split())


def tokenize(text: str, *, max_tokens: int = MAX_TOKENS) -> Tuple[str, ...]:
    """把查询切成关键词：英文词 + 中文二元组（去重、保序、有上限）。"""

    raw = _SEPARATORS.sub(" ", (text or "").strip())
    tokens: List[str] = []
    for word in _WORD.findall(raw):
        token = word.lower().strip("_")
        if len(token) >= 2:
            tokens.append(token)
    for run in _CJK_RUN.findall(raw):
        if len(run) == 1:
            tokens.append(run)
            continue
        for index in range(len(run) - 1):
            tokens.append(run[index : index + 2])

    ordered: List[str] = []
    for token in tokens:
        if token and token not in ordered:
            ordered.append(token)
        if len(ordered) >= max_tokens:
            break
    return tuple(ordered)


def like_clause(column: str, tokens: Sequence[str]) -> Tuple[str, List[str]]:
    """构造参数化的"任一 token 命中"条件（`column` 只接受内部列名）。"""

    if not tokens:
        return "(1=0)", []
    parts = [f"{column} LIKE ?" for _ in tokens]
    params = [f"%{token}%" for token in tokens]
    return "(" + " OR ".join(parts) + ")", params


def score(text: str, tokens: Sequence[str]) -> int:
    """命中 token 数（0 表示未命中）；用于稳定排序的第一关键字。"""

    haystack = normalize(text)
    if not haystack:
        return 0
    return sum(1 for token in tokens if token and token in haystack)


__all__ = ["MAX_TOKENS", "like_clause", "normalize", "score", "tokenize"]
