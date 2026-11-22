"""回复末尾表情标记解析（P12）：严格解析，不宽泛 replace。

语义照搬旧插件（`main.py:1972-1975`），并按 R01 补修扩展：
- 规范语法 `[[表情:ID]]` / `[[sticker:ID]]`（re.I），捕获体 ≤60 字符且不含
  `]`/换行；也兼容模型实际输出的单层 `[表情:sa-<hex32>]` / `[sticker:sa-…]`
  （单层只接受合法资产 ID 形态，不做自然语言容错）；
- 双层与单层匹配都以**原始字符串位置**为准，统一按出现顺序排序，混合写法
  `[表情:A][[表情:B]]` 不会变成 B、A；双层内部的单层重叠匹配丢弃；
- 清理与排序使用同一组匹配（逐段拼接剩余正文），不出现"剥离顺序"与
  "发送顺序"不一致；
- 一条回复可带多个标记：按出现顺序组成 `targets` 列表（去重由调用方按
  `max_stickers_per_reply` 处理，保留原文首次出现位置）；
- 模型只能引用 `list_stickers` 返回的稳定资产 ID（`sa-<hex32>`）；
  名字不做模糊匹配发送。
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import List, Optional, Tuple

#: 双层标记：捕获体宽松（≤60 字符），后续按资产 ID 严格校验。
MARKER = re.compile(
    r"\[\[\s*(?:表情|sticker)\s*[:：]\s*([^\]\n]{1,60}?)\s*\]\]",
    re.IGNORECASE,
)

#: 单层标记：仅接受稳定资产 ID，前后不得紧邻方括号（排除双层标记内层）。
SINGLE_MARKER = re.compile(
    r"(?<!\[)\[\s*(?:表情|sticker)\s*[:：]\s*(sa-[0-9a-fA-F]{32})\s*\](?!\])",
    re.IGNORECASE,
)


@dataclass
class MarkerScan:
    """一次标记扫描的结果。"""

    #: 第一个标记捕获体（原始文本，未 lstrip("#")）；无标记时为 ""。
    target: str = ""
    #: 正文中出现的标记总数（含第一个）。
    markers: int = 0
    #: 剥掉全部标记后的正文（首尾空白已清理）。
    cleaned: str = ""
    #: 全部标记捕获体（按原文出现顺序，保留重复项，去重由调用方处理）。
    targets: List[str] = field(default_factory=list)


def _hits(raw: str) -> List[Tuple[int, int, str]]:
    """收集（起, 止, 捕获体），按原始位置排序；丢弃双层内部的单层重叠。"""

    double_hits = [
        (match.start(), match.end(), match.group(1).strip())
        for match in MARKER.finditer(raw)
    ]
    consumed = [(start, end) for start, end, _ in double_hits]
    single_hits = []
    for match in SINGLE_MARKER.finditer(raw):
        start, end = match.start(), match.end()
        if any(start >= inner_start and end <= inner_end for inner_start, inner_end in consumed):
            continue
        single_hits.append((start, end, match.group(1).strip()))
    return sorted(double_hits + single_hits, key=lambda item: item[0])


def scan_markers(text: str) -> Optional[MarkerScan]:
    """解析并剥离标记；没有标记返回 None（正文原样使用，不做替换）。"""

    raw = text or ""
    hits = _hits(raw)
    if not hits:
        return None
    pieces: List[str] = []
    cursor = 0
    for start, end, _ in hits:
        pieces.append(raw[cursor:start])
        cursor = end
    pieces.append(raw[cursor:])
    targets = [target for _, _, target in hits]
    return MarkerScan(
        target=targets[0],
        markers=len(targets),
        cleaned="".join(pieces).strip(),
        targets=targets,
    )
