"""确定性意图识别（不调模型、不猜测）。

只做两件事：
1. 判断"用户是否明确要求保存"——命中就把内容原样交出去，内容为空则要求补充，
   不做语义抽取、不脑补"大概想记什么"；
2. 判断"是否在问过去发生的事"——这类问题必须走检索；检索失败/没命中时要如实说明，
   不能包装成"从未发生"。

识别是基于固定短语与分句边界的确定性规则，宁可漏判也不乱判：
`remember` 工具仍可由模型主动调用，本模块只负责"保存必达"这条早于模型的路径。
"""

from __future__ import annotations

from typing import Optional, Tuple

#: 明确要求保存的短语（长的优先匹配，避免被更短的模式抢先）。
SAVE_PHRASES = (
    "帮我记住",
    "帮忙记住",
    "帮我记一下",
    "帮我记下",
    "帮我保存",
    "麻烦记住",
    "请记住",
    "记住一下",
    "记一下",
    "记下来",
    "保存一下",
    "存一下",
    "别忘了",
    "记住",
)

#: 句首/分句边界：只有出现在这些位置才算"请求"，避免把"我记住了"当保存请求。
_CLAUSE_BOUNDARY = set("，。；：、！？!?；\n\t 　”’）)")

#: 过去事实类问题（必须以检索为准）。
RECALL_HINTS = (
    "记得",
    "记不记得",
    "还记",
    "跟你说过",
    "我说过",
    "提到过",
    "讲过",
    "让你记",
    "存过",
    "上次",
    "上回",
    "之前",
    "以前",
    "昨天",
    "前天",
    "前天",
    "前几天",
)

#: 内容结尾要剥掉的标点。
_TRAILING = "。！？!?…；;，,、 \t\n\r」』"

_IDENTITY_HINTS = ("他", "她", "它", "这个人", "那个人", "对方")


def _collapse(text: str) -> str:
    return " ".join((text or "").split())


def detect_explicit_save(text: str) -> Optional[Tuple[str, str]]:
    """返回 ``(命中的短语, 要保存的内容)``；完全没有保存请求时返回 `None`。

    内容可能为空字符串——表示"用户确实要求保存但没说记什么"，
    调用方应明确要求补充，而不是保存一条空记录。
    """

    cleaned = _collapse(text)
    if not cleaned:
        return None
    for phrase in sorted(SAVE_PHRASES, key=len, reverse=True):
        start = 0
        while True:
            index = cleaned.find(phrase, start)
            if index < 0:
                break
            start = index + len(phrase)
            if index > 0 and cleaned[index - 1] not in _CLAUSE_BOUNDARY:
                continue
            rest = cleaned[index + len(phrase) :]
            if rest.startswith("了"):
                # "我记住了""记住了吗"是回顾/应答，不是保存请求。
                continue
            content = rest.lstrip("：:，,、 　").strip()
            content = content.strip(_TRAILING)
            if content in {"这个", "这件事", "这", "那个", "那件事"}:
                content = ""
            return phrase, content
    return None


def is_recall_question(text: str) -> bool:
    """是否在问"过去发生过什么/我说过什么"（这类问题必须走检索）。"""

    cleaned = _collapse(text)
    if not cleaned:
        return False
    return any(hint in cleaned for hint in RECALL_HINTS)


def mentions_identity(text: str) -> bool:
    """是否出现"他/她/对方"这类需要先稳定身份的说法。"""

    cleaned = _collapse(text)
    return any(hint in cleaned for hint in _IDENTITY_HINTS)


__all__ = [
    "RECALL_HINTS",
    "SAVE_PHRASES",
    "detect_explicit_save",
    "is_recall_question",
    "mentions_identity",
]
