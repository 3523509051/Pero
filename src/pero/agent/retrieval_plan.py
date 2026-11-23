"""确定性检索计划（不调用模型、不靠少量关键词一刀切关闭检索）。

一轮回复开始前，先按**可复核的规则**决定本轮要查哪些来源、查哪几层：

- 整句寒暄/应答：不查语义记忆、不查知识库；但明确回顾仍然必须查。
- 连续追问（"继续""然后呢"）：当前 L2 已装载的近期原文就能解释，不重复查。
- 明确回顾（"记得上次…"）：语义记忆（L3 事实 + L4 前情）+ 回顾检索（原文/事实/事件）。
- 人物定向（问某个成员的事）：语义记忆必须带上 L3 人物事实层。
- 知识资料：只有"已启用 + 自动检索开启 + 不像寒暄/不是纯继续"才查。
- 主人权限（私聊群内回顾）：与前三项独立，不受知识/语义开关影响。

计划同时给出每个来源的**原因**与**实际查询层**，本轮结果放进同一份快照，
续答（同一次运行内的工具后续请求）复用这份快照，不重复查同一来源。
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Dict, Tuple

#: 纯寒暄/应答：整句就是这些词，没有可检索的信息量。
_GREETINGS = {
    "你好", "您好", "在吗", "在么", "在不在", "哈喽", "hello", "hi", "嗨",
    "早", "早安", "午安", "晚安", "谢谢", "多谢", "好的", "好", "嗯",
    "嗯嗯", "收到", "ok", "okk", "行", "可以", "哈哈", "哈哈哈", "草",
    "？", "?", "。。。", "...", "666", "好滴", "好嘞", "明白", "了解",
}

#: 可重复/可叠用的寒暄词：整句由它们叠成才算寒暄（"好的好的""谢谢啦"）。
_GREETING_PIECES = (
    "好的", "好滴", "好嘞", "谢谢", "多谢", "收到", "明白", "了解",
    "哈哈", "嗯嗯", "ok",
)
#: 寒暄句尾的语气词，剥掉不影响判定。
_GREETING_TAILS = "啦呀啊哦了呢吧~～"

#: 纯继续/追问：没有新话题，当前 L2 已装载的近期原文足以解释。
_CONTINUATIONS = {
    "继续", "继续吧", "继续说", "接着说", "然后呢", "然后", "往下说",
    "还有呢", "再说说", "接着", "接着呢", "说下去",
}

#: 明确回顾的标志词（这类问题**不能**跳过检索）。
_RECALL_WORDS = (
    "记得", "还记得", "上次", "之前", "以前", "那天", "当时", "刚才",
    "说过", "提过", "聊过", "答应", "约定", "是不是说", "有没有说",
)

#: 人物定向：代词或直接问"谁"。
_PERSON_PRONOUNS = ("他", "她", "TA", "ta", "谁", "这个人", "那位", "哪位")
#: 人物属性问句（"Amy喜欢什么""小雨住哪"）：主语 + 属性词。
_PERSON_ATTR = re.compile(
    r"^[^\s，。！？,!?]{1,10}(喜欢|讨厌|爱吃|住|几岁|多大|什么星座|生日|"
    r"在哪|做什么|干什么|工作|学校|老家)"
)

#: 知识资料类提问的弱信号（仅用于"是否值得查知识库"，不用于关闭检索）。
_KNOWLEDGE_HINTS = (
    "怎么", "如何", "为什么", "是什么", "区别", "步骤", "教程", "资料",
    "文档", "规则", "流程", "配置", "报错", "原理", "推荐",
)

#: 记忆查询层：回顾/人物问题不再重复取原文（回顾检索负责原文）。
_LAYERS_WITH_RAW: Tuple[str, ...] = ("L3", "L4", "raw")
_LAYERS_FACTS_ONLY: Tuple[str, ...] = ("L3", "L4")


@dataclass(frozen=True)
class RetrievalPlan:
    """本轮检索计划：查哪些来源、各自原因、记忆查哪几层。"""

    memory: bool = False
    chat_recall: bool = False
    knowledge: bool = False
    owner: bool = False
    reasons: Dict[str, str] = field(default_factory=dict)
    greeting: bool = False
    memory_target: str = ""
    memory_layers: Tuple[str, ...] = _LAYERS_WITH_RAW

    def as_dict(self) -> Dict[str, Any]:
        return {
            "memory": self.memory,
            "chat_recall": self.chat_recall,
            "knowledge": self.knowledge,
            "owner": self.owner,
            "greeting": self.greeting,
            "memory_target": self.memory_target,
            "memory_layers": list(self.memory_layers),
            "reasons": dict(self.reasons),
        }


def _normalised(text: str) -> str:
    """去掉发言人前缀与空白，得到"这句话本身"。

    批内渲染会带 `[成员甲]` 或 `成员甲:` 前缀；不剥掉会把"谢谢啦"误判成
    带新话题的消息，从而对一个纯寒暄去查知识库。
    """

    value = str(text or "")
    value = re.sub(r"^\s*[\[【][^\]】]{1,20}[\]】]\s*", "", value)
    value = re.sub(r"^\s*[^\s:：\[\]【】]{1,12}\s*[:：]\s*", "", value)
    return "".join(value.split()).strip("，,。.！!~～")


def is_greeting(text: str) -> bool:
    """整句寒暄/应答判定：**整句**只是寒暄（可叠用）才成立。

    "好的路线呢""谢谢你的方案，第二个呢"都不是寒暄——它们带新话题，
    必须继续走检索；只有"好的""好的好的""谢谢啦""收到收到"这类整句应答才跳过。
    """

    value = _normalised(text)
    if not value:
        return True
    if value in _GREETINGS:
        return True
    if len(value) > 8:
        return False
    # 整句由寒暄词叠成（"好的好的""谢谢谢谢啦""嗯嗯"）。
    stripped = value
    changed = False
    for piece in _GREETING_PIECES:
        while stripped.startswith(piece):
            stripped = stripped[len(piece):]
            changed = True
    if not changed:
        return False
    stripped = stripped.strip(_GREETING_TAILS)
    return not stripped


def is_continuation(text: str) -> bool:
    """纯继续/追问：整句就是"继续""然后呢"，没有新的信息点。"""

    return _normalised(text) in _CONTINUATIONS


def is_recall_question(text: str) -> bool:
    value = str(text or "")
    return any(word in value for word in _RECALL_WORDS)


def is_person_question(text: str) -> bool:
    """是否在问某个具体人物（决定记忆检索必须带 L3 事实层）。"""

    value = _normalised(text)
    if not value:
        return False
    if any(word in value for word in _PERSON_PRONOUNS):
        return True
    return bool(_PERSON_ATTR.match(value))


def _has_ascii_term(value: str) -> bool:
    """短查询里出现英文/数字词（"DNS""API 报错"）通常是有信息量的提问。"""

    return bool(re.search(r"[A-Za-z0-9_]{2,}", value))


def plan_retrieval(
    text: str,
    *,
    private: bool,
    knowledge_enabled: bool,
    knowledge_automatic: bool,
    owner_available: bool,
) -> RetrievalPlan:
    """返回确定性检索计划；所有来源都给出原因，便于前端与审计核对。"""

    value = str(text or "")
    normalised = _normalised(value)
    greeting = is_greeting(value)
    recall = is_recall_question(value)
    continuation = is_continuation(value) and not recall
    person = is_person_question(value)
    reasons: Dict[str, str] = {}

    # 回顾问题优先：寒暄判定不能覆盖"记得…"这类明确回顾。
    # 纯继续由当前 L2 解释，不重复检索；人物问题必须查事实。
    if continuation:
        memory = False
        reasons["memory"] = "continuation_l2"
    elif recall:
        memory = True
        reasons["memory"] = "recall_question"
    elif person:
        memory = True
        reasons["memory"] = "person_directed"
    elif greeting:
        memory = False
        reasons["memory"] = "greeting_skip"
    else:
        memory = True
        reasons["memory"] = "topic_message"

    target = "recall" if recall else ("person_directed" if person else "")
    layers = _LAYERS_FACTS_ONLY if target else _LAYERS_WITH_RAW
    if target:
        # 人物/回顾：L3 事实 + L4 前情；原文交给回顾检索（避免同一段原文注入两次）。
        reasons["memory_layers"] = "facts_and_summary"
    else:
        reasons["memory_layers"] = "summary_and_raw"

    # 人物问题即使没有回顾词，也要走回顾检索拿事实与事件（同一份持久记录）。
    chat_recall = recall or person
    reasons["chat_recall"] = (
        "recall_question" if recall
        else ("person_directed" if person else "not_recall_question")
    )

    # 知识资料：启用 + 自动检索 + 非寒暄 + 非纯继续 + 有信息量。
    knowledge = False
    if not knowledge_enabled:
        reasons["knowledge"] = "disabled"
    elif not knowledge_automatic:
        reasons["knowledge"] = "automatic_off"
    elif continuation:
        reasons["knowledge"] = "continuation_l2"
    elif greeting and not recall:
        reasons["knowledge"] = "greeting_skip"
    elif (
        len(normalised) < 4
        and not _has_ascii_term(normalised)
        and not any(hint in value for hint in _KNOWLEDGE_HINTS)
    ):
        reasons["knowledge"] = "too_short"
    else:
        knowledge = True
        reasons["knowledge"] = "enabled_automatic"

    owner = bool(private and owner_available)
    reasons["owner"] = (
        "private_owner_recall" if owner
        else ("not_private" if not private else "owner_unavailable")
    )

    return RetrievalPlan(
        memory=memory,
        chat_recall=chat_recall,
        knowledge=knowledge,
        owner=owner,
        reasons=reasons,
        greeting=greeting and not recall,
        memory_target=target,
        memory_layers=layers,
    )


def plan_snapshot(plan: RetrievalPlan) -> Dict[str, Any]:
    """本轮计划快照（供状态接口与本轮结果合并展示）。"""

    return {
        "sources": plan.as_dict(),
        "queried": [],
        "skipped": [
            name for name, enabled in (
                ("memory", plan.memory),
                ("chat_recall", plan.chat_recall),
                ("knowledge", plan.knowledge),
            ) if not enabled
        ],
    }


__all__ = [
    "RetrievalPlan",
    "is_continuation",
    "is_greeting",
    "is_person_question",
    "is_recall_question",
    "plan_retrieval",
    "plan_snapshot",
]
