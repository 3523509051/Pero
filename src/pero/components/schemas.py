"""组件的对外描述数据结构。

每个组件必须能报告：固定 ID、类型、版本、可配置字段、能力、依赖和真实状态。
状态值是“当前事实”，不是计划：
未实现的业务能力不得写成 ``available``。
"""

from __future__ import annotations

from enum import Enum
from typing import Any, Dict, List

from pydantic import BaseModel, Field


class ComponentType(str, Enum):
    """首版组件类型。新增类型需同步 core/contracts.py 的协议定义。"""

    CHANNEL = "channel_adapter"
    MODEL_PROVIDER = "model_provider"
    TOOL = "tool"
    MEMORY_REPOSITORY = "memory_repository"
    RETRIEVER = "retriever"
    CONVERSATION_POLICY = "conversation_policy"


COMPONENT_TYPE_DESCRIPTIONS: Dict[ComponentType, str] = {
    ComponentType.CHANNEL: "通道适配器：转入标准消息、执行标准发送、报告通道状态。",
    ComponentType.MODEL_PROVIDER: "模型提供方：接收标准请求，返回文本、工具调用与用量。",
    ComponentType.TOOL: "工具：参数定义、执行结果、取消与状态。",
    ComponentType.MEMORY_REPOSITORY: "记忆存储：原文、记忆与来源的保存和查询。",
    ComponentType.RETRIEVER: "检索器：按范围检索并返回证据与查询状态。",
    ComponentType.CONVERSATION_POLICY: "会话策略：唤醒、接话、合并与上下文组装规则。",
}


class ComponentStatus(str, Enum):
    """组件真实状态。"""

    #: 已实现且当前可用（本阶段仅有“只构造请求、不联网”的组件）。
    AVAILABLE = "available"
    #: 已实现但只能构造请求，尚未执行网络调用（例如本阶段的模型请求构造器）。
    CONSTRUCTION_ONLY = "construction_only"
    #: 协议已定义，实现尚未开始。
    NOT_IMPLEMENTED = "not_implemented"
    #: 有实现但外部依赖未连接（例如通道未接入）。
    NOT_CONNECTED = "not_connected"
    #: 有实现但尚未初始化数据或运行态（例如记忆库未建、凭据未配置）。
    NOT_INITIALIZED = "not_initialized"
    #: 组件已实现、配置齐全，但尚未与真实远端核对过（不能算连接成功）。
    NOT_VERIFIED = "not_verified"


class ConfigFieldDescription(BaseModel):
    """可配置字段说明。默认值仅供展示，真实凭据一律不返回。"""

    name: str
    type: str
    required: bool = False
    default: Any = None
    description: str = ""
    secret: bool = False


class ComponentDescriptor(BaseModel):
    id: str
    type: ComponentType
    version: str
    description: str
    config_fields: List[ConfigFieldDescription] = Field(default_factory=list)
    capabilities: Dict[str, Any] = Field(default_factory=dict)
    dependencies: List[str] = Field(default_factory=list)
    status: ComponentStatus
    status_detail: str | None = None
