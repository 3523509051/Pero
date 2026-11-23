"""组件注册表：按固定 ID 登记、查询并报告真实状态。

注册表是“可换实现而不改核心”的唯一入口：
- 固定 ID 唯一，重复登记直接报错，不做静默覆盖；
- 描述与实例一起登记，状态来自描述本身；
- 查询结果按 (类型, ID) 稳定排序，便于前端与检查者比对。
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from pero.components.schemas import ComponentDescriptor, ComponentType


class ComponentRegistryError(Exception):
    """注册表相关错误基类。"""


class DuplicateComponentError(ComponentRegistryError):
    """同一 ID 被重复登记。"""


class UnknownComponentError(ComponentRegistryError):
    """查询未登记的组件 ID。"""


def _registered_at() -> str:
    """注册时间用北京时间（带 +08:00），不用服务器本地时区。"""

    from pero.core.timebase import now_iso

    return now_iso()


@dataclass(frozen=True)
class RegisteredComponent:
    descriptor: ComponentDescriptor
    instance: Any = None
    registered_at: str = field(default_factory=lambda: _registered_at())

    @property
    def has_instance(self) -> bool:
        return self.instance is not None


class ComponentRegistry:
    """进程内的组件登记表。线程安全，只登记不热替换。"""

    def __init__(self) -> None:
        self._items: Dict[str, RegisteredComponent] = {}
        self._lock = threading.RLock()

    def register(
        self,
        descriptor: ComponentDescriptor,
        instance: Any = None,
    ) -> RegisteredComponent:
        component_id = descriptor.id
        with self._lock:
            if component_id in self._items:
                raise DuplicateComponentError(f"组件 ID 已登记，不允许覆盖：{component_id}")
            registered = RegisteredComponent(descriptor=descriptor, instance=instance)
            self._items[component_id] = registered
            return registered

    def get(self, component_id: str) -> RegisteredComponent:
        with self._lock:
            try:
                return self._items[component_id]
            except KeyError as exc:
                raise UnknownComponentError(f"未登记的组件 ID：{component_id}") from exc

    def get_optional(self, component_id: Optional[str]) -> Optional[RegisteredComponent]:
        if not component_id:
            return None
        with self._lock:
            return self._items.get(component_id)

    def has(self, component_id: str) -> bool:
        with self._lock:
            return component_id in self._items

    def all(self, component_type: Optional[ComponentType] = None) -> List[RegisteredComponent]:
        with self._lock:
            items = list(self._items.values())
        if component_type is not None:
            items = [item for item in items if item.descriptor.type == component_type]
        return sorted(items, key=lambda item: (item.descriptor.type.value, item.descriptor.id))

    def count_by_type(self) -> Dict[str, int]:
        counts: Dict[str, int] = {member.value: 0 for member in ComponentType}
        for item in self.all():
            counts[item.descriptor.type.value] += 1
        return counts

    def __len__(self) -> int:
        with self._lock:
            return len(self._items)

    def __contains__(self, component_id: object) -> bool:
        return isinstance(component_id, str) and self.has(component_id)

    def __repr__(self) -> str:  # pragma: no cover - 调试用
        return f"ComponentRegistry(ids={sorted(self._items)!r})"
