"""插件包内轻量 JSON Schema 子集校验；不触发宿主工具包初始化。"""

from __future__ import annotations

from typing import Any, List, Mapping, Sequence

MAX_ERRORS = 5


def _type_ok(value: Any, expected: str) -> bool:
    if expected == "object":
        return isinstance(value, dict)
    if expected == "array":
        return isinstance(value, list)
    if expected == "string":
        return isinstance(value, str)
    if expected == "boolean":
        return isinstance(value, bool)
    if expected == "integer":
        return isinstance(value, int) and not isinstance(value, bool)
    if expected == "number":
        return isinstance(value, (int, float)) and not isinstance(value, bool)
    if expected == "null":
        return value is None
    return True


def validate_arguments(value: Any, schema: Mapping[str, Any], *, path: str = "arguments") -> List[str]:
    errors: List[str] = []
    _validate(value, schema, path, errors)
    return errors[:MAX_ERRORS]


def _validate(value: Any, schema: Any, path: str, errors: List[str]) -> None:
    if len(errors) >= MAX_ERRORS or not isinstance(schema, Mapping):
        return
    expected = schema.get("type")
    if isinstance(expected, str) and not _type_ok(value, expected):
        errors.append(f"{path} 类型应为 {expected}")
        return
    enum = schema.get("enum")
    if isinstance(enum, Sequence) and not isinstance(enum, (str, bytes)) and value not in list(enum):
        errors.append(f"{path} 取值不在允许集合内")
    if isinstance(value, dict):
        properties = schema.get("properties")
        properties = properties if isinstance(properties, Mapping) else {}
        required = schema.get("required")
        if isinstance(required, Sequence) and not isinstance(required, (str, bytes)):
            for key in required:
                if isinstance(key, str) and key not in value:
                    errors.append(f"{path}.{key} 为必填")
        additional = schema.get("additionalProperties", True)
        for key, item in value.items():
            if key in properties:
                _validate(item, properties[key], f"{path}.{key}", errors)
            elif additional is False:
                errors.append(f"{path}.{key} 不是允许的字段")
            elif isinstance(additional, Mapping):
                _validate(item, additional, f"{path}.{key}", errors)
    if isinstance(value, list):
        items = schema.get("items")
        if isinstance(items, Mapping):
            for index, item in enumerate(value):
                _validate(item, items, f"{path}[{index}]", errors)
        for key, label, compare in (("minItems", "至少需要", "min"), ("maxItems", "最多允许", "max")):
            limit = schema.get(key)
            if isinstance(limit, int) and ((compare == "min" and len(value) < limit)
                                           or (compare == "max" and len(value) > limit)):
                errors.append(f"{path} {label} {limit} 项")
    if isinstance(value, str):
        for key, label in (("minLength", "不能小于"), ("maxLength", "不能超过")):
            limit = schema.get(key)
            if isinstance(limit, int) and ((key == "minLength" and len(value) < limit)
                                           or (key == "maxLength" and len(value) > limit)):
                errors.append(f"{path} 长度{label} {limit}")
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        for key, label in (("minimum", "不能小于"), ("maximum", "不能大于")):
            limit = schema.get(key)
            if isinstance(limit, (int, float)) and ((key == "minimum" and value < limit)
                                                    or (key == "maximum" and value > limit)):
                errors.append(f"{path} {label} {limit}")
