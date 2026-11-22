"""极简 JSON Schema 校验（只覆盖本阶段工具实际用到的子集）。

不引入 jsonschema 等新依赖：模型给的 arguments 是不可信字符串，
这里做类型、必填、未知字段、枚举和范围检查，足以拒绝幻觉参数。
"""

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


def validate_arguments(
    value: Any,
    schema: Mapping[str, Any],
    *,
    path: str = "arguments",
) -> List[str]:
    """返回错误说明列表；空列表表示通过。"""

    errors: List[str] = []
    _validate(value, schema, path, errors)
    return errors[:MAX_ERRORS]


def _validate(value: Any, schema: Any, path: str, errors: List[str]) -> None:
    if len(errors) >= MAX_ERRORS:
        return
    if not isinstance(schema, Mapping):
        return

    expected = schema.get("type")
    if isinstance(expected, str) and not _type_ok(value, expected):
        errors.append(f"{path} 类型应为 {expected}")
        return

    enum = schema.get("enum")
    if isinstance(enum, Sequence) and not isinstance(enum, (str, bytes)):
        if value not in list(enum):
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
        minimum_items = schema.get("minItems")
        if isinstance(minimum_items, int) and len(value) < minimum_items:
            errors.append(f"{path} 至少需要 {minimum_items} 项")
        maximum_items = schema.get("maxItems")
        if isinstance(maximum_items, int) and len(value) > maximum_items:
            errors.append(f"{path} 最多允许 {maximum_items} 项")

    if isinstance(value, str):
        minimum_length = schema.get("minLength")
        if isinstance(minimum_length, int) and len(value) < minimum_length:
            errors.append(f"{path} 长度不能小于 {minimum_length}")
        maximum_length = schema.get("maxLength")
        if isinstance(maximum_length, int) and len(value) > maximum_length:
            errors.append(f"{path} 长度不能超过 {maximum_length}")

    if isinstance(value, (int, float)) and not isinstance(value, bool):
        minimum = schema.get("minimum")
        if isinstance(minimum, (int, float)) and value < minimum:
            errors.append(f"{path} 不能小于 {minimum}")
        maximum = schema.get("maximum")
        if isinstance(maximum, (int, float)) and value > maximum:
            errors.append(f"{path} 不能大于 {maximum}")
