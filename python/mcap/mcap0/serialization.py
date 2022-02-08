from typing import Any, Dict, List, Tuple, Union
from .records import McapRecord

ValueType = Union[bytes, str, int, List[Tuple[str, str]], Dict[str, str]]


def type_prefix(value: Any, value_type: List[str]) -> str:
    if isinstance(value, dict):
        return "m:"
    if isinstance(value, list):
        return "a:"
    if isinstance(value, bytes):
        return "b:"
    if isinstance(value, int):
        if value_type == ["int"]:
            return "i:"
        else:
            return "n:"
    if isinstance(value, str):
        return "s:"
    raise Exception(f"Unknown type: {value}.")


def stringify_key_value_list(list: List[Tuple[str, str]], types: List[str]) -> str:
    items = sorted(list, key=lambda kv: kv[0])
    return (
        "{"
        + ",".join(
            [
                f"{stringify_typed_value(k, [types[0]])}={stringify_typed_value(v, [types[1]])}"
                for k, v in items
            ]
        )
        + "}"
    )


def stringify_value(value: ValueType, value_type: List[str]) -> str:
    if isinstance(value, bytes):
        values = ["{0:#0{1}}".format(b, 2) for b in value]
        return "".join(["<", *values, ">"])
    if isinstance(value, dict):
        return stringify_key_value_list(list(value.items()), value_type)
    if isinstance(value, list):
        return stringify_key_value_list(value, value_type)
    if isinstance(value, str):
        return '""' if len(value) == 0 else value
    else:
        return str(value)


def stringify_typed_value(value: Any, value_type: List[str]) -> str:
    prefix = type_prefix(value, value_type)
    return prefix + stringify_value(value, value_type)


def stringify_record(record: McapRecord):
    items = sorted(record.__dict__.items(), key=lambda kv: kv[0])
    name = type(record).__name__
    fields: List[str] = []
    for k, v in items:
        value_type = record.__dataclass_fields__[k].metadata.get("value_type") or [
            "any",
            "any",
        ]
        fields.append(f"{k}={stringify_typed_value(v, value_type)}")
    return " ".join([name, *fields])
