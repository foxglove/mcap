from typing import Any, Dict, List, Tuple, Union

from .records import McapRecord

ValueType = Union[bytes, str, int, List[Tuple[str, str]], Dict[str, str]]


def normalize_value(value: ValueType, value_type: List[str]) -> Any:
    if isinstance(value, bytes):
        return list(value)
    else:
        return value


def stringify_record(record: McapRecord):
    fields = [
        (k, normalize_value(v, ["any", "any"]))
        for k, v in sorted(record.__dict__.items())
    ]
    return {"type": type(record).__name__, "fields": fields}
