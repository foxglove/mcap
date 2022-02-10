from typing import Any, Dict, Union

from .records import McapRecord

ValueType = Union[bytes, str, int, Dict[str, str]]


def normalize_value(value: ValueType) -> Any:
    if isinstance(value, bytes):
        return [normalize_value(v) for v in value]
    if isinstance(value, int):
        return str(value)
    if isinstance(value, dict):
        return {k: normalize_value(v) for k, v in value.items()}
    else:
        return value


def stringify_record(record: McapRecord):
    fields = [(k, normalize_value(v)) for k, v in sorted(record.__dict__.items())]
    return {"type": type(record).__name__, "fields": fields}
