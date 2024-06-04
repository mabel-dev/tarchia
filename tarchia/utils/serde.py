from dataclasses import fields
from dataclasses import is_dataclass
from enum import Enum
from typing import Any
from typing import Dict

import orjson


def to_json(self) -> bytes:
    def default_serializer(o):
        if isinstance(o, Enum):
            return o.__name__
        raise TypeError(f"Object of type {o.__class__.__name__} is not JSON serializable")

    return orjson.dumps(to_dict(self), default=default_serializer)


def to_dict(obj: Any) -> Dict[str, Any]:
    """
    Recursively converts a dataclass to a dictionary.

    Parameters:
        obj: Any
            The dataclass instance to convert.

    Returns:
        A dictionary representation of the dataclass.
    """
    if not is_dataclass(obj):
        raise ValueError("Provided object is not a dataclass instance")

    def convert_value(value: Any) -> Any:
        """
        Recursively converts values to appropriate types.
        """
        if is_dataclass(value):
            return to_dict(value)
        elif isinstance(value, list):
            return [convert_value(item) for item in value]
        elif isinstance(value, dict):
            return {key: convert_value(val) for key, val in value.items()}
        elif isinstance(value, Enum):
            return value.name
        else:
            return value

    return {field.name: convert_value(getattr(obj, field.name)) for field in fields(obj)}
