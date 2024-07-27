from enum import Enum
from typing import Any
from typing import Dict

from pydantic import BaseModel


def to_dict(obj: Any) -> Dict[str, Any]:
    """
    Recursively converts a dataclass to a dictionary.

    Parameters:
        obj: Any
            The dataclass instance to convert.

    Returns:
        A dictionary representation of the dataclass.
    """
    if not isinstance(obj, BaseModel):
        raise ValueError("Provided object is not a BaseModel instance")

    def convert_value(value: Any) -> Any:
        """
        Recursively converts values to appropriate types.
        """
        if isinstance(value, BaseModel):
            return to_dict(value)
        elif isinstance(value, list):
            return [convert_value(item) for item in value]
        elif isinstance(value, dict):
            return {key: convert_value(val) for key, val in value.items()}
        elif isinstance(value, Enum):
            return value.name
        else:
            return value

    return {field: convert_value(getattr(obj, field)) for field in obj.model_fields}
