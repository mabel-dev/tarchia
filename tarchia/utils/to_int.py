import datetime
from decimal import Decimal
from typing import Any
from typing import Optional


def to_int(value: Any) -> Optional[int]:
    """
    Convert a value to an integer that is orderable to the original value.

    Parameters:
        value (Any): The input value to be converted.

    Returns:
        int: The orderable integer representation of the input value.
    """
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return round(value)
    if isinstance(value, datetime.date):
        return int(value.strftime("%s"))
    if isinstance(value, datetime.datetime):
        return round(value.timestamp())
    if isinstance(value, datetime.time):
        return value.hour * 3600 + value.minute * 60 + value.second
    if isinstance(value, Decimal):
        return round(value)
    if isinstance(value, str):
        padded_value = (value + "\x00" * 8)[:8]  # Pad with nulls to ensure at least 8 characters
        return int.from_bytes(padded_value.encode(), "big")
    if isinstance(value, bytes):
        padded_value = value[:8].ljust(8, b"\x00")  # Pad with null bytes to ensure at least 8 bytes
        return int.from_bytes(padded_value, "big")
    return None  # Return None for anything else
