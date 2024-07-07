import datetime
from decimal import Decimal
from typing import Any
from typing import Optional


def to_int(value: Any) -> Optional[int]:
    """
    Convert a value to an integer that is orderable to the original value and ensure it fits within the signed 64-bit integer range.

    Parameters:
        value (Any): The input value to be converted.

    Returns:
        int: The orderable integer representation of the input value, ensured to fit within the signed 64-bit integer range.
    """
    min_signed_64bit = -(2**63)
    max_signed_64bit = 2**63 - 1

    def ensure_64bit_range(val: int) -> int:
        return max(min_signed_64bit, min(val, max_signed_64bit))

    if isinstance(value, int):
        return ensure_64bit_range(value)
    if isinstance(value, float):
        return ensure_64bit_range(round(value))
    if isinstance(value, datetime.date):
        timestamp = int(value.strftime("%s"))
        return ensure_64bit_range(timestamp)
    if isinstance(value, datetime.datetime):
        timestamp = round(value.timestamp())
        return ensure_64bit_range(timestamp)
    if isinstance(value, datetime.time):
        time_value = value.hour * 3600 + value.minute * 60 + value.second
        return ensure_64bit_range(time_value)
    if isinstance(value, Decimal):
        return ensure_64bit_range(round(value))
    if isinstance(value, str):
        padded_value = (value + "\x00" * 8)[:8]  # Pad with nulls to ensure at least 8 characters
        int_value = int.from_bytes(padded_value.encode(), "big")
        return ensure_64bit_range(int_value)
    if isinstance(value, bytes):
        padded_value = value[:8].ljust(8, b"\x00")  # Pad with null bytes to ensure at least 8 bytes
        int_value = int.from_bytes(padded_value, "big")
        return ensure_64bit_range(int_value)

    return None  # Return None for anything else
