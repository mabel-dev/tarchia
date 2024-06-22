import re
import uuid

from tarchia.constants import identifier_validation


def generate_uuid() -> str:
    """Generate a new UUID."""
    return str(uuid.uuid4())


def is_valid_sql_identifier(identifier: str) -> bool:
    """Is the string a valid SQL identifier"""
    return identifier and re.match(identifier_validation, identifier)
