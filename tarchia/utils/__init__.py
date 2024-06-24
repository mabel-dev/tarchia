import re
import uuid

from tarchia.config import METADATA_ROOT
from tarchia.constants import IDENTIFIER_REG_EX


def generate_uuid() -> str:
    """Generate a new UUID."""
    return str(uuid.uuid4())


def is_valid_sql_identifier(identifier: str) -> bool:
    """Is the string a valid SQL identifier"""
    return identifier and re.match(IDENTIFIER_REG_EX, identifier)


def build_root(root: str, owner: str, table_id: str) -> str:
    """Wrap some repeat legwork"""
    value = root.replace("[metadata_root]", METADATA_ROOT)
    value = value.replace("[owner]", owner)
    value = value.replace("[table_id]", table_id)

    return value
