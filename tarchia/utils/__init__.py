import uuid

from fastapi import Request

from tarchia.utils.config import METADATA_ROOT


def generate_uuid() -> str:
    """Generate a new UUID."""
    return str(uuid.uuid4()).replace("-", "")[-16:]


def get_base_url(request: Request) -> str:
    return request.url.scheme + "://" + request.url.netloc


def build_root(root: str, owner: str, table_id: str) -> str:
    """Wrap some repeat legwork"""
    value = root.replace("[metadata_root]", METADATA_ROOT)
    value = value.replace("[owner]", owner)
    value = value.replace("[table_id]", table_id)

    return value
