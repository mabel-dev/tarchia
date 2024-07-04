"""
Models to support handling manifests.
"""

from enum import Enum
from typing import Dict
from typing import Optional

from pydantic import Field

from tarchia.models.tarchia_base import TarchiaBaseModel


class EntryType(Enum):
    Manifest = "Manifest"  # is this a pointer to another manifest
    Data = "Data"  # is this a data file


class ManifestEntry(TarchiaBaseModel):
    file_path: str
    file_format: str
    file_type: EntryType
    record_count: Optional[int] = None
    file_size: Optional[int] = None
    lower_bounds: Dict[str, int] = Field(default_factory=dict)
    upper_bounds: Dict[str, int] = Field(default_factory=dict)


MANIFEST_SCHEMA = {
    "type": "record",
    "name": "ManifestEntry",
    "fields": [
        {"name": "file_path", "type": "string"},
        {"name": "file_format", "type": "string"},
        {
            "name": "file_type",
            "type": {"type": "enum", "name": "EntryType", "symbols": ["Manifest", "Data"]},
        },
        {"name": "record_count", "type": "int"},
        {"name": "file_size", "type": "int"},
        {"name": "lower_bounds", "type": {"type": "map", "values": "int"}},
        {"name": "upper_bounds", "type": {"type": "map", "values": "int"}},
    ],
}
