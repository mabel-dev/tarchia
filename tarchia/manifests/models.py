"""
Models to support handling manifests.
"""

from dataclasses import dataclass
from dataclasses import field
from enum import Enum
from typing import Dict
from typing import List
from typing import Tuple


class EntryType(Enum):
    Manifest = "Manifest"  # is this a pointer to another manifest
    Data = "Data"  # is this a data file


@dataclass
class Histogram:
    bins: List[Tuple[int, int]]


@dataclass
class ManifestEntry:
    file_path: str
    file_format: str
    file_type: EntryType
    record_count: int
    file_size: int
    null_value_counts: Dict[str, int]
    lower_bounds: Dict[str, int] = field(default_factory=dict)
    upper_bounds: Dict[str, int] = field(default_factory=dict)

    def serialize(self):
        from tarchia.utils.serde import to_dict

        data = to_dict(self)
        return data


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
        {"name": "null_value_counts", "type": {"type": "map", "values": "int"}},
    ],
}
