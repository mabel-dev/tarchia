"""
Models to support handling manifests.
"""

from dataclasses import dataclass
from dataclasses import field
from enum import Enum
from typing import Dict
from typing import List
from typing import Optional
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
    value_counts: Dict[str, int]
    null_value_counts: Dict[str, int]
    lower_bounds: Dict[str, int] = field(default_factory=dict)
    upper_bounds: Dict[str, int] = field(default_factory=dict)
    histograms: Dict[str, Histogram] = field(default_factory=dict)
    kmv_hashes: Dict[str, List[int]] = field(default_factory=dict)
    key_metadata: Optional[str] = None

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
        {"name": "value_counts", "type": {"type": "map", "values": "int"}},
        {"name": "null_value_counts", "type": {"type": "map", "values": "int"}},
        {
            "name": "histograms",
            "type": {
                "type": "map",
                "values": {
                    "type": "record",
                    "name": "Histogram",
                    "fields": [
                        {
                            "name": "bins",
                            "type": {"type": "array", "items": {"type": "array", "items": "int"}},
                        }
                    ],
                },
            },
        },
        {
            "name": "kmv_hashes",
            "type": {"type": "map", "values": {"type": "array", "items": "int"}},
        },
        {"name": "key_metadata", "type": ["null", "string"], "default": None},
    ],
}
