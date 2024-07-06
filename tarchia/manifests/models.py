"""
This module provides models to support handling manifests in the Tarchia system.

A manifest is a critical component in the data management workflow, representing a collection of
files and associated metadata. This module includes classes and definitions to manage and utilize
these manifests effectively.

Classes:
    EntryType: Enum representing the type of an entry in a manifest. An entry can either be a
      pointer to another manifest or a data file.
    ManifestEntry: Represents an entry in a manifest file, providing metadata about a file, such
      as its path, format, type, and other attributes.

Attributes:
    MANIFEST_SCHEMA (dict): Avro schema definition for the ManifestEntry, describing the structure
      and data types of the fields in a manifest entry.

The ManifestEntry class includes fields for file path, format, type, record count, file size,
checksum, and data bounds, offering a comprehensive structure for manifest management.
"""

from enum import Enum
from typing import Dict
from typing import Optional

from pydantic import Field

from tarchia.models.tarchia_base import TarchiaBaseModel


class EntryType(Enum):
    """
    Enum representing the type of an entry in a manifest.

    Attributes:
        Manifest (str): Indicates the entry is a pointer to another manifest.
        Data (str): Indicates the entry is a data file.
    """

    Manifest = "Manifest"
    Data = "Data"


class ManifestEntry(TarchiaBaseModel):
    """
    Represents an entry in a manifest file, providing metadata about a file.

    Attributes:
        file_path (str): The path to the file.
        file_format (str): The format of the file (e.g., 'parquet', 'json').
        file_type (EntryType): The type of the entry (e.g., 'type1', 'type2').
        record_count (Optional[int]): The number of records in the file. Defaults to None.
        file_size (Optional[int]): The size of the file in bytes. Defaults to None.
        sha256_checksum (Optional[str]): The SHA-256 checksum of the file. Defaults to None.
        lower_bounds (Dict[str, int]): A dictionary containing the lower bounds for data values.
        upper_bounds (Dict[str, int]): A dictionary containing the upper bounds for data values.
    """

    file_path: str
    file_format: str
    file_type: EntryType
    record_count: Optional[int] = None
    file_size: Optional[int] = None
    sha256_checksum: Optional[str] = None
    lower_bounds: Dict[str, int] = Field(default_factory=dict)
    upper_bounds: Dict[str, int] = Field(default_factory=dict)


# Avro schema definition for the ManifestEntry
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
        {"name": "record_count", "type": ["null", "int"], "default": None},
        {"name": "file_size", "type": ["null", "int"], "default": None},
        {"name": "sha256_checksum", "type": ["null", "string"], "default": None},
        {"name": "lower_bounds", "type": {"type": "map", "values": "int"}},
        {"name": "upper_bounds", "type": {"type": "map", "values": "int"}},
    ],
}
