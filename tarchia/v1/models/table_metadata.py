from dataclasses import dataclass
from dataclasses import field
from enum import Enum
from typing import Any
from typing import Dict
from typing import List
from typing import Optional


@dataclass
class EncryptionDetails:
    algorithm: str
    key_id: str
    fields: List[str]


class RolePermission(Enum):
    READ = "read"
    WRITE = "write"
    OWN = "own"


@dataclass
class DatasetPermissions:
    role: str
    permission: RolePermission


@dataclass
class Field:
    name: str
    required: bool
    type: str
    default: Optional[Any] = None


@dataclass
class Schema:
    schema_id: int
    fields: List[Field]


@dataclass
class Snapshot:
    snapshot_id: int
    parent_snapshot_id: Optional[int]
    timestamp_ms: int
    manifest_list: str
    summary: Dict[str, str]
    schema_id: int


@dataclass
class SnapshotLogEntry:
    snapshot_id: int
    timestamp_ms: int


@dataclass
class TableMetadata:
    format_version: int
    disposition: str
    table_uuid: str
    location: str
    last_updated_ms: int
    encryption_details: EncryptionDetails
    permissions: List[DatasetPermissions]
    schemas: List[Schema]
    snapshots: List[Snapshot]
    current_snapshot_id: int
    snapshot_log: List[SnapshotLogEntry]
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class StreamingMetadata:
    format_version: int
    disposition: str
    table_uuid: str
    location: str
    last_updated_ms: int
    encryption_details: EncryptionDetails
    permissions: List[DatasetPermissions]
    schemas: List[Schema]
    current_snapshot: Snapshot
    metadata: Dict[str, Any] = field(default_factory=dict)
