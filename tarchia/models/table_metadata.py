from dataclasses import dataclass
from dataclasses import field
from enum import Enum
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from uuid import uuid4


def _uuid():
    return str(uuid4())


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
    encryption_details: EncryptionDetails


@dataclass
class SnapshotLogEntry:
    snapshot_id: int
    timestamp_ms: int


@dataclass
class TableMetadata:
    location: str
    last_updated_ms: int
    permissions: List[DatasetPermissions]
    schemas: List[Schema]
    snapshots: List[Snapshot]
    current_snapshot_id: int
    snapshot_log: List[SnapshotLogEntry]
    format_version: int = 1
    table_uuid: str = field(default_factory=_uuid)
    metadata: Dict[str, Any] = field(default_factory=dict)
    disposition: str = "table"


@dataclass
class StreamingMetadata:
    location: str
    last_updated_ms: int
    permissions: List[DatasetPermissions]
    schemas: List[Schema]
    current_snapshot: Snapshot
    format_version: int = 1
    table_uuid: str = field(default_factory=_uuid)
    metadata: Dict[str, Any] = field(default_factory=dict)
    disposition: str = "streaming"
