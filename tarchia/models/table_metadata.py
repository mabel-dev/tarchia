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
class Column:
    name: str
    required: bool
    type: str
    default: Optional[Any] = None


@dataclass
class Schema:
    schema_id: int
    columns: List[Column]


@dataclass
class Snapshot:
    snapshot_id: int
    parent_snapshot_path: Optional[str]
    timestamp_ms: int
    manifest_path: str
    schema: Schema
    encryption_details: EncryptionDetails


@dataclass
class TableMetadata:
    location: str
    partitioning: List[str]
    last_updated_ms: int
    permissions: List[DatasetPermissions]
    current_snapshot: Snapshot
    format_version: int = 1
    table_uuid: str = field(default_factory=_uuid)
    disposition: str = "table"
