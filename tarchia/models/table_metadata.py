from dataclasses import dataclass
from dataclasses import field
from enum import Enum
from typing import Any
from typing import List
from typing import Optional
from uuid import uuid4

from utils.serde import to_dict


class RolePermission(Enum):
    READ = "read"
    WRITE = "write"
    OWN = "own"


class TableDisposition(Enum):
    SNAPSHOT = "snaphot"
    CONTINOUS = "continuous"
    EXTERNAL = "external"


class IndexType(Enum):
    BINARY = "binary"


def _uuid():
    return str(uuid4())


@dataclass
class EncryptionDetails:
    algorithm: str
    key_id: str
    fields: List[str]


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
    columns: List[Column]


@dataclass
class Snapshot:
    snapshot_id: str
    parent_snapshot_path: Optional[str]  # can be not for this dataset
    last_updated_ns: int
    manifest_path: Optional[str]
    schema: Schema
    encryption_details: EncryptionDetails


@dataclass
class TableMetadata:
    name: str
    location: str
    partitioning: List[str]
    last_updated_ns: int
    permissions: List[DatasetPermissions]
    schema: Schema
    current_snapshot_id: Optional[str]
    format_version: int = 1
    table_id: str = field(default_factory=_uuid)
    disposition: TableDisposition = TableDisposition.SNAPSHOT
    metadata: dict = field(default_factory=dict)

    def validate(self):
        return True

    @property
    def dic(self):
        return to_dict(self)
