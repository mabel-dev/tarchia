from dataclasses import dataclass
from dataclasses import field
from enum import Enum
from typing import Any
from typing import List
from typing import Optional
from uuid import uuid4

from tarchia.utils.serde import to_dict


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
    parent_snapshot_path: Optional[str]
    last_updated_ms: int
    manifest_path: Optional[str]
    schema: Schema
    encryption_details: EncryptionDetails

    def serialize(self) -> dict:
        return to_dict(self)


@dataclass
class TableCatalogEntry:
    """
    The Catalog entry for a table.

    This is intended to be stored in a document store like FireStore or MongoDB.
    """

    name: str
    location: str
    partitioning: List[str]
    last_updated_ms: int
    permissions: List[DatasetPermissions]
    schema: Schema
    current_snapshot_id: Optional[str]
    format_version: int = 1
    table_id: str = field(default_factory=_uuid)
    disposition: TableDisposition = TableDisposition.SNAPSHOT
    metadata: dict = field(default_factory=dict)

    def validate(self) -> bool:
        return True

    def serialize(self) -> dict:
        return to_dict(self)
