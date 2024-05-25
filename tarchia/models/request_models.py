from dataclasses import dataclass
from dataclasses import field
from typing import Any
from typing import Dict, Optional
from typing import List

from models import DatasetPermissions
from models import RolePermission, TableDisposition
from models import Schema


def default_permissions() -> List[DatasetPermissions]:
    return [DatasetPermissions(role="*", permission=RolePermission.READ)]

def default_partitioning() -> List[str]:
    return ["year", "month", "day"]

@dataclass
class AddSnapshotRequest:
    parent: str


@dataclass
class CreateTableRequest:
    location: str
    schema: Schema
    paritioning: Optional[List[str]] = field(default=default_partitioning)
    disposition: TableDisposition = TableDisposition.SNAPSHOT
    permissions: List[DatasetPermissions] = field(default=default_permissions)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class TableCloneRequest:
    metadata: str


@dataclass
class UpdateMetadataRequest:
    metadata: str


@dataclass
class UpdateSchemaRequest:
    metadata: str
