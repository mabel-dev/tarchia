import re
from dataclasses import dataclass
from dataclasses import field
from typing import Any
from typing import Dict
from typing import List
from typing import Optional

from .table_metadata import DatasetPermissions
from .table_metadata import RolePermission
from .table_metadata import Schema
from .table_metadata import TableDisposition


def default_permissions() -> List[DatasetPermissions]:
    return [DatasetPermissions(role="*", permission=RolePermission.READ)]


def default_partitioning() -> List[str]:
    return ["year", "month", "day"]


@dataclass
class AddSnapshotRequest:
    parent: str


@dataclass
class CreateTableRequest:
    name: str
    location: str
    schema: Schema
    paritioning: Optional[List[str]] = field(default=default_partitioning)
    disposition: TableDisposition = TableDisposition.SNAPSHOT
    permissions: List[DatasetPermissions] = field(default=default_permissions)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def validate(self):
        if not self.name or not re.match(r"^[A-Za-z_]\w*$", self.name):
            return False

        return True


@dataclass
class TableCloneRequest:
    metadata: str
    name: str


@dataclass
class UpdateMetadataRequest:
    metadata: str


@dataclass
class UpdateSchemaRequest:
    metadata: str
