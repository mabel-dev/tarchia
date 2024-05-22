from dataclasses import dataclass
from dataclasses import field
from typing import Any
from typing import Dict
from typing import List

from models import DatasetPermissions
from models import RolePermission
from models import Schema


def default_permissions():
    return [DatasetPermissions(role="*", permission=RolePermission.READ)]


@dataclass
class AddSnapshotRequest:
    parent: str


@dataclass
class CreateTableRequest:
    disposition: str
    location: str
    schema: Schema
    permissions: List[DatasetPermissions] = field(default_factory=default_permissions)
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
