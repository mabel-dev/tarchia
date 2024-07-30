import time
from enum import Enum
from typing import Any
from typing import List
from typing import Literal
from typing import Optional

from orso.schema import FlatColumn
from orso.schema import OrsoTypes
from pydantic import Field

from tarchia.exceptions import DataEntryError

from .eventable import Eventable
from .tarchia_base import TarchiaBaseModel


class OwnerType(Enum):
    """
    Enum for owner types.
    """

    ORGANIZATION = "ORGANIZATION"
    INDIVIDUAL = "INDIVIDUAL"


class RolePermission(Enum):
    """
    Enum for role permissions.
    """

    READ = "READ"
    WRITE = "WRITE"
    OWN = "OWN"


class TableDisposition(Enum):
    """
    Enum for table dispositions.
    """

    SNAPSHOT = "SNAPSHOT"
    CONTINUOUS = "CONTINUOUS"
    EXTERNAL = "EXTERNAL"


class TableVisibility(Enum):
    """
    Enum for table visibility.
    """

    PRIVATE = "PRIVATE"  # visibility granted explicitly
    INTERNAL = "INTERNAL"  # restricted to group members
    PUBLIC = "PUBLIC"  # no restrictions


class EncryptionDetails(TarchiaBaseModel):
    """
    Model representing encryption details.

    Attributes:
        algorithm (str): The encryption algorithm used.
        key_id (str): The identifier for the encryption key.
        fields (List[str]): The fields to be encrypted.
    """

    algorithm: str
    key_id: str
    fields: List[str]


class DatasetPermissions(TarchiaBaseModel):
    """
    Model representing dataset permissions.

    Attributes:
        role (str): The role that has the permission.
        permission (RolePermission): The type of permission granted.
    """

    role: str
    permission: RolePermission


class Column(TarchiaBaseModel):
    """
    Model representing a column in a schema.

    Attributes:
        name (str): The name of the column.
        required (bool): Whether the column is required.
        type (str): The data type of the column.
        default (Optional[Any]): The default value of the column, if any.
    """

    name: str
    default: Optional[Any] = None
    type: OrsoTypes = OrsoTypes.VARCHAR
    description: Optional[str] = ""
    aliases: List[str] = []

    def is_valid(self):
        # The name must be valid SQL
        if not self.name.isidentifier():
            raise DataEntryError(
                fields=["name"],
                message="Column names cannot start with a digit and can only contain alphanumerics and underscores.",
            )

        # We need to be able to build valid Orso FlatColumns
        # it has validation we can leverage
        FlatColumn(**self.as_dict())


class Schema(TarchiaBaseModel):
    """
    Model representing a schema.

    Attributes:
        columns (List[Column]): The list of columns in the schema.
    """

    columns: List[Column]


class TableCatalogEntry(Eventable, TarchiaBaseModel):
    """
    The Catalog entry for a table.

    This is intended to be stored in a document store like FireStore or MongoDB.
    """

    class EventTypes(Enum):
        """Supported Eventables"""

        NEW_COMMIT = "NEW_COMMIT"

    name: str
    steward: str
    owner: str
    table_id: str
    relation: Literal["table"] = "table"
    location: Optional[str]
    partitioning: Optional[List[str]]
    last_updated_ms: int
    freshness_life_in_days: int
    retention_in_days: int
    permissions: List[DatasetPermissions]
    visibility: TableVisibility
    description: Optional[str] = ""
    current_commit_sha: Optional[str] = None
    current_history: Optional[str] = None
    format_version: int = Field(default=1)
    disposition: TableDisposition = Field(default=TableDisposition.SNAPSHOT)
    metadata: dict = Field(default_factory=dict)
    created_at: int = int(time.time_ns() / 1e6)

    def is_valid(self):
        # only columns in the schema can be encrypted

        # partitioning requires a location

        return True


class OwnerEntry(Eventable, TarchiaBaseModel):
    """
    Model for owners.

    Attributes:
        name (str): The name of the owning user/organization.
        type (OwnerType): The type of the owner.
        user (str): The name of the user/group that owns this group.
        memberships (List(str)): Identifiers to automatically map users to Owners
        created_at (int): Timestamp this record was creted.
    """

    class EventTypes(Enum):
        """Supported Eventables"""

        TABLE_CREATED = "TABLE_CREATED"
        TABLE_DELETED = "TABLE_DELETED"
        VIEW_CREATED = "VIEW_CREATED"
        VIEW_DELETED = "VIEW_DELETED"

    name: str
    owner_id: str
    type: OwnerType
    steward: str
    memberships: List[str]
    description: Optional[str] = ""
    created_at: int = int(time.time_ns() / 1e6)

    def is_valid(self):
        if not self.name.isidentifier():
            raise DataEntryError(
                fields=["name"],
                message="Owner name cannot start with a digit and can only contain alphanumerics and underscores.",
            )


class ViewCatalogEntry(TarchiaBaseModel):
    """
    The Catalog entry for a view.
    """

    name: str
    steward: str
    owner: str
    view_id: str
    statement: str
    relation: Literal["view"] = "view"
    metadata: dict = Field(default_factory=dict)
    created_at: int = int(time.time_ns() / 1e6)
    description: Optional[str] = ""
    format_version: int = Field(default=1)


class Transaction(TarchiaBaseModel):
    transaction_id: str
    expires_at: int
    table_id: str
    table: str
    owner: str
    encryption: Optional[EncryptionDetails]
    table_schema: Schema
    parent_commit_sha: Optional[str] = None
    additions: List[str] = Field(default_factory=list)
    deletions: List[str] = Field(default_factory=list)
    truncate: bool = False
