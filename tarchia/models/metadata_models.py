from enum import Enum
from typing import Any
from typing import List
from typing import Optional

from orso.schema import FlatColumn
from orso.schema import OrsoTypes
from pydantic import Field

from tarchia.utils import is_valid_sql_identifier

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

    def validate(self):
        # The name must be valid SQL
        if not is_valid_sql_identifier(self.name):
            raise ValueError(f"Invalid column name '{self.name}'.")

        # We need to be able to build valid Orso FlatColumns
        # it has validation we can leverage
        try:
            column = FlatColumn(**self.as_dict())
        except Exception as load_error:
            print(load_error)
            raise ValueError(f"Column definition for '{self.name}' is invalid.") from load_error


class Schema(TarchiaBaseModel):
    """
    Model representing a schema.

    Attributes:
        columns (List[Column]): The list of columns in the schema.
    """

    columns: List[Column]


class Snapshot(TarchiaBaseModel):
    """
    Model representing a snapshot.

    Attributes:
        snapshot_id (str): The unique identifier for the snapshot.
        parent_snapshot_path (Optional[str]): The path to the parent snapshot.
        last_updated_ms (int): The last update timestamp in milliseconds.
        manifest_path (Optional[str]): The path to the manifest.
        table_schema (Schema): The schema associated with the snapshot.
        encryption_details (EncryptionDetails): The encryption details for the snapshot.
    """

    snapshot_id: str
    parent_snapshot_path: Optional[str]
    last_updated_ms: int
    manifest_path: Optional[str]
    table_schema: Schema
    encryption_details: EncryptionDetails


class TableCatalogEntry(TarchiaBaseModel):
    """
    The Catalog entry for a table.

    This is intended to be stored in a document store like FireStore or MongoDB.

    Attributes:
        name (str): The name of the table.
        steward (str): The individual responsible for this table.
        owner (str): The namespace of the table.
        location (str): The location of the table data.
        partitioning (List[str]): The partitioning information.
        last_updated_ms (int): The last update timestamp in milliseconds.
        permissions (List[DatasetPermissions]): The permissions associated with the table.
        current_schema (Schema): The schema of the table.
        current_snapshot_id (Optional[str]): The current snapshot identifier.
        format_version (int): The format version of the table.
        table_id (str): The unique identifier for the table.
        disposition (TableDisposition): The disposition of the table.
        metadata (dict): Additional metadata for the table.
    """

    name: str
    steward: str
    owner: str
    table_id: str
    location: str
    partitioning: List[str]
    last_updated_ms: int
    permissions: List[DatasetPermissions]
    visibility: TableVisibility
    current_schema: Schema
    current_snapshot_id: Optional[str]
    format_version: int = Field(default=1)
    disposition: TableDisposition = Field(default=TableDisposition.SNAPSHOT)
    metadata: dict = Field(default_factory=dict)


class OwnerModel(TarchiaBaseModel):
    """
    Model for owners.

    Attributes:
        name (str): The name of the owning user/organization.
        type (OwnerType): The type of the owner.
        user (str): The name of the user/group that owns this group.
        memberships (List(str)): Identifiers to automatically map users to Owners
    """

    name: str
    type: OwnerType
    steward: str
    memberships: List[str]
