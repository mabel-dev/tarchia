from enum import Enum
from typing import Any
from typing import List
from typing import Optional

from orso.schema import FlatColumn
from orso.schema import OrsoTypes
from pydantic import BaseModel
from pydantic import Field

from tarchia.utils import is_valid_sql_identifier


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


class EncryptionDetails(BaseModel):
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


class DatasetPermissions(BaseModel):
    """
    Model representing dataset permissions.

    Attributes:
        role (str): The role that has the permission.
        permission (RolePermission): The type of permission granted.
    """

    role: str
    permission: RolePermission


class Column(BaseModel):
    """
    Model representing a column in a schema.

    Attributes:
        name (str): The name of the column.
        required (bool): Whether the column is required.
        type (str): The data type of the column.
        default (Optional[Any]): The default value of the column, if any.
    """

    name: str
    default: Optional[Any]
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
            column = FlatColumn(**self.model_dump())
        except Exception as load_error:
            print(load_error)
            raise ValueError(f"Column definition for '{self.name}' is invalid.") from load_error


class Schema(BaseModel):
    """
    Model representing a schema.

    Attributes:
        columns (List[Column]): The list of columns in the schema.
    """

    columns: List[Column]

    def serialize(self) -> dict:
        """
        Serialize the snapshot to a dictionary.

        Returns:
            dict: The serialized snapshot.
        """
        return self.model_dump()


class Snapshot(BaseModel):
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

    def serialize(self) -> dict:
        """
        Serialize the snapshot to a dictionary.

        Returns:
            dict: The serialized snapshot.
        """
        return self.model_dump()


class TableCatalogEntry(BaseModel):
    """
    The Catalog entry for a table.

    This is intended to be stored in a document store like FireStore or MongoDB.

    Attributes:
        name (str): The name of the table.
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
    table_id: str
    location: str
    partitioning: List[str]
    last_updated_ms: int
    permissions: List[DatasetPermissions]
    current_schema: Schema
    current_snapshot_id: Optional[str]
    format_version: int = Field(default=1)
    disposition: TableDisposition = Field(default=TableDisposition.SNAPSHOT)
    metadata: dict = Field(default_factory=dict)

    def validate(self) -> bool:
        """
        Validate the table catalog entry.

        Returns:
            bool: True if the entry is valid, False otherwise.
        """
        return True

    def serialize(self) -> dict:
        """
        Serialize the table catalog entry to a dictionary.

        Returns:
            dict: The serialized table catalog entry.
        """
        from tarchia.utils.serde import to_dict

        return to_dict(self)
