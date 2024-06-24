from typing import Any
from typing import Dict
from typing import List
from typing import Optional

from pydantic import Field
from pydantic import field_validator

from tarchia.utils import is_valid_sql_identifier

from .metadata_models import Column
from .metadata_models import DatasetPermissions
from .metadata_models import OwnerType
from .metadata_models import RolePermission
from .metadata_models import Schema
from .metadata_models import TableDisposition
from .metadata_models import TableVisibility
from .tarchia_base import TarchiaBaseModel


def default_permissions() -> List[DatasetPermissions]:
    """
    Default permissions for a dataset.

    Returns:
        List[DatasetPermissions]: A list containing default read permission for all roles.
    """
    return [DatasetPermissions(role="*", permission=RolePermission.READ)]


def default_partitioning() -> List[str]:
    """
    Default partitioning fields.

    Returns:
        List[str]: A list containing default partitioning fields (year, month, day).
    """
    return ["year", "month", "day"]


class AddSnapshotRequest(TarchiaBaseModel):
    """
    Model for adding a snapshot request.

    Attributes:
        parent (str): The parent snapshot identifier.
    """

    parent: str


class CreateOwnerRequest(TarchiaBaseModel):
    """
    Model for creating an owner (an org or individual).

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


class CreateTableRequest(TarchiaBaseModel):
    """
    Model for creating a table request.

    Attributes:
        name (str): The name of the table.
        steward: (str): The indivial responsible for this table.
        location (str): The location of the table data.
        table_schema (Schema): The schema of the table.
        partitioning (Optional[List[str]]): The partitioning information, default is ["year", "month", "day"].
        disposition (TableDisposition): The disposition of the table, default is SNAPSHOT.
        permissions (List[DatasetPermissions]): The permissions associated with the table, default is read permission for all roles.
        metadata (Dict[str, Any]): Additional metadata for the table, default is an empty dictionary.
    """

    name: str
    steward: str
    location: str
    table_schema: Schema
    visibility: TableVisibility = TableVisibility.PRIVATE
    partitioning: Optional[List[str]] = Field(default_factory=default_partitioning)
    disposition: TableDisposition = TableDisposition.SNAPSHOT
    permissions: List[DatasetPermissions] = Field(default_factory=default_permissions)
    metadata: Dict[str, Any] = Field(default_factory=dict)

    @field_validator("name")
    def validate_name(cls, name):
        """
        Validate the table name to ensure it matches the required pattern.

        Args:
            name (str): The name of the table.

        Returns:
            str: The validated table name.

        Raises:
            ValueError: If the name does not match the required pattern.
        """
        if not is_valid_sql_identifier(name):
            raise ValueError("Invalid table name")
        return name


class TableCloneRequest(TarchiaBaseModel):
    """
    Model for table clone request.

    Attributes:
        name (str): The name of the cloned table.
        metadata (Dict[str, Any]): Additional metadata for the cloned table, default is an empty dictionary.
    """

    name: str


class UpdateSchemaRequest(TarchiaBaseModel):
    """
    Model for updating schema request.

    Attributes:
        columns (List[Column]): The list of columns in the schema.
    """

    columns: List[Column]
