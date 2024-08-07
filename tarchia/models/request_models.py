from typing import Any
from typing import Dict
from typing import List
from typing import Optional

from pydantic import Field
from pydantic import field_validator

from tarchia.exceptions import DataEntryError

from .metadata_models import DatasetPermissions
from .metadata_models import EncryptionDetails
from .metadata_models import OwnerType
from .metadata_models import RolePermission
from .metadata_models import Schema
from .metadata_models import TableDisposition
from .metadata_models import TableVisibility
from .tarchia_base import TarchiaBaseModel


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
    description: str
    memberships: List[str]


class CreateTableRequest(TarchiaBaseModel):
    """
    Model for creating a table request.

    Attributes:
        name (str): The name of the table.
        steward (str): The indivial responsible for this table.
        location (str): The location of the table data.
        freshness_life_in_days (int): The number of days this data is considered live
        retention_in_days (int): Days this data must be retained for
        table_schema (Schema): The schema of the table.
        partitioning (Optional[List[str]]): The partitioning information, default is ["year", "month", "day"].
        disposition (TableDisposition): The disposition of the table, default is SNAPSHOT.
        permissions (List[DatasetPermissions]): The permissions associated with the table, default is read permission for all roles.
        metadata (Dict[str, Any]): Additional metadata for the table, default is an empty dictionary.
    """

    name: str
    steward: str
    location: Optional[str]
    freshness_life_in_days: int
    retention_in_days: int
    table_schema: Schema
    description: str
    visibility: TableVisibility = TableVisibility.PRIVATE
    partitioning: Optional[List[str]] = ["year", "month", "day"]
    disposition: TableDisposition = TableDisposition.SNAPSHOT
    permissions: List[DatasetPermissions] = [
        DatasetPermissions(role="*", permission=RolePermission.READ)
    ]
    encryption: Optional[EncryptionDetails] = None
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
        if not name.isidentifier():
            raise DataEntryError(
                fields=["name"],
                message="Table name cannot start with a digit and can only contain alphanumerics and underscores.",
            )
        return name


class UpdateMetadataRequest(TarchiaBaseModel):
    metadata: dict = Field(default_factory=dict)


class UpdateValueRequest(TarchiaBaseModel):
    value: Any


class TransactionRequest(TarchiaBaseModel):
    encoded_transaction: str


class CommitRequest(TransactionRequest):
    commit_message: str


class StageFilesRequest(TransactionRequest):
    paths: List[str]


class CreateViewRequest(TarchiaBaseModel):
    name: str
    statement: str
    metadata: dict = Field(default_factory=dict)
    description: Optional[str] = ""
