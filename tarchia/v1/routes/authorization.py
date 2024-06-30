from typing import List

from fastapi import APIRouter
from fastapi import Path

from tarchia.constants import IDENTIFIER_REG_EX
from tarchia.models import DatasetPermissions

router = APIRouter()


@router.post("/tables/{owner}/{table}/permissions")
async def grant_permissions(
    permissions: DatasetPermissions,
    owner: str = Path(description="The owner of the table.", pattern=IDENTIFIER_REG_EX),
    table: str = Path(description="The name of the table.", pattern=IDENTIFIER_REG_EX),
):
    # Store or update the permissions in your permissions management system.
    return {
        "message": "Permissions updated",
        "table": f"{owner}.{table}",
        "permissions": permissions,
    }


@router.delete("/tables/{owner}/{table}/permissions")
async def revoke_permissions(
    permissions: DatasetPermissions,
    owner: str = Path(description="The owner of the table.", pattern=IDENTIFIER_REG_EX),
    table: str = Path(description="The name of the table.", pattern=IDENTIFIER_REG_EX),
):
    # Store or update the permissions in your permissions management system.
    return {
        "message": "Permissions updated",
        "table": f"{owner}.{table}",
        "permissions": permissions,
    }


@router.get("/tables/{owner}/{table}/permissions/check")
async def check_permissions(
    user_attributes: List[str],
    requested_permission: str,
    owner: str = Path(description="The owner of the table.", pattern=IDENTIFIER_REG_EX),
    table: str = Path(description="The name of the table.", pattern=IDENTIFIER_REG_EX),
):
    # Retrieve dataset permissions from your storage
    dataset_permissions = (
        DatasetPermissions()
    )  # Assuming this retrieves a DatasetPermissions instance

    # Determine if user has the necessary permission
    attribute_set = set(user_attributes)
    if (
        requested_permission == "read"
        and attribute_set.intersection(dataset_permissions.read)
        or requested_permission == "write"
        and attribute_set.intersection(dataset_permissions.write)
        or requested_permission == "own"
        and attribute_set.intersection(dataset_permissions.own)
    ):
        permission_granted = True
    else:
        permission_granted = False

    return {
        "message": "Permission check completed",
        "table": f"{owner}.{table}",
        "requested_permission": requested_permission,
        "permission_granted": permission_granted,
    }
