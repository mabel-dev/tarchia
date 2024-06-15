"""

Routes:
    [POST]      /v1/tables/{tableIdentifier}/permissions
    [GET]       /v1/tables/{tableIdentifier}/permissions/check
"""

from typing import List

from fastapi import APIRouter

from tarchia.models import DatasetPermissions

router = APIRouter()


@router.post("/tables/{tableIdentifier}/permissions")
async def grant_permissions(tableIdentifier: str, permissions: DatasetPermissions):
    # Store or update the permissions in your permissions management system.
    return {
        "message": "Permissions updated",
        "identifier": tableIdentifier,
        "permissions": permissions,
    }


@router.delete("/tables/{tableIdentifier}/permissions")
async def revoke_permissions(tableIdentifier: str, permissions: DatasetPermissions):
    # Store or update the permissions in your permissions management system.
    return {
        "message": "Permissions updated",
        "identifier": tableIdentifier,
        "permissions": permissions,
    }


@router.get("/tables/{tableIdentifier}/permissions/check")
async def check_permissions(
    tableIdentifier: str, user_attributes: List[str], requested_permission: str
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
        "identifier": tableIdentifier,
        "requested_permission": requested_permission,
        "permission_granted": permission_granted,
    }
