"""
API Router for managing owners in the Tarchia catalog system.

This module defines the FastAPI routes for creating, reading, updating,
and deleting owners. It utilizes the ORJSONResponse for efficient JSON
responses and handles various exceptions and validation scenarios.

Endpoints:
    - POST /owners: Create a new owner.
    - GET /owners/{owner}: Read an owner by name.
    - PATCH /owners/{owner}/{attribute}: Update an attribute of an owner.
    - DELETE /owners/{owner}: Delete an owner.

Exceptions:
    - AlreadyExistsError: Raised when attempting to create an owner that already exists.
    - HTTPException: Raised for various validation errors and constraints.
"""

from fastapi import APIRouter
from fastapi import HTTPException
from fastapi.responses import ORJSONResponse

from tarchia.exceptions import AlreadyExistsError
from tarchia.models import CreateOwnerRequest
from tarchia.models import OwnerEntry
from tarchia.models import UpdateValueRequest

router = APIRouter()


@router.post("/owners", response_class=ORJSONResponse)
async def create_owner(request: CreateOwnerRequest):
    """
    Create a new owner.

    Parameters:
        request: CreateOwnerRequest
            The request containing the owner details.

    Returns:
        JSON response with a message and owner name.
    """
    from tarchia.catalog import catalog_factory
    from tarchia.utils import generate_uuid

    catalog_provider = catalog_factory()

    catalog_entry = catalog_provider.get_owner(name=request.name)
    if catalog_entry:
        raise AlreadyExistsError(entity=request.name)

    new_owner = OwnerEntry(
        name=request.name,
        owner_id=generate_uuid(),
        type=request.type,
        steward=request.steward,
        memberships=request.memberships,
    )
    catalog_provider.update_owner(new_owner)

    return {
        "message": "Owner Created",
        "owner": request.name,
    }


@router.get("/owners/{owner}", response_class=ORJSONResponse)
async def read_owner(owner: str):
    """
    Read an owner by name.

    Parameters:
        owner: str
            The name of the owner.

    Returns:
        JSON response with the owner details.
    """
    from tarchia.utils.catalogs import identify_owner

    entry = identify_owner(owner)
    return entry.as_dict()


@router.patch("/owners/{owner}/{attribute}", response_class=ORJSONResponse)
async def update_owner(owner: str, attribute: str, request: UpdateValueRequest):
    """
    Update an attribute of an owner.

    Parameters:
        owner: str
            The name of the owner.
        attribute: str
            The attribute to update.
        request: UpdateValueRequest
            The request containing the new value for the attribute.

    Returns:
        JSON response with a message, owner name, and updated attribute.
    """
    from tarchia.catalog import catalog_factory
    from tarchia.utils.catalogs import identify_owner

    if attribute not in {"steward"}:
        raise HTTPException(status_code=405, detail=f"Attribute {attribute} cannot be PATCHed.")

    catalog_provider = catalog_factory()
    entry = identify_owner(owner)
    setattr(entry, attribute, request.value)
    catalog_provider.update_owner(entry)

    return {"message": "Owner Updated", "owner": owner, "attribute": attribute}


@router.delete("/owners/{owner}", response_class=ORJSONResponse)
async def delete_owner(owner: str):
    """
    Delete an owner.

    Parameters:
        owner: str
            The name of the owner.

    Returns:
        JSON response with a message and owner name.
    """
    from tarchia.catalog import catalog_factory
    from tarchia.utils.catalogs import identify_owner

    entry = identify_owner(owner)
    catalog_provider = catalog_factory()

    if catalog_provider.list_tables(owner):
        raise HTTPException(status_code=409, detail="Cannot delete an owner with active tables.")

    catalog_provider.delete_owner(entry.owner_id)

    return {
        "message": "Owner Deleted",
        "owner": owner,
    }
