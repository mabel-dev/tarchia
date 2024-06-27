from fastapi import APIRouter
from fastapi.responses import ORJSONResponse

from tarchia.exceptions import AlreadyExistsError
from tarchia.models import CreateOwnerRequest
from tarchia.models import OwnerEntry
from tarchia.models import UpdateValueRequest
from tarchia.repositories.catalog import catalog_factory
from tarchia.storage import storage_factory
from tarchia.utils import generate_uuid
from tarchia.utils.catalogs import identify_owner

router = APIRouter()

catalog_provider = catalog_factory()
storage_provider = storage_factory()


@router.post("/owners", response_class=ORJSONResponse)
async def create_owner(request: CreateOwnerRequest):
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
        "owner": f"{request.name}",
    }


@router.get("/owners/{owner}", response_class=ORJSONResponse)
async def read_owner(owner: str):
    entry = identify_owner(owner)
    return entry.as_dict()


@router.patch("/owners/{owner}/{attribute}", response_class=ORJSONResponse)
async def update_owner(owner: str, attribute: str, request: UpdateValueRequest):
    if attribute not in {"steward"}:
        raise ValueError(f"Unable to update {attribute}")
    entry = identify_owner(owner)
    setattr(entry, attribute, request.value)
    catalog_provider.update_owner(entry)
    return {"message": "Owner Updated", "owner": owner, "attribute": attribute}


@router.delete("/owners/{owner}", response_class=ORJSONResponse)
async def delete_owner(owner: str):
    entry = identify_owner(owner)
    tables = catalog_provider.list_tables(owner)
    if len(tables) > 0:
        raise ValueError("Cannot delete an owner of tables")
    catalog_provider.delete_owner(owner)
    return {
        "message": "Owner Deleted",
        "owner": owner,
    }
