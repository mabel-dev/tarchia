"""

Routes:
    [POST]      /v1/tables
    [GET]       /v1/tables
    [GET]       /v1/tables/{tableIdentifier}
    [DELETE]    /v1/tables/{tableIdentifier}
    [POST]      /v1/tables/{tableIdentifier}/clone
"""

from typing import Optional

from catalog import catalog_factory
from fastapi import APIRouter
from fastapi import HTTPException
from models import CreateTableRequest
from models import TableCloneRequest
from models import TableMetadata
from models import UpdateSchemaRequest

router = APIRouter()
catalog_provider = catalog_factory()


@router.get("/tables")
async def list_tables():

    return {"message": "Listing all tables"}


@router.post("/tables")
async def create_table(request: CreateTableRequest):
    if request.disposition == "table":
        new_table = TableMetadata()
    else:
        raise HTTPException(status_code=422, detail="Invalid disposition")


@router.get("/tables/{tableIdentifier}")
async def get_table(
    tableIdentifier: str, as_at: Optional[int] = None, filter: Optional[str] = None
):
    return {"message": "Table details", "identifier": tableIdentifier}


@router.get("/tables/{tableIdentifier}/{snapshotIdentifier}")
async def get_table(tableIdentifier: str, snapshotIdentifier: str):
    return {"message": "Table details", "identifier": tableIdentifier}


@router.delete("/tables/{tableIdentifier}")
async def delete_table(tableIdentifier: str):
    return {"message": "Table deleted", "identifier": tableIdentifier}


@router.post("/tables/{tableIdentifier}/clone")
async def clone_table(tableIdentifier: str, request: TableCloneRequest):
    return {"message": "clone updated", "identifier": tableIdentifier}
