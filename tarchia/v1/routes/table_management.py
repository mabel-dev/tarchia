"""

Routes:
    [POST]      /v1/tables
    [GET]       /v1/tables
    [GET]       /v1/tables/{tableIdentifier}
    [DELETE]    /v1/tables/{tableIdentifier}
    [GET]       /v1/tables/{tableIdentifier}/schemas
    [POST]      /v1/tables/{tableIdentifier}/schemas
    [POST]      /v1/tables/{tableIdentifier}/clone
"""

from typing import Optional

from fastapi import APIRouter
from v1.models import CreateTableRequest
from v1.models import TableCloneRequest
from v1.models import UpdateSchemaRequest

router = APIRouter()


@router.get("/tables")
async def list_tables():
    return {"message": "Listing all tables"}


@router.post("/tables")
async def create_table(request: CreateTableRequest):
    return {"message": "Table created", "table_details": request}


@router.get("/tables/{tableIdentifier}")
async def get_table(tableIdentifier: str):
    return {"message": "Table details", "identifier": tableIdentifier}


@router.delete("/tables/{tableIdentifier}")
async def delete_table(tableIdentifier: str):
    return {"message": "Table deleted", "identifier": tableIdentifier}


@router.get("/tables/{tableIdentifier}/schemas")
async def list_schemas(tableIdentifier: str, asOfTime: Optional[int] = None):
    return {
        "message": "Listing schemas for table",
        "identifier": tableIdentifier,
        "asOfTime": asOfTime,
    }


@router.post("/tables/{tableIdentifier}/schemas")
async def update_schema(tableIdentifier: str, request: UpdateSchemaRequest):
    return {"message": "Schema updated", "identifier": tableIdentifier, "schema": request.schema}


@router.post("/tables/{tableIdentifier}/clone")
async def clone_table(tableIdentifier: str, request: TableCloneRequest):
    return {"message": "clone updated", "identifier": tableIdentifier}
