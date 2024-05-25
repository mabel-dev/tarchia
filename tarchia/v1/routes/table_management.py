"""

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

def _uuid() -> str:
    import uuid
    return str(uuid.uuid4())

@router.get("/tables")
async def list_tables():

    return {"message": "Listing all tables"}


@router.post("/tables")
async def create_table(request: CreateTableRequest):

    new_table = TableMetadata(
        table_uuid=_uuid(),
        format_version=1,
        location=request.location,
        partitioning=request.paritioning,
        permissions=request.permissions,
        disposition=request.disposition,
        metadata=request.metadata
    )
    new_table.validate()

    catalog_provider.update_table_metadata(table_id=new_table.table_uuid, metadata=new_table)    


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
