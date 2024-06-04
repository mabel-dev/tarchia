"""

"""

import time
import uuid
from typing import Optional

from catalog import catalog_factory
from fastapi import APIRouter
from fastapi import Request
from fastapi import Response
from fastapi.responses import ORJSONResponse
from models import CreateTableRequest
from models import TableCloneRequest
from models import TableMetadata
from storage import storage_factory

router = APIRouter()
catalog_provider = catalog_factory()
storage_provider = storage_factory()


def _uuid() -> str:
    return str(uuid.uuid4())


@router.get("/tables", response_class=ORJSONResponse)
async def list_tables(request: Request):
    """
    Retrieve a list of tables and their current snapshots.

    This endpoint queries the catalog provider for the list of tables and
    augments each table's data with the URL to its current snapshot, if available.

    Returns:
    - List[Dict[str, Any]]:
        A list of tables with their metadata, including the snapshot URL if applicable.
    """
    base_url = request.url.scheme + "://" + request.url.netloc

    tables = catalog_provider.list_tables()
    for table in tables:
        table_id = table["table_id"]
        current_snapshot_id = table.get("current_snapshot_id")
        if current_snapshot_id is not None:
            # provide the URL to call to get the snapshot
            table["snapshot"] = f"{base_url}/tables/{table_id}/{current_snapshot_id}"
    return tables


@router.post("/tables", response_class=ORJSONResponse)
async def create_table(request: CreateTableRequest):
    """
    Create a new table in the catalog.

    This endpoint creates a new table with the specified metadata and stores it in the catalog.

    Parameters:
    - request: CreateTableRequest
        The request body containing the table metadata.
    """
    table_id = _uuid()

    # We create tables without any snapshot, at create-time the table has no data and some
    # table types (external) we never record snapshots for.
    new_table = TableMetadata(
        name=request.name,
        table_id=table_id,
        format_version=1,
        location=request.location,
        partitioning=request.paritioning,
        permissions=request.permissions,
        disposition=request.disposition,
        metadata=request.metadata,
        current_snapshot_id=None,
        schema=request.schema,
        last_updated_ns=time.time_ns(),
    )
    new_table.validate()

    # Save the table to the Catalog
    catalog_provider.update_table_metadata(table_id=new_table.table_id, metadata=new_table.dic)

    # 204 (No Content)
    return Response(status_code=204)


@router.get("/tables/{tableIdentifier}", response_class=ORJSONResponse)
@router.get("/tables/{tableIdentifier}/{snapshotIdentifier}", response_class=ORJSONResponse)
async def get_table(
    tableIdentifier: str,
    as_at: Optional[int] = None,
    snapshotIdentifier: Optional[str] = None,
):
    """
    return the schema and the filelist
    """

    quit()

    latest_table = catalog_provider.get_table(tableIdentifier)
    if snapshotIdentifier is None and as_at is None:
        snapshotIdentifier = latest_table.get("snapshot_id")
    if as_at is not None:
        # get all the snapshots
        storage_provider.blob_list(prefix="prefix", as_at=as_at)


@router.delete("/tables/{tableIdentifier}", response_class=ORJSONResponse)
async def delete_table(tableIdentifier: str):
    catalog_provider.delete_table_metadata(tableIdentifier)


@router.post("/tables/{tableIdentifier}/clone", response_class=ORJSONResponse)
@router.post("/tables/{tableIdentifier}/{snapshotIdentifier}/clone", response_class=ORJSONResponse)
async def clone_table(
    tableIdentifier: str, request: TableCloneRequest, snapshotIdentifier: Optional[str] = None
):
    return {"message": "clone updated", "identifier": tableIdentifier}
