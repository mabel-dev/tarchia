"""

"""

import time
import uuid
from typing import List
from typing import Optional
from typing import Tuple

from fastapi import APIRouter
from fastapi import Request
from fastapi import Response
from fastapi.responses import ORJSONResponse

from tarchia.catalog import catalog_factory
from tarchia.models import CreateTableRequest
from tarchia.models import TableCatalogEntry
from tarchia.models import TableCloneRequest
from tarchia.storage import storage_factory

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
    new_table = TableCatalogEntry(
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
    filters: Optional[List[Tuple[str, str, str]]] = None,
):
    """
    Return information required to access a table.

    We must have a table identifier, this will return the current position
    for the table. We can optionally specify a snotshot ID, with this we can
    look at a fixed historic version. We can also optionally provide a
    timestamp and we work out which snapshot to read. This is the least
    preferred option and requires more work to resolve.

    We also accept filters, these are conjunctive (ANDed) and used to
    perform blob filtering based on statistics held for each blob.

    We return the snapshot and the bloblist for the snapshot.
    """
    # Do we have a valid request
    if snapshotIdentifier is not None and as_at is not None:
        raise DataEntryError(
            endpoint="",
            fields=["snapshotIdentifier", "as_at"],
            message="Cannot provide a time-based search (as_at) and an index-based search (snapshotIdentifier) in the same request.",
        )

    # read the data from the catalog for this table
    catalog_entry = catalog_provider.get_table(tableIdentifier)

    # if we have no snapshot or as_at, we want the current version
    if snapshotIdentifier is None and as_at is None:
        snapshotIdentifier = catalog_entry.get("snapshot_id")

    if as_at is not None:

        # get all the snapshots
        storage_provider.blob_list(prefix="prefix", as_at=as_at)

    blobs = get_manifest(manifest, storage_provider, filters)

    return {**table_data, "blobs": blobs}


@router.delete("/tables/{tableIdentifier}", response_class=ORJSONResponse)
async def delete_table(tableIdentifier: str):
    catalog_provider.delete_table_metadata(tableIdentifier)


@router.post("/tables/{tableIdentifier}/clone", response_class=ORJSONResponse)
@router.post("/tables/{tableIdentifier}/{snapshotIdentifier}/clone", response_class=ORJSONResponse)
async def clone_table(
    tableIdentifier: str, request: TableCloneRequest, snapshotIdentifier: Optional[str] = None
):
    return {"message": "clone updated", "identifier": tableIdentifier}
