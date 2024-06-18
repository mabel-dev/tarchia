""" """

import time
from typing import Optional

import orjson
from fastapi import APIRouter
from fastapi import Path
from fastapi import Query
from fastapi import Request
from fastapi import Response
from fastapi.responses import ORJSONResponse

from tarchia.catalog import catalog_factory
from tarchia.config import METADATA_ROOT
from tarchia.exceptions import DataEntryError
from tarchia.exceptions import TableHasNoDataError
from tarchia.manifest import get_manifest
from tarchia.manifest import parse_filters
from tarchia.models import CreateTableRequest
from tarchia.models import TableCatalogEntry
from tarchia.storage import storage_factory
from tarchia.utils.helpers import generate_uuid
from tarchia.utils.helpers import identify_table

SNAPSHOT_ROOT = f"{METADATA_ROOT}/[table_id]/snapshots/"
MANIFEST_ROOT = f"{METADATA_ROOT}/[table_id]/manifests/"

router = APIRouter()
catalog_provider = catalog_factory()
storage_provider = storage_factory()


@router.get("/tables", response_class=ORJSONResponse)
async def list_tables(request: Request):
    """
    Retrieve a list of tables and their current snapshots.

    This endpoint queries the catalog provider for the list of tables and
    augments each table's data with the URL to its current snapshot, if available.

    Returns:
        List[Dict[str, Any]]: A list of tables with their metadata, including the snapshot URL if applicable.
    """
    base_url = request.url.scheme + "://" + request.url.netloc

    tables = catalog_provider.list_tables()
    for table in tables:
        table_id = table["table_id"]
        current_snapshot_id = table.get("current_snapshot_id")
        if current_snapshot_id is not None:
            # provide the URL to call to get the snapshot
            table["snapshot_url"] = f"{base_url}/tables/{table_id}/{current_snapshot_id}"
    return tables


@router.post("/tables", response_class=ORJSONResponse)
async def create_table(request: CreateTableRequest):
    """
    Create a new table in the catalog.

    This endpoint creates a new table with the specified metadata and stores it in the catalog.

    Parameters:
        request: CreateTableRequest - The request body containing the table metadata.
    """
    request.validate()

    # check if we have a table with that name already
    catalog_entry = catalog_provider.get_table(request.name)
    if catalog_entry:
        raise ValueError("table name already exists")

    table_id = generate_uuid()

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
        last_updated_ms=int(time.time_ns() / 1e6),
    )

    # Save the table to the Catalog
    catalog_provider.update_table_metadata(
        table_id=new_table.table_id, metadata=new_table.serialize()
    )
    # create the metadata folder, put a file with the table name in there
    storage_provider.write_blob(f"{METADATA_ROOT}/{table_id}/{request.name}", b"")

    return {
        "message": "Table Created",
        "table": table_id,
    }


@router.get("/tables/{tableIdentifier}", response_class=ORJSONResponse)
async def get_table(
    tableIdentifier: str = Path(description="The unique identifier of the table."),
    snapshotIdentifier: Optional[str] = Query(
        None, description="The unique identifier of the snapshot to retrieve."
    ),
    as_at: Optional[int] = Query(
        None,
        description="Retrieve the table state as of this timestamp, in nanoseconds after Linux epoch.",
        example=1625097600,
    ),
    #    filters: Optional[str] = Query(
    #        None,
    #        description="List of filters to apply in the format (field, operator, value).",
    #        example="column1=value1,column2>value2"
    #    ),
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

    Parameters:
        tableIdentifier: str - The unique identifier of the table.
        as_at: Optional[int] - Timestamp to retrieve the table state as of this time.
        snapshotIdentifier: Optional[str] - The unique identifier of the snapshot to retrieve.
        filters: Optional[str] - Comma separated list of filters to apply in the format (field=value).

    Returns:
        Dict[str, Any]: The table definition along with the snapshot and the list of blobs.
    """
    # read the data from the catalog for this table
    catalog_entry = identify_table(tableIdentifier)

    # if the table has no snapshots, return only the table information
    if catalog_entry.current_snapshot_id is None:
        if snapshotIdentifier is not None or as_at is not None:
            raise TableHasNoDataError(table=tableIdentifier)
        return catalog_entry.serialize()

    table_id = catalog_entry.table_id
    my_snapshot_root = SNAPSHOT_ROOT.replace("[table_id]", table_id)

    # Do we have a valid request
    if snapshotIdentifier is not None and as_at is not None:
        raise DataEntryError(
            endpoint="",
            fields=["snapshotIdentifier", "as_at"],
            message="Cannot provide a time-based search (as_at) and an index-based search (snapshotIdentifier) in the same request.",
        )

    # if we have no snapshot or as_at, we want the current version
    if snapshotIdentifier is None and as_at is None:
        snapshotIdentifier = catalog_entry.snapshot_id

    if as_at is not None:
        # Get the snapshot before a given timestamp
        candidates = storage_provider.blob_list(prefix=my_snapshot_root, as_at=as_at)
        if len(candidates) != 1:
            raise TableHasNoDataError(table=tableIdentifier, as_at=as_at)
        snapshotIdentifier = candidates[0].split("-")[-1].split(".")[0]

    snapshot_file = storage_provider.read_blob(
        f"{SNAPSHOT_ROOT}/snapshot-{snapshotIdentifier}.json"
    )
    snapshot = orjson.loads(snapshot_file)

    # retrieve the list of blobs from the manifests
    filter_conditions = parse_filters(filters)
    blobs = get_manifest(snapshot.get("manifest_path"), storage_provider, filter_conditions)

    # build the response
    table_definition = catalog_entry.serialize()
    table_definition.update(snapshot)
    table_definition["blobs"] = blobs

    return table_definition


@router.delete("/tables/{tableIdentifier}", response_class=ORJSONResponse)
async def delete_table(tableIdentifier: str):
    """
    Delete a table from the catalog.

    Parameters:
        tableIdentifier: str - The identifier of the table to be deleted.

    Note:
        The metadata and data files for this table is NOT deleted.
    """
    catalog_entry = identify_table(tableIdentifier)
    table_id = catalog_entry.table_id
    catalog_provider.delete_table_metadata(table_id)

    return {
        "message": "Table Deleted",
        "table": table_id,
    }
