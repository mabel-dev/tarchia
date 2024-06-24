""" """

import time
from typing import Optional

import orjson
from fastapi import APIRouter
from fastapi import Path
from fastapi import Query
from fastapi import Request
from fastapi.responses import ORJSONResponse

from tarchia.config import METADATA_ROOT
from tarchia.constants import IDENTIFIER_REG_EX
from tarchia.constants import SNAPSHOT_ROOT
from tarchia.exceptions import TableHasNoDataError
from tarchia.manifests import get_manifest
from tarchia.manifests import parse_filters
from tarchia.models import CreateTableRequest
from tarchia.models import TableCatalogEntry
from tarchia.repositories.catalog import catalog_factory
from tarchia.storage import storage_factory
from tarchia.utils import build_root
from tarchia.utils import generate_uuid
from tarchia.utils.catalog import identify_table

router = APIRouter()


@router.get("/tables/{owner}", response_class=ORJSONResponse)
async def list_tables(owner: str, request: Request):
    """
    Retrieve a list of tables and their current snapshots.

    This endpoint queries the catalog provider for the list of tables and
    augments each table's data with the URL to its current snapshot, if available.

    Returns:
        List[Dict[str, Any]]: A list of tables with their metadata, including the snapshot URL if applicable.
    """
    catalog_provider = catalog_factory()
    base_url = request.url.scheme + "://" + request.url.netloc

    tables = catalog_provider.list_tables(owner)
    for table in tables:
        table_id = table["table_id"]
        current_snapshot_id = table.get("current_snapshot_id")
        if current_snapshot_id is not None:
            # provide the URL to call to get the snapshot
            table["snapshot_url"] = (
                f"{base_url}/tables/{owner}/{table_id}/snapshots/{current_snapshot_id}"
            )
    return tables


@router.post("/tables/{owner}", response_class=ORJSONResponse)
async def create_table(
    request: CreateTableRequest,
    owner: str = Path(description="The owner of the table.", regex=IDENTIFIER_REG_EX),
):
    """
    Create a new table in the catalog.

    This endpoint creates a new table with the specified metadata and stores it in the catalog.

    Parameters:
        request: CreateTableRequest - The request body containing the table metadata.
    """
    catalog_provider = catalog_factory()
    storage_provider = storage_factory()
    # check if we have a table with that name already
    catalog_entry = catalog_provider.get_table(owner=owner, table=request.name)
    if catalog_entry:
        raise ValueError("table name already exists")

    table_id = generate_uuid()

    # We create tables without any snapshot, at create-time the table has no data and some
    # table types (external) we never record snapshots for.
    new_table = TableCatalogEntry(
        name=request.name,
        owner=owner,
        steward=request.steward,
        table_id=table_id,
        format_version=1,
        location=request.location,
        partitioning=request.partitioning,
        visibility=request.visibility,
        permissions=request.permissions,
        disposition=request.disposition,
        metadata=request.metadata,
        current_snapshot_id=None,
        current_schema=request.table_schema,
        last_updated_ms=int(time.time_ns() / 1e6),
    )

    # Save the table to the Catalog
    catalog_provider.update_table(table_id=new_table.table_id, entry=new_table)
    # create the metadata folder, put a file with the table name in there
    storage_provider.write_blob(f"{METADATA_ROOT}/{owner}/{table_id}/{request.name}", b"")

    return {
        "message": "Table Created",
        "table": f"{owner}.{request.name}",
    }


@router.patch("/tables/{owner}/{table}", response_class=ORJSONResponse)
async def update_table(
    request: Request,
    owner: str = Path(description="The owner of the table.", regex=IDENTIFIER_REG_EX),
    table: str = Path(description="The name of the table.", regex=IDENTIFIER_REG_EX),
):
    catalog_provider = catalog_factory()

    catalog_entry = identify_table(owner, table)
    body = await request.json()

    if any(k in {"schema", "metadata"} for k in body):
        raise ValueError("Schema and Metadata must be updated via dedicated end points")

    if not all(k in {"visibility", "steward"} for k in body):
        raise ValueError("Read only property attempted to be updated")

    for key, value in body.items():
        if key == "visibility":
            catalog_entry.visibility = value

    catalog_provider.update_table(table_id=catalog_entry.table_id, entry=catalog_entry)

    return {
        "message": "Table updated",
        "table": f"{owner}.{table}",
    }


@router.get("/tables/{owner}/{table}", response_class=ORJSONResponse)
async def get_table(
    owner: str = Path(description="The owner of the table.", regex=IDENTIFIER_REG_EX),
    table: str = Path(description="The name of the table.", regex=IDENTIFIER_REG_EX),
    as_at: Optional[int] = Query(
        default=None,
        description="Retrieve the table state as of this timestamp, in nanoseconds after Linux epoch.",
    ),
    filters=None,
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
        table: str - The unique identifier of the table.
        as_at: Optional[int] - Timestamp to retrieve the table state as of this time.
        filters: Optional[str] - Comma separated list of filters to apply in the format (field=value).

    Returns:
        Dict[str, Any]: The table definition along with the snapshot and the list of blobs.
    """
    storage_provider = storage_factory()
    # read the data from the catalog for this table
    catalog_entry = identify_table(owner, table)

    # if the table has no snapshots, return only the table information
    if catalog_entry.current_snapshot_id is None:
        return catalog_entry.as_dict()

    snapshot_id = catalog_entry.snapshot_id
    table_id = catalog_entry.table_id
    snapshot_root = build_root(SNAPSHOT_ROOT, owner=owner, table_id=table_id)

    if as_at:
        # Get the snapshot before a given timestamp
        candidates = storage_provider.blob_list(prefix=snapshot_root, as_at=as_at)
        if len(candidates) != 1:
            raise TableHasNoDataError(owner=owner, table=table, as_at=as_at)
        snapshot_id = candidates[0].split("-")[-1].split(".")[0]

    snapshot_file = storage_provider.read_blob(f"{snapshot_root}/asat-{snapshot_id}.json")
    snapshot = orjson.loads(snapshot_file)

    # retrieve the list of blobs from the manifests
    filter_conditions = parse_filters(filters)
    blobs = get_manifest(snapshot.get("manifest_path"), storage_provider, filter_conditions)

    # build the response
    table_definition = catalog_entry.as_dict()
    table_definition.update(snapshot)
    table_definition["blobs"] = blobs

    return table_definition


@router.get("/tables/{owner}/{table}/snapshots/{snapshot}", response_class=ORJSONResponse)
async def get_table_snapshot(
    owner: str = Path(description="The owner of the table.", regex=IDENTIFIER_REG_EX),
    table: str = Path(description="The name of the table.", regex=IDENTIFIER_REG_EX),
    snapshot: str = Path(description="The snapshot to retrieve."),
):
    pass


@router.delete("/tables/{owner}/{table}", response_class=ORJSONResponse)
async def delete_table(
    owner: str = Path(description="The owner of the table.", regex=IDENTIFIER_REG_EX),
    table: str = Path(description="The name of the table.", regex=IDENTIFIER_REG_EX),
):
    """
    Delete a table from the catalog.

    Parameters:
        tableIdentifier: str - The identifier of the table to be deleted.

    Note:
        The metadata and data files for this table is NOT deleted.
    """
    catalog_provider = catalog_factory()
    storage_provider = storage_factory()

    catalog_entry = identify_table(owner=owner, table=table)
    table_id = catalog_entry.table_id
    catalog_provider.delete_table(table_id)

    # mark the entry as deleted
    # we save the catalog entry to give the option to manually restate the table
    storage_provider.write_blob(
        f"{METADATA_ROOT}/{owner}/{table_id}/deleted.json", catalog_entry.serialize()
    )

    return {
        "message": "Table Deleted",
        "table": table_id,
    }
