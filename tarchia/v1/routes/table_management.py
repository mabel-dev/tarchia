import time

from fastapi import APIRouter
from fastapi import Path
from fastapi import Request
from fastapi.responses import ORJSONResponse

from tarchia.config import METADATA_ROOT
from tarchia.constants import IDENTIFIER_REG_EX
from tarchia.exceptions import AlreadyExistsError
from tarchia.models import CreateTableRequest
from tarchia.models import TableCatalogEntry
from tarchia.models import UpdateMetadataRequest
from tarchia.models import UpdateSchemaRequest
from tarchia.models import UpdateValueRequest

router = APIRouter()


@router.get("/tables/{owner}", response_class=ORJSONResponse)
async def list_tables(owner: str, request: Request):
    """
    Retrieve a list of tables and their current commits.

    This endpoint queries the catalog provider for the list of tables and
    augments each table's data with the URL to its current commit, if available.

    Returns:
        List[Dict[str, Any]]: A list of tables with their metadata, including the commit URL if applicable.
    """
    from tarchia.catalog import catalog_factory

    base_url = request.url.scheme + "://" + request.url.netloc
    catalog_provider = catalog_factory()
    table_list = []

    tables = catalog_provider.list_tables(owner)
    for table in tables:
        # filter down the items we return
        table = {
            k: v
            for k, v in table.items()
            if k
            in {
                "table_id",
                "current_commit_sha",
                "name",
                "description",
                "visibility",
                "owner",
                "last_updated_ms",
                "steward",
                "metadata",
            }
        }
        current_commit_sha = table.get("current_commit_sha")
        table_name = table.get("name")
        if current_commit_sha is not None:
            # provide the URL to call to get the latest commit
            table["commit_url"] = (
                f"{base_url}/v1/tables/{owner}/{table_name}/commits/{current_commit_sha}"
            )
        table_list.append(table)
    return table_list


@router.post("/tables/{owner}", response_class=ORJSONResponse)
async def create_table(
    request: CreateTableRequest,
    owner: str = Path(description="The owner of the table.", pattern=IDENTIFIER_REG_EX),
):
    """
    Create a new table in the catalog.

    This endpoint creates a new table with the specified metadata and stores it in the catalog.

    Parameters:
        request: CreateTableRequest - The request body containing the table metadata.
    """
    from tarchia.catalog import catalog_factory
    from tarchia.storage import storage_factory
    from tarchia.utils import generate_uuid
    from tarchia.utils.catalogs import identify_owner

    # check if we have a table with that name already
    catalog_provider = catalog_factory()
    storage_provider = storage_factory()
    catalog_entry = catalog_provider.get_table(owner=owner, table=request.name)
    if catalog_entry:
        # return a 409
        raise AlreadyExistsError(entity=request.name)

    # can we find the owner?
    identify_owner(name=owner)

    table_id = generate_uuid()

    # We create tables without any commit, at create-time the table has no data and some
    # table types (external) we never record commits for.
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
        current_commit_sha=None,
        current_schema=request.table_schema,
        last_updated_ms=int(time.time_ns() / 1e6),
        encryption_details=request.encryption_details,
    )

    # Save the table to the Catalog
    catalog_provider.update_table(table_id=new_table.table_id, entry=new_table)
    # create the metadata folder, put a file with the table name in there
    storage_provider.write_blob(f"{METADATA_ROOT}/{owner}/{table_id}/{request.name}", b"")

    return {
        "message": "Table Created",
        "table": f"{owner}.{request.name}",
    }


@router.get("/tables/{owner}/{table}", response_class=ORJSONResponse)
async def get_table(
    request: Request,
    owner: str = Path(description="The owner of the table.", pattern=IDENTIFIER_REG_EX),
    table: str = Path(description="The name of the table.", pattern=IDENTIFIER_REG_EX),
):
    """
    This is essentially a test that a table exists. It doesn't read the commits or
    manifests so it can reply faster.

    Parameters:
        table: str - The unique identifier of the table.
        as_at: Optional[int] - Timestamp to retrieve the table state as of this time.

    Returns:
        Dict[str, Any]: The table definition
    """
    from tarchia.utils.catalogs import identify_table

    catalog_entry = identify_table(owner, table)

    table = catalog_entry.as_dict()

    current_commit_sha = catalog_entry.current_commit_sha
    base_url = request.url.scheme + "://" + request.url.netloc
    if current_commit_sha is not None:
        # provide the URL to call to get the latest snapshot
        table["commit_url"] = (
            f"{base_url}/v1/tables/{catalog_entry.owner}/{catalog_entry.name}/commits/{current_commit_sha}"
        )

    return table


@router.delete("/tables/{owner}/{table}", response_class=ORJSONResponse)
async def delete_table(
    owner: str = Path(description="The owner of the table.", pattern=IDENTIFIER_REG_EX),
    table: str = Path(description="The name of the table.", pattern=IDENTIFIER_REG_EX),
):
    """
    Delete a table from the catalog.

    Parameters:
        tableIdentifier: str - The identifier of the table to be deleted.

    Note:
        The metadata and data files for this table is NOT deleted.
    """
    from tarchia.catalog import catalog_factory
    from tarchia.storage import storage_factory
    from tarchia.utils.catalogs import identify_table

    catalog_entry = identify_table(owner=owner, table=table)
    catalog_provider = catalog_factory()
    storage_provider = storage_factory()

    table_id = catalog_entry.table_id
    catalog_provider.delete_table(table_id)

    # mark the entry as deleted
    # we save the catalog entry to give the option to manually restate the table
    storage_provider.write_blob(
        f"{METADATA_ROOT}/{owner}/{table_id}/deleted.json", catalog_entry.serialize()
    )

    return {
        "message": "Table Deleted",
        "table": f"{owner}.{table}",
    }


@router.patch("/tables/{owner}/{table}/schema")
async def update_schema(
    schema: UpdateSchemaRequest,
    owner: str = Path(description="The owner of the table.", pattern=IDENTIFIER_REG_EX),
    table: str = Path(description="The name of the table.", pattern=IDENTIFIER_REG_EX),
):
    from tarchia.catalog import catalog_factory
    from tarchia.schemas import validate_schema_update
    from tarchia.utils.catalogs import identify_table

    # is the new schema valid
    for col in schema.columns:
        col.is_valid()

    catalog_entry = identify_table(owner=owner, table=table)

    # is the evolution valid
    validate_schema_update(current_schema=catalog_entry.current_schema, updated_schema=schema)

    # update the schema
    table_id = catalog_entry.table_id
    catalog_entry.current_schema = schema
    catalog_provider = catalog_factory()
    catalog_provider.update_table(table_id, catalog_entry)

    return {
        "message": "Schema Updated",
        "table": f"{owner}.{table}",
    }


@router.patch("/tables/{owner}/{table}/metadata")
async def update_metadata(
    metadata: UpdateMetadataRequest,
    owner: str = Path(description="The owner of the table.", pattern=IDENTIFIER_REG_EX),
    table: str = Path(description="The name of the table.", pattern=IDENTIFIER_REG_EX),
):
    from tarchia.catalog import catalog_factory
    from tarchia.utils.catalogs import identify_table

    catalog_entry = identify_table(owner, table)
    table_id = catalog_entry.table_id
    catalog_entry.metadata = metadata.metadata
    catalog_provider = catalog_factory()
    catalog_provider.update_table(table_id=table_id, entry=catalog_entry)

    return {
        "message": "Metadata updated",
        "table": f"{owner}.{table}",
    }


@router.patch("/tables/{owner}/{table}/{attribute}", response_class=ORJSONResponse)
async def update_table(
    value: UpdateValueRequest,
    attribute: str,
    owner: str = Path(description="The owner of the table.", pattern=IDENTIFIER_REG_EX),
    table: str = Path(description="The name of the table.", pattern=IDENTIFIER_REG_EX),
):
    from tarchia.catalog import catalog_factory
    from tarchia.utils.catalogs import identify_table

    if attribute not in {"visibility", "steward"}:
        raise ValueError(f"Data attribute {attribute} cannot be modified via the API")

    catalog_entry = identify_table(owner, table)
    setattr(catalog_entry, attribute, value.value)
    catalog_provider = catalog_factory()

    catalog_provider.update_table(table_id=catalog_entry.table_id, entry=catalog_entry)

    return {
        "message": "Table updated",
        "table": f"{owner}.{table}",
    }
