import time

from fastapi import APIRouter
from fastapi import Path
from fastapi import Request
from fastapi.responses import ORJSONResponse

from tarchia.exceptions import AlreadyExistsError
from tarchia.interfaces.catalog import catalog_factory
from tarchia.interfaces.storage import storage_factory
from tarchia.models import CreateTableRequest
from tarchia.models import TableCatalogEntry
from tarchia.models import UpdateMetadataRequest
from tarchia.models import UpdateValueRequest
from tarchia.utils.config import METADATA_ROOT
from tarchia.utils.constants import COMMITS_ROOT
from tarchia.utils.constants import HISTORY_ROOT
from tarchia.utils.constants import IDENTIFIER_REG_EX
from tarchia.utils.constants import MAIN_BRANCH

router = APIRouter()
catalog_provider = catalog_factory()
storage_provider = storage_factory()


@router.get("/tables/{owner}", response_class=ORJSONResponse)
async def list_tables(owner: str, request: Request):
    """
    Retrieve a list of tables and their current commits.

    This endpoint queries the catalog provider for the list of tables and
    augments each table's data with the URL to its current commit, if available.

    Returns:
        List[Dict[str, Any]]: A list of tables with their metadata, including the commit URL if applicable.
    """

    base_url = request.url.scheme + "://" + request.url.netloc

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
    request: Request,
    table_definition: CreateTableRequest,
    owner: str = Path(description="The owner of the table.", pattern=IDENTIFIER_REG_EX),
):
    """
    Create a new table in the catalog.

    This endpoint creates a new table with the specified metadata and stores it in the catalog.

    Parameters:
        request: CreateTableRequest - The request body containing the table metadata.
    """
    from tarchia.metadata.history import HistoryTree
    from tarchia.models import Commit
    from tarchia.utils import build_root
    from tarchia.utils import generate_uuid
    from tarchia.utils.catalogs import identify_owner

    base_url = request.url.scheme + "://" + request.url.netloc
    timestamp = int(time.time_ns() / 1e6)

    # check if we have a table with that name already
    catalog_entry = catalog_provider.get_table(owner=owner, table=table_definition.name)
    if catalog_entry:
        # return a 409
        raise AlreadyExistsError(entity=table_definition.name)

    # can we find the owner?
    owner_entry = identify_owner(name=owner)
    table_id = generate_uuid()

    commit_root = build_root(COMMITS_ROOT, owner=owner, table_id=table_id)
    history_root = build_root(HISTORY_ROOT, owner=owner, table_id=table_id)

    # build the new commit record
    new_commit = Commit(
        data_hash="",
        user="user",
        message="Initial commit",
        branch=MAIN_BRANCH,
        parent_commit_sha=None,
        last_updated_ms=timestamp,
        manifest_path=None,
        table_schema=table_definition.table_schema,
        encryption=table_definition.encryption,
    )

    # write the initial commit to storage
    commit_path = f"{commit_root}/commit-{new_commit.commit_sha}.json"
    storage_provider.write_blob(commit_path, new_commit.serialize())

    # we know we have no history, so we initialize it
    history = HistoryTree(MAIN_BRANCH)
    history.commit(new_commit.history_entry)
    history_raw = history.save_to_avro()
    history_uuid = generate_uuid()
    history_file = f"{history_root}/history-{history_uuid}.avro"
    storage_provider.write_blob(history_file, history_raw)

    # We create tables without any data
    new_table = TableCatalogEntry(
        name=table_definition.name,
        owner=owner,
        steward=table_definition.steward,
        table_id=table_id,
        format_version=1,
        location=table_definition.location,
        partitioning=table_definition.partitioning,
        visibility=table_definition.visibility,
        permissions=table_definition.permissions,
        disposition=table_definition.disposition,
        metadata=table_definition.metadata,
        current_commit_sha=new_commit.commit_sha,
        last_updated_ms=timestamp,
        freshness_life_in_days=table_definition.freshness_life_in_days,
        retention_in_days=table_definition.retention_in_days,
    )

    # create the metadata folder, put a file with the table name in there
    storage_provider.write_blob(f"{METADATA_ROOT}/{owner}/{table_id}/{table_definition.name}", b"")

    # Save the table to the Catalog - do this last
    catalog_provider.update_table(table_id=new_table.table_id, entry=new_table)

    # trigger webhooks - this should be async so we don't wait for the outcome
    owner_entry.trigger_event(
        owner_entry.EventTypes.TABLE_CREATED,
        {
            "event": "TABLE_CREATED",
            "table": f"{owner}.{table_definition.name}",
            "url": f"{base_url}/v1/tables/{owner}/{table_definition}",
        },
    )

    return {
        "message": "Table Created",
        "table": f"{owner}.{table_definition.name}",
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

    from tarchia.utils.catalogs import identify_owner
    from tarchia.utils.catalogs import identify_table

    owner_entry = identify_owner(name=owner)
    catalog_entry = identify_table(owner=owner, table=table)

    table_id = catalog_entry.table_id
    catalog_provider.delete_table(table_id)

    # mark the entry as deleted
    # we save the catalog entry to give the option to manually restate the table
    storage_provider.write_blob(
        f"{METADATA_ROOT}/{owner}/{table_id}/deleted.json", catalog_entry.serialize()
    )

    # trigger webhooks - this should be async so we don't wait for the outcome
    owner_entry.trigger_event(
        owner_entry.EventTypes.TABLE_DELETED,
        {"event": "TABLE_DELETED", "table": f"{owner}.{table}"},
    )

    return {
        "message": "Table Deleted",
        "table": f"{owner}.{table}",
    }


@router.patch("/tables/{owner}/{table}/schema")
async def update_schema(
    schema: Request,
    owner: str = Path(description="The owner of the table.", pattern=IDENTIFIER_REG_EX),
    table: str = Path(description="The name of the table.", pattern=IDENTIFIER_REG_EX),
):
    from tarchia.metadata.schemas import validate_schema_update
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
    from tarchia.utils.catalogs import identify_table

    catalog_entry = identify_table(owner, table)
    table_id = catalog_entry.table_id
    catalog_entry.metadata = metadata.metadata
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
    from tarchia.utils.catalogs import identify_table

    if attribute not in {"visibility", "steward"}:
        raise ValueError(f"Data attribute {attribute} cannot be modified via the API")

    catalog_entry = identify_table(owner, table)
    setattr(catalog_entry, attribute, value.value)

    catalog_provider.update_table(table_id=catalog_entry.table_id, entry=catalog_entry)

    return {
        "message": "Table updated",
        "table": f"{owner}.{table}",
    }
