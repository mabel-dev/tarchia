""" """

from fastapi import APIRouter
from fastapi import Response

from tarchia.catalog import catalog_factory
from tarchia.exceptions import TableNotFoundError
from tarchia.models import UpdateSchemaRequest
from tarchia.utils.helpers import identify_table

catalog_provider = catalog_factory()

router = APIRouter()


@router.patch("/tables/{tableIdentifier}/schema")
async def update_schema(tableIdentifier: str, request: UpdateSchemaRequest):
    try:
        catalog_entry = identify_table(tableIdentifier)
        table_id = catalog_entry.table_id
        catalog_entry.current_schema = request
        catalog_provider.update_table_metadata(table_id, catalog_entry)
        return Response(status_code=204)
    except Exception as e:
        print(e)


@router.get("/tables/{tableIdentifier}/schema")
async def latest_schema(tableIdentifier: str):
    # read the data from the catalog for this table
    catalog_entry = catalog_provider.get_table(tableIdentifier)
    if catalog_entry is None:
        raise TableNotFoundError(table=tableIdentifier)
    return catalog_entry.get("current_schema")
