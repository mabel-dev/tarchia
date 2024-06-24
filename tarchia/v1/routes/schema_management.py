""" """

from fastapi import APIRouter
from fastapi import Path
from fastapi import Response

from tarchia.constants import IDENTIFIER_REG_EX
from tarchia.exceptions import TableNotFoundError
from tarchia.models import UpdateSchemaRequest
from tarchia.repositories.catalog import catalog_factory
from tarchia.utils.catalog import identify_table

catalog_provider = catalog_factory()

router = APIRouter()


@router.patch("/tables/{owner}/{table}/schema")
async def update_schema(
    schema: UpdateSchemaRequest,
    owner: str = Path(description="The owner of the table.", regex=IDENTIFIER_REG_EX),
    table: str = Path(description="The name of the table.", regex=IDENTIFIER_REG_EX),
):
    for col in schema.columns:
        col.validate()

    catalog_entry = identify_table(owner=owner, table=table)
    table_id = catalog_entry.table_id
    catalog_entry.current_schema = schema
    catalog_provider.update_table_metadata(table_id, catalog_entry)
    return Response(status_code=204)


@router.get("/tables/{owner}/{table}/schema")
async def latest_schema(
    owner: str = Path(description="The owner of the table.", regex=IDENTIFIER_REG_EX),
    table: str = Path(description="The name of the table.", regex=IDENTIFIER_REG_EX),
):
    # read the data from the catalog for this table
    catalog_entry = catalog_provider.get_table(owner=owner, table=table)
    if catalog_entry is None:
        raise TableNotFoundError(owner=owner, table=table)
    return catalog_entry.get("current_schema")
