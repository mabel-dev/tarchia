"""

Routes:
    [POST]      /v1/tables/{tableIdentifier}/metadata
    [GET]       /v1/tables/{tableIdentifier}/metadata
"""

from fastapi import APIRouter
from models import UpdateMetadataRequest
from models import UpdateSchemaRequest

router = APIRouter()


@router.post("/tables/{tableIdentifier}/metadata")
async def update_metadata(tableIdentifier: str, request: UpdateMetadataRequest):
    return {
        "message": "Metadata updated",
        "identifier": tableIdentifier,
        "metadata": request.metadata,
    }


@router.post("/tables/{tableIdentifier}/schemas")
async def update_schema(tableIdentifier: str, request: UpdateSchemaRequest):
    return {"message": "Schema updated", "identifier": tableIdentifier, "schema": request.schema}
