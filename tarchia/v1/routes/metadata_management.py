"""

Routes:
    [POST]      /v1/tables/{tableIdentifier}/metadata
    [GET]       /v1/tables/{tableIdentifier}/metadata
"""

from typing import Optional

from fastapi import APIRouter
from models import UpdateMetadataRequest

router = APIRouter()


@router.post("/tables/{tableIdentifier}/metadata")
async def update_metadata(tableIdentifier: str, request: UpdateMetadataRequest):
    return {
        "message": "Metadata updated",
        "identifier": tableIdentifier,
        "metadata": request.metadata,
    }
