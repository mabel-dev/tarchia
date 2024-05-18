"""

Routes:
    [POST]      /v1/tables/{tableIdentifier}/metadata
    [GET]       /v1/tables/{tableIdentifier}/metadata
"""

from typing import Optional

from fastapi import APIRouter
from v1.models import UpdateMetadataRequest

router = APIRouter()


@router.post("/tables/{tableIdentifier}/metadata")
async def update_metadata(tableIdentifier: str, request: UpdateMetadataRequest):
    return {
        "message": "Metadata updated",
        "identifier": tableIdentifier,
        "metadata": request.metadata,
    }


@router.get("/tables/{tableIdentifier}/metadata")
async def get_metadata(tableIdentifier: str, asOfTime: Optional[int] = None):
    return {
        "message": "Table metadata retrieved",
        "identifier": tableIdentifier,
        "asOfTime": asOfTime,
    }
