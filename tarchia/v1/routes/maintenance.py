"""

Routes:
    [POST]      /v1/tables/{tableIdentifier}/maintenance/compact
    [POST]      /v1/tables/{tableIdentifier}/maintenance/refresh_metadata
"""

from fastapi import APIRouter

router = APIRouter()


@router.post("/tables/{tableIdentifier}/maintenance/compact")
async def compact_table(tableIdentifier: str):
    """
    Compaction optimizes the layout of data files, combining smaller files into larger ones to improve read performance.
    """
    return {"message": "Table compaction initiated", "identifier": tableIdentifier}


@router.post("/tables/{tableIdentifier}/maintenance/refresh_metadata")
async def refresh_metadata(tableIdentifier: str):
    """
    This task could involve cleaning up old metadata files, refreshing metadata to reflect external changes, or even triggering a full rebuild of metadata if it becomes corrupted or too large.
    """
    return {"message": "Metadata refresh initiated", "identifier": tableIdentifier}
