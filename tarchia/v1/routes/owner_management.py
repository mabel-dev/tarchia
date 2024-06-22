from fastapi import APIRouter
from fastapi.responses import ORJSONResponse

from tarchia.models import CreateOwnerRequest
from tarchia.models import TableCatalogEntry

router = APIRouter()


@router.post("/owners", response_class=ORJSONResponse)
async def create_table(request: CreateOwnerRequest):
    pass
