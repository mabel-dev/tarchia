from typing import List

from fastapi import APIRouter
from fastapi import Request
from fastapi.responses import ORJSONResponse

router = APIRouter()


@router.get("/search", response_class=ORJSONResponse)
async def search(request: Request, term: str, scopes: List[str] = None):
    raise NotImplementedError("Not Implemented")
