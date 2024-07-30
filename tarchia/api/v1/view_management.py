from fastapi import APIRouter
from fastapi import Path
from fastapi import Request
from fastapi.responses import ORJSONResponse

from tarchia.utils.constants import IDENTIFIER_REG_EX

router = APIRouter()


@router.get("/views/{owner}", response_class=ORJSONResponse)
async def list_views(
    request: Request,
    owner: str = Path(description="The owner of the view.", pattern=IDENTIFIER_REG_EX),
):
    raise NotImplementedError("Not Implemented")


@router.post("/views/{owner}", response_class=ORJSONResponse)
async def create_view(
    request: Request,
    owner: str = Path(description="The owner of the view.", pattern=IDENTIFIER_REG_EX),
):
    raise NotImplementedError("Not Implemented")


@router.get("/views/{owner}/{view}", response_class=ORJSONResponse)
async def get_view(
    request: Request,
    owner: str = Path(description="The owner of the view.", pattern=IDENTIFIER_REG_EX),
    view: str = Path(description="The view.", pattern=IDENTIFIER_REG_EX),
):
    raise NotImplementedError("Not Implemented")


@router.delete("/views/{owner}/{view}", response_class=ORJSONResponse)
async def delete_view(
    request: Request,
    owner: str = Path(description="The owner of the view.", pattern=IDENTIFIER_REG_EX),
    view: str = Path(description="The view.", pattern=IDENTIFIER_REG_EX),
):
    raise NotImplementedError("Not Implemented")
