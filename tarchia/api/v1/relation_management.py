""" """

from fastapi import APIRouter
from fastapi import Path
from fastapi import Request
from fastapi.responses import ORJSONResponse

from tarchia.interfaces.catalog import catalog_factory
from tarchia.utils.constants import IDENTIFIER_REG_EX

router = APIRouter()
catalog_provider = catalog_factory()


@router.get("/relations/{owner}", response_class=ORJSONResponse)
async def list_relations(
    request: Request,
    owner: str = Path(description="The owner of the relation.", pattern=IDENTIFIER_REG_EX),
):
    raise NotImplementedError("")


@router.get("/relations/{owner}/{relation}", response_class=ORJSONResponse)
async def list_relations(
    request: Request,
    owner: str = Path(description="The owner of the relation.", pattern=IDENTIFIER_REG_EX),
    relation: str = Path(description="The name of the relation.", pattern=IDENTIFIER_REG_EX),
):
    raise NotImplementedError("")
