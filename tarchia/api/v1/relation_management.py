""" """

from fastapi import APIRouter
from fastapi import Path
from fastapi import Request
from fastapi.responses import ORJSONResponse

from tarchia.exceptions import NotFoundError
from tarchia.exceptions import TableNotFoundError
from tarchia.exceptions import ViewNotFoundError
from tarchia.interfaces.catalog import catalog_factory
from tarchia.utils.constants import IDENTIFIER_REG_EX

router = APIRouter()
catalog_provider = catalog_factory()


@router.get("/relations/{owner}/{relation}", response_class=ORJSONResponse)
async def get_relation(
    request: Request,
    owner: str = Path(description="The owner of the relation.", pattern=IDENTIFIER_REG_EX),
    relation: str = Path(description="The name of the relation.", pattern=IDENTIFIER_REG_EX),
):
    """
    When a user enters a relation in a query, we don't know if it's a
    table or a view, rather than the client have to do series of calls,
    we look up both for them and return the match (or Not Found)
    """

    from tarchia.utils.catalogs import identify_table
    from tarchia.utils.catalogs import identify_view

    from .table_management import get_table
    from .view_management import get_view

    try:
        # this will fail if the entry isn't a known table
        identify_table(owner, relation)
        # call the get_table routine and return the result
        response = await get_table(request, owner=owner, table=relation)
        return response
    except TableNotFoundError:
        pass

    try:
        # this will fail if the entry isn't a known view
        identify_view(owner, relation)
        # call the get_view routine and return the result
        response = await get_view(request, owner=owner, view=relation)
        return response
    except ViewNotFoundError:
        pass

    raise NotFoundError(f"Unable to find a table or view called {owner}.{relation}")
