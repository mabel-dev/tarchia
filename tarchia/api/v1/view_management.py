import time

from fastapi import APIRouter
from fastapi import Path
from fastapi import Request
from fastapi.responses import ORJSONResponse

from tarchia.interfaces.catalog import catalog_factory
from tarchia.models import CreateViewRequest
from tarchia.models import UpdateMetadataRequest
from tarchia.models import UpdateValueRequest
from tarchia.models import ViewCatalogEntry
from tarchia.utils import get_base_url
from tarchia.utils.constants import IDENTIFIER_REG_EX

router = APIRouter()
catalog_provider = catalog_factory()


@router.get("/views/{owner}", response_class=ORJSONResponse)
async def list_views(
    request: Request,
    owner: str = Path(description="The owner of the view.", pattern=IDENTIFIER_REG_EX),
):
    base_url = get_base_url(request=request)

    view_list = []

    views = catalog_provider.list_views(owner)
    for view in views:
        # filter down the items we return
        view = {
            k: v
            for k, v in view.items()
            if k
            in {
                "view_id",
                "name",
                "description",
                "statement",
                "owner",
                "last_updated_ms",
                "metadata",
                "created_at",
            }
        }
        view_name = view.get("name")

        view["view_url"] = f"{base_url}/v1/views/{owner}/{view_name}"
        view_list.append(view)
    return view_list


@router.post("/views/{owner}", response_class=ORJSONResponse)
async def create_view(
    request: Request,
    view_definition: CreateViewRequest,
    owner: str = Path(description="The owner of the view.", pattern=IDENTIFIER_REG_EX),
):
    from tarchia.exceptions import AlreadyExistsError
    from tarchia.utils import generate_uuid
    from tarchia.utils.catalogs import identify_owner

    # can we find the owner?
    owner_entry = identify_owner(name=owner)

    base_url = get_base_url(request=request)
    timestamp = int(time.time_ns() / 1e6)

    # check if we have a table with that name already
    table_exists = catalog_provider.get_table(owner=owner, table=view_definition.name)
    if table_exists:
        # return a 409
        raise AlreadyExistsError(entity=view_definition.name)

    catalog_entry = catalog_provider.get_view(owner=owner, view=view_definition.name)
    if catalog_entry:
        # return a 409
        raise AlreadyExistsError(entity=view_definition.name)

    view_id = generate_uuid()

    # We create tables without any data
    new_view = ViewCatalogEntry(
        name=view_definition.name,
        statement=view_definition.statement,
        owner=owner,
        view_id=view_id,
        format_version=1,
        description=view_definition.description,
        metadata=view_definition.metadata,
        last_updated_ms=timestamp,
    )
    # Save the table to the Catalog - do this last
    catalog_provider.update_view(view_id=new_view.view_id, entry=new_view)

    # trigger webhooks - this should be async so we don't wait for the outcome
    owner_entry.trigger_event(
        owner_entry.EventTypes.VIEW_CREATED,
        {
            "event": "VIEW_CREATED",
            "view": f"{owner}.{view_definition.name}",
            "url": f"{base_url}/v1/views/{owner}/{view_definition.name}",
        },
    )

    return {
        "message": "View Created",
        "view": f"{owner}.{view_definition.name}",
    }


@router.get("/views/{owner}/{view}", response_class=ORJSONResponse)
async def get_view(
    owner: str = Path(description="The owner of the view.", pattern=IDENTIFIER_REG_EX),
    view: str = Path(description="The view.", pattern=IDENTIFIER_REG_EX),
):
    from tarchia.utils.catalogs import identify_view

    catalog_entry = identify_view(owner, view)

    return catalog_entry.as_dict()


@router.delete("/views/{owner}/{view}", response_class=ORJSONResponse)
async def delete_view(
    owner: str = Path(description="The owner of the view.", pattern=IDENTIFIER_REG_EX),
    view: str = Path(description="The view.", pattern=IDENTIFIER_REG_EX),
):
    from tarchia.utils.catalogs import identify_owner
    from tarchia.utils.catalogs import identify_view

    owner_entry = identify_owner(name=owner)
    catalog_entry = identify_view(owner=owner, view=view)

    view_id = catalog_entry.view_id
    catalog_provider.delete_view(view_id)

    # trigger webhooks - this should be async so we don't wait for the outcome
    owner_entry.trigger_event(
        owner_entry.EventTypes.VIEW_DELETED,
        {"event": "VIEW_DELETED", "view": f"{owner}.{view}"},
    )

    return {
        "message": "View Deleted",
        "view": f"{owner}.{view}",
    }


@router.patch("/views/{owner}/{view}/{attribute}", response_class=ORJSONResponse)
async def update_view(
    value: UpdateValueRequest,
    attribute: str,
    owner: str = Path(description="The owner of the table.", pattern=IDENTIFIER_REG_EX),
    view: str = Path(description="The name of the view.", pattern=IDENTIFIER_REG_EX),
):
    from tarchia.utils.catalogs import identify_view

    if attribute not in {"statement", "description"}:
        raise ValueError(f"Data attribute {attribute} cannot be modified via the API")

    catalog_entry = identify_view(owner, view)
    setattr(catalog_entry, attribute, value.value)
    catalog_provider.update_view(view_id=catalog_entry.view_id, entry=catalog_entry)

    return {
        "message": "View updated",
        "view": f"{owner}.{view}",
    }


@router.patch("/views/{owner}/{view}/metadata")
async def update_metadata(
    metadata: UpdateMetadataRequest,
    owner: str = Path(description="The owner of the table.", pattern=IDENTIFIER_REG_EX),
    view: str = Path(description="The name of the view.", pattern=IDENTIFIER_REG_EX),
):
    from tarchia.utils.catalogs import identify_view

    catalog_entry = identify_view(owner, view)
    view_id = catalog_entry.view_id
    catalog_entry.metadata = metadata.metadata
    catalog_provider.update_view(view_id=view_id, entry=catalog_entry)

    return {
        "message": "Metadata updated",
        "view": f"{owner}.{view}",
    }
