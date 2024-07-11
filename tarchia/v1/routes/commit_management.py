"""
Commits only for the main branch.

[GET] /repos/{owner}/{repo}/commits
[GET] /repos/{owner}/{repo}/commits/{commit_sha}
"""

import datetime
from typing import Literal
from typing import Optional
from typing import Union

import orjson
from fastapi import APIRouter
from fastapi import Path
from fastapi import Query
from fastapi import Request
from fastapi.responses import ORJSONResponse

from tarchia.constants import COMMITS_ROOT
from tarchia.constants import HISTORY_ROOT
from tarchia.constants import IDENTIFIER_REG_EX
from tarchia.constants import MAIN_BRANCH
from tarchia.exceptions import CommitNotFoundError
from tarchia.exceptions import TableHasNoDataError
from tarchia.models import Schema

router = APIRouter()


@router.get("/tables/{owner}/{table}/commits/{commit_sha}", response_class=ORJSONResponse)
async def get_table_commit(
    request: Request,
    owner: str = Path(description="The owner of the table.", pattern=IDENTIFIER_REG_EX),
    table: str = Path(description="The name of the table.", pattern=IDENTIFIER_REG_EX),
    commit_sha: Union[int, Literal["latest"]] = Path(description="The commit to retrieve."),
    filters: Optional[str] = Query(None, description="Filters to push to manifest reader"),
):
    from tarchia.manifests import get_manifest
    from tarchia.manifests.pruning import parse_filters
    from tarchia.storage import storage_factory
    from tarchia.utils import build_root
    from tarchia.utils.catalogs import identify_table

    base_url = request.url.scheme + "://" + request.url.netloc

    # read the data from the catalog for this table
    catalog_entry = identify_table(owner, table)
    table_id = catalog_entry.table_id
    if commit_sha == "latest":
        commit_sha = catalog_entry.current_commit_sha

    commit_root = build_root(COMMITS_ROOT, owner=owner, table_id=table_id)
    storage_provider = storage_factory()
    commit_file = storage_provider.read_blob(f"{commit_root}/commit-{commit_sha}.json")
    if not commit_file:
        raise CommitNotFoundError(owner, table, commit_sha)

    commit_entry = orjson.loads(commit_file)

    # retrieve the list of blobs from the manifests
    filters = parse_filters(filters, Schema(**commit_entry["table_schema"]))
    blobs = [
        (entry.file_path, entry.file_size, entry.record_count)
        for entry in get_manifest(commit_entry.get("manifest_path"), storage_provider, filters)
    ]

    # build the response
    table_definition = catalog_entry.as_dict()
    table_definition.update(commit_entry)
    table_definition.pop("current_commit_sha", None)
    table_definition.pop("current_schema", None)
    table_definition.pop("last_updated_ms", None)
    table_definition.pop("partitioning")
    table_definition.pop("location")
    table_definition["commit_sha"] = commit_sha
    table_definition["commit_url"] = (
        f"{base_url}/v1/tables/{catalog_entry.owner}/{catalog_entry.name}/commits/{commit_sha}"
    )
    table_definition["blobs"] = blobs

    return table_definition


@router.get("/tables/{owner}/{table}/commits", response_class=ORJSONResponse)
async def get_list_of_table_commits(
    request: Request,
    owner: str = Path(description="The owner of the table.", pattern=IDENTIFIER_REG_EX),
    table: str = Path(description="The name of the table.", pattern=IDENTIFIER_REG_EX),
    before: datetime.datetime = Query(None, description="Filter commits"),
    after: datetime.datetime = Query(None, description="Filter commits"),
    page_size: int = Query(100, description="Maximum items to show"),
):
    from tarchia.history import HistoryTree
    from tarchia.storage import storage_factory
    from tarchia.utils import build_root
    from tarchia.utils.catalogs import identify_table

    # read the data from the catalog for this table
    catalog_entry = identify_table(owner, table)
    table_id = catalog_entry.table_id

    storage_provider = storage_factory()
    history_root = build_root(HISTORY_ROOT, owner=owner, table_id=table_id)
    history = None
    if catalog_entry.current_history:
        history_file = f"{history_root}/history-{catalog_entry.current_history}.avro"
        history_raw = storage_provider.read_blob(history_file)
        if history_raw:
            history = HistoryTree.load_from_avro(history_raw)

    if history:
        for commit in history.walk_branch(MAIN_BRANCH):
            print(commit)

    # if there's more, give the URL for the next page
