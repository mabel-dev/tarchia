"""

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

from tarchia.exceptions import CommitNotFoundError
from tarchia.models import Schema
from tarchia.utils.constants import COMMITS_ROOT
from tarchia.utils.constants import HISTORY_ROOT
from tarchia.utils.constants import IDENTIFIER_REG_EX
from tarchia.utils.constants import MAIN_BRANCH

router = APIRouter()


@router.get("/tables/{owner}/{table}/commits/{commit_sha}", response_class=ORJSONResponse)
async def get_table_commit(
    request: Request,
    owner: str = Path(description="The owner of the table.", pattern=IDENTIFIER_REG_EX),
    table: str = Path(description="The name of the table.", pattern=IDENTIFIER_REG_EX),
    commit_sha: Union[str, Literal["head"]] = Path(description="The commit to retrieve."),
    filters: Optional[str] = Query(None, description="Filters to push to manifest reader"),
):
    from tarchia.interfaces.storage import storage_factory
    from tarchia.metadata.manifests import get_manifest
    from tarchia.metadata.manifests.pruning import parse_filters
    from tarchia.utils import build_root
    from tarchia.utils.catalogs import identify_table

    base_url = request.url.scheme + "://" + request.url.netloc

    # read the data from the catalog for this table
    catalog_entry = identify_table(owner, table)
    table_id = catalog_entry.table_id
    if commit_sha == "head":
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
        {"path": entry.file_path, "bytes": entry.file_size, "records": entry.record_count}
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
    table_definition.pop("subscriptions")
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
    from tarchia.interfaces.storage import storage_factory
    from tarchia.metadata.history import HistoryTree
    from tarchia.utils import build_root
    from tarchia.utils.catalogs import identify_table

    base_url = request.url.scheme + "://" + request.url.netloc
    branch = MAIN_BRANCH

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
            history = HistoryTree.load_from_avro(history_raw, branch)

    response = {"table": f"{owner}.{table}", "branch": branch, "commits": []}
    if history:
        walker = history.walk_branch(MAIN_BRANCH)
        commit = next(walker, None)
        while commit:
            commit_timestamp = commit.timestamp
            if before and commit_timestamp >= int(before.timestamp() * 1000):
                commit = next(walker, None)
                continue
            if after and commit_timestamp < int(after.timestamp() * 1000):
                break

            commit_dict = commit.as_dict()
            commit_dict["commit_url"] = (
                f"{base_url}/v1/tables/{catalog_entry.owner}/{catalog_entry.name}/commits/{commit.sha}"
            )
            response["commits"].append(commit_dict)

            commit = next(walker, None)
            if len(response["commits"]) >= page_size:
                if commit:
                    before_timestamp = datetime.datetime.fromtimestamp(
                        commit.timestamp / 1000
                    ).strftime("%Y-%m-%dT%H:%M:%S")
                    response["next_page"] = (
                        f"{base_url}/v1/tables/{owner}/{table}/commits?page_size={page_size}&before={before_timestamp}"
                    )
                    if after:
                        after_timestamp = after.strftime("%Y-%m-%dT%H:%M:%S")
                        response["next_page"] += f"&after={after_timestamp}"
                break

    return response
