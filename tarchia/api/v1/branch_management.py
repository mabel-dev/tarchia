"""
[GET]   /tables/{owner}/{table}/branches/{branch}  (returns HEAD)
[POST]  /tables/{owner}/{table}/branches
[DELETE]/tables/{owner}/{table}/branches/{branch}
[POST]  /tables/{owner}/{table}/merge
"""

import orjson
from fastapi import APIRouter
from fastapi import Path
from fastapi import Query
from fastapi import Request
from fastapi.responses import ORJSONResponse

from tarchia.exceptions import CommitNotFoundError
from tarchia.models import Schema
from tarchia.utils.constants import IDENTIFIER_REG_EX

router = APIRouter()


@router.get("/tables/{owner}/{table}/branches/{branch}", response_class=ORJSONResponse)
async def get_branch(
    owner: str = Path(description="The owner of the table.", pattern=IDENTIFIER_REG_EX),
    table: str = Path(description="The name of the table.", pattern=IDENTIFIER_REG_EX),
    branch: str = Path(description="The name of the branch.", pattern=IDENTIFIER_REG_EX),
):
    # Your logic to retrieve branch information, including the HEAD commit
    branch_info = {
        "owner": owner,
        "table": table,
        "branch": branch,
        "head_commit": "commit_sha_of_head",  # Replace with actual HEAD commit SHA
        "commits": ["commit1", "commit2", "commit3"],  # Example commits
    }
    return branch_info


@router.post("/tables/{owner}/{table}/branches", response_class=ORJSONResponse)
async def create_branch(
    owner: str = Path(description="The owner of the table.", pattern=IDENTIFIER_REG_EX),
    table: str = Path(description="The name of the table.", pattern=IDENTIFIER_REG_EX),
    branch: str = Query(description="The name of the new branch.", pattern=IDENTIFIER_REG_EX),
    source_branch: str = Query(
        description="The name of the source branch to create from.", pattern=IDENTIFIER_REG_EX
    ),
):
    # Your logic to create a new branch from the source branch
    new_branch_info = {
        "owner": owner,
        "table": table,
        "branch": branch,
        "source_branch": source_branch,
        "message": "Branch created successfully",
    }
    return new_branch_info


@router.delete("/tables/{owner}/{table}/branches/{branch}", response_class=ORJSONResponse)
async def delete_branch(
    owner: str = Path(description="The owner of the table.", pattern=IDENTIFIER_REG_EX),
    table: str = Path(description="The name of the table.", pattern=IDENTIFIER_REG_EX),
    branch: str = Path(description="The name of the branch to delete.", pattern=IDENTIFIER_REG_EX),
):
    # Your logic to delete a branch
    delete_info = {
        "owner": owner,
        "table": table,
        "branch": branch,
        "message": "Branch deleted successfully",
    }
    return delete_info


@router.post("/tables/{owner}/{table}/merge", response_class=ORJSONResponse)
async def merge_branch(
    owner: str = Path(description="The owner of the table.", pattern=IDENTIFIER_REG_EX),
    table: str = Path(description="The name of the table.", pattern=IDENTIFIER_REG_EX),
    base: str = Query(description="The branch to merge into.", pattern=IDENTIFIER_REG_EX),
    head: str = Query(description="The branch to merge from.", pattern=IDENTIFIER_REG_EX),
):
    # Your logic to perform the merge operation
    # For now, we assume there are no merge conflicts

    # Example response, replace with actual merge logic
    merge_info = {
        "owner": owner,
        "table": table,
        "base_branch": base,
        "head_branch": head,
        "message": "Branches merged successfully",
    }
    return merge_info
