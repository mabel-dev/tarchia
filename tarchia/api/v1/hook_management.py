"""
/tables/{owner}/{table}/hooks
/owners/{owner}/hooks

/{hook_id}/
/{hook_id}/ping


we only record the latest attempt
"""

from fastapi import APIRouter
from fastapi import Request
from fastapi.responses import ORJSONResponse

router = APIRouter()


@router.get("/tables/{owner}/{table}/hooks", response_class=ORJSONResponse)
async def get_table_hooks(request: Request, owner: str):
    raise NotImplementedError("Not Implemented")


@router.post("/tables/{owner}/{table}/hooks", response_class=ORJSONResponse)
async def create_table_hooks(request: Request, owner: str):
    raise NotImplementedError("Not Implemented")


@router.get("/tables/{owner}/{table}/hooks/{hook}", response_class=ORJSONResponse)
async def get_table_hooks_by_id(request: Request, owner: str):
    raise NotImplementedError("Not Implemented")


@router.patch("/tables/{owner}/{table}/hooks/{hook}", response_class=ORJSONResponse)
async def update_table_hooks_by_id(request: Request, owner: str):
    raise NotImplementedError("Not Implemented")


@router.delete("/tables/{owner}/{table}/hooks/{hook}", response_class=ORJSONResponse)
async def delete_table_hooks_by_id(request: Request, owner: str):
    raise NotImplementedError("Not Implemented")


@router.get("/tables/{owner}/{table}/hooks/{hook}/ping", response_class=ORJSONResponse)
async def ping_table_hooks_by_id(request: Request, owner: str):
    raise NotImplementedError("Not Implemented")


@router.get("/owner/{owner}/hooks", response_class=ORJSONResponse)
async def get_owner_hooks(request: Request, owner: str):
    raise NotImplementedError("Not Implemented")


@router.post("/owner/{owner}/hooks", response_class=ORJSONResponse)
async def create_owner_hooks(request: Request, owner: str):
    raise NotImplementedError("Not Implemented")


@router.get("/owner/{owner}/hooks/{hook}", response_class=ORJSONResponse)
async def get_owner_hooks_by_id(request: Request, owner: str):
    raise NotImplementedError("Not Implemented")


@router.patch("/owner/{owner}/hooks/{hook}", response_class=ORJSONResponse)
async def update_owner_hooks_by_id(request: Request, owner: str):
    raise NotImplementedError("Not Implemented")


@router.delete("/owner/{owner}/hooks/{hook}", response_class=ORJSONResponse)
async def delete_owner_hooks_by_id(request: Request, owner: str):
    raise NotImplementedError("Not Implemented")


@router.get("/owner/{owner}/hooks/{hook}/ping", response_class=ORJSONResponse)
async def ping_owner_hooks_by_id(request: Request, owner: str):
    raise NotImplementedError("Not Implemented")
