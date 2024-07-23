from fastapi import APIRouter

from .commit_management import router as commit_router
from .data_management import router as data_router
from .owner_management import router as owner_router
from .table_management import router as table_router

v1_router = APIRouter(prefix="/v1")
v1_router.include_router(commit_router, tags=["Commit Management"])
v1_router.include_router(data_router, tags=["Data Management"])
v1_router.include_router(owner_router, tags=["Owner Management"])
v1_router.include_router(table_router, tags=["Table Management"])