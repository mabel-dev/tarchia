from fastapi import APIRouter

from .authorization import router as authorization_router
from .data_management import router as data_router
from .maintenance import router as maintenance_router
from .owner_management import router as owner_router
from .schema_management import router as schema_router
from .table_management import router as table_router

v1_router = APIRouter(prefix="/v1")
# v1_router.include_router(authorization_router, tags=["Access Control"])
# v1_router.include_router(data_router, tags=["Data Management"])
# v1_router.include_router(maintenance_router, tags=["Maintenance"])
# v1_router.include_router(owner_router, tags=["Owner Management"])
v1_router.include_router(schema_router, tags=["Schema Management"])
v1_router.include_router(table_router, tags=["Table Management"])
