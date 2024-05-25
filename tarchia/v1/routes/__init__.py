from fastapi import APIRouter

from .authorization import router as authorization_router
from .data_management import router as data_router
from .maintenance import router as maintenance_router
from .metadata_management import router as metadata_router
from .table_management import router as table_router

v1_router = APIRouter(prefix="/v1")
v1_router.include_router(authorization_router, tags=["Access Control"])
v1_router.include_router(data_router, tags=["Data Management"])
v1_router.include_router(maintenance_router, tags=["Maintenance"])
v1_router.include_router(metadata_router, tags=["Metadata Management"])
v1_router.include_router(table_router, tags=["Table Management"])
