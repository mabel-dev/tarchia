from fastapi import APIRouter
from fastapi import Request
from fastapi.responses import ORJSONResponse
from typing import List
router = APIRouter()



@router.get("/views/{owner}", response_class=ORJSONResponse)
async def list_views(request: Request, owner:str):
    raise NotImplementedError("Not Implemented")

@router.post("/views/{owner}", response_class=ORJSONResponse)
async def create_view(request: Request, owner:str):
    raise NotImplementedError("Not Implemented")

@router.get("/views/{owner}/{view}", response_class=ORJSONResponse)
async def get_view(request: Request, owner:str, view:str):
    raise NotImplementedError("Not Implemented")

@router.delete("/views/{owner}/{view}", response_class=ORJSONResponse)
async def delete_view(request: Request, owner:str, view:str):
    raise NotImplementedError("Not Implemented")