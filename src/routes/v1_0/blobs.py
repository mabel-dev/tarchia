

from fastapi import APIRouter, HTTPException, Request, Response
from fastapi.responses import ORJSONResponse

blobs = APIRouter(prefix="/v1.0/blobs", tags=["Blobs"])

@blobs.get("", response_class=ORJSONResponse)
async def get_blobs(request: Request):

    return {}

@blobs.post("", response_class=ORJSONResponse)
async def post_blobs(request: Request):

    return {}

@blobs.put("", response_class=ORJSONResponse)
async def put_blobs(request: Request):

    return {}

@blobs.delete("", response_class=ORJSONResponse)
async def delete_blobs(request: Request):

    return {}