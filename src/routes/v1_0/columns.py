

from fastapi import APIRouter, HTTPException, Request, Response
from fastapi.responses import ORJSONResponse

columns = APIRouter(prefix="/v1.0/columns", tags=["Columns"])

@columns.get("", response_class=ORJSONResponse)
async def get_columns(request: Request):

    return {}

@columns.post("", response_class=ORJSONResponse)
async def post_columns(request: Request):

    return {}

@columns.put("", response_class=ORJSONResponse)
async def put_columns(request: Request):

    return {}

@columns.delete("", response_class=ORJSONResponse)
async def delete_columns(request: Request):

    return {}