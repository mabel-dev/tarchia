

from fastapi import APIRouter, HTTPException, Request, Response
from fastapi.responses import ORJSONResponse

v1_0 = APIRouter(prefix="/v1.0")

@v1_0.get("/table", response_class=ORJSONResponse)
async def get_table(request: Request):

    return {}