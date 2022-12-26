

from fastapi import APIRouter, HTTPException, Request, Response
from fastapi.responses import ORJSONResponse

datasets = APIRouter(prefix="/v1.0/datasets", tags=["Datasets"])

@datasets.get("", response_class=ORJSONResponse)
async def list_datasets(request: Request):

    return {}

@datasets.get("/{dataset}", response_class=ORJSONResponse)
async def get_dataset(dataset: str, request: Request):

    return {}

@datasets.post("", response_class=ORJSONResponse)
async def post_datasets(request: Request):

    return {}

@datasets.put("", response_class=ORJSONResponse)
async def put_datasets(request: Request):

    return {}

@datasets.delete("", response_class=ORJSONResponse)
async def delete_datasets(request: Request):

    return {}

@datasets.get("/{dataset}/blobs", response_class=ORJSONResponse)
async def get_datasets(dataset: str, request: Request):

    return {}

@datasets.post("/{dataset}/blobs", response_class=ORJSONResponse)
async def post_datasets(dataset: str, request: Request):

    return {}

@datasets.put("/{dataset}/blobs", response_class=ORJSONResponse)
async def put_datasets(dataset: str, request: Request):

    return {}

@datasets.delete("/{dataset}/blobs", response_class=ORJSONResponse)
async def delete_datasets(dataset: str, request: Request):

    return {}