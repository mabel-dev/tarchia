

from fastapi import APIRouter, HTTPException, Request, Response
from fastapi.responses import ORJSONResponse

datasets = APIRouter(prefix="/v1.0/datasets", tags=["Datasets"])

@datasets.get("", response_class=ORJSONResponse)
async def list_datasets(request: Request):
    """
    Return all datasets in the store
    """
    return [
        {"preferred_name": "nvd.cves", "canonical_name": "c:/files/nvd/cves/", "schema": [{"name": "CVE", "type": "VARCHAR"}]}
    ]

@datasets.get("/{dataset}", response_class=ORJSONResponse)
async def get_dataset(dataset: str, request: Request):
    """
    Return a specific dataset
    """
    return {}

@datasets.post("", response_class=ORJSONResponse)
async def create_or_update_dataset(request: Request):
    """
    Create a new or update an existing dataset
    """
    return {}

@datasets.get("/{dataset}/blobs", response_class=ORJSONResponse)
async def get_blobs_in_dataset(dataset: str, request: Request):
    """
    Get the blobs for a dataset
    This should have an optional prefix filter on it
    """
    return {}

@datasets.get("/{dataset}/blobs/{blob}", response_class=ORJSONResponse)
async def get_blob(dataset: str, request: Request):
    """
    Get the detail for a specific blob
    """
    return {}

@datasets.post("/{dataset}/blobs", response_class=ORJSONResponse)
async def create_or_update_blob(dataset: str, request: Request):
    """
    Add a new blob to the dataset
    """
    return {}
