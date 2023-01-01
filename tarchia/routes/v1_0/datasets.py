from fastapi import APIRouter, HTTPException, Request, Response, Query, Path
from fastapi.responses import ORJSONResponse

import config
import models.v1_0


datasets = APIRouter(prefix="/v1.0/datasets", tags=["Datasets"])
DATASET_COLLECTION_NAME = config.DATASET_COLLECTION_NAME


@datasets.get("/", response_class=ORJSONResponse)
async def list_datasets(
    request: Request,
    filter: str = Query(
        None,
        example=".*opteryx.*",
        description="Regular Expression filter for dataset names (preferred and aliases).",
    ),
    describe: bool = Query(
        False, description="Return extended details, including schema information."
    ),
):
    """
    Return all datasets in the store, with optional regular expression filter.
    """
    return [
        {
            "id": "12345",
            "type": "dataset",
            "href": "https://tarchia.opteryx.app/datasets/12345",
            "preferred_name": "nvd.cves",
            "canonical_name": "c:/files/nvd/cves/",
            "aliases": [],
            "schema": [{"name": "CVE", "type": "VARCHAR"}],
            "permissions": [],
        }
    ]


@datasets.get("/{dataset}", response_class=ORJSONResponse)
async def get_dataset(
    request: Request,
    dataset: str = Path(
        None,
        example="123456",
        description="Dataset ID for the dataset to fetch the details for.",
    ),
):
    """
    Return a specific dataset
    """
    return {}


@datasets.post("/", response_class=ORJSONResponse)
async def create_or_update_dataset(request: Request):
    """
    Create a new or update an existing dataset
    """
    return {}

