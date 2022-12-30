from io import BytesIO

import numpy
import orjson

from cityhash import CityHash64
from fastapi import APIRouter, HTTPException, Request, Response, Query, Path
from fastapi.responses import ORJSONResponse
from opteryx.managers.kvstores import KV_store_factory

import models.v1_0

from profiler.blob_reader import read_blob
from profiler.stats_builder import build_stats

datasets = APIRouter(prefix="/v1.0/datasets", tags=["Datasets"])

## all class methods below here have been added for Opteryx
def dumps(obj):  # pragma: no cover
    import orjson

    def handler(obj):
        if isinstance(obj, numpy.integer):
            return int(obj)
        if isinstance(obj, numpy.inexact):
            return float(obj)
        raise TypeError

    byte_dump = orjson.dumps(obj, default=handler, option=orjson.OPT_SERIALIZE_NUMPY)
    return BytesIO(byte_dump)


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


@datasets.get("/{dataset}/blobs", response_class=ORJSONResponse)
async def get_blobs_in_dataset(
    request: Request,
    dataset: str = Path(
        None,
        example="123456",
        description="Dataset ID for the dataset to find blobs for.",
    ),
    prefix: str = None,
):
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
async def create_or_update_blob(
    dataset: str, blob: models.v1_0.NewBlob, request: Request
):
    """
    Add a new blob to the dataset
    """

    # read the payload
    payload = await request.body()
    payload = orjson.loads(payload)

    # fetch the blob
    location = payload["location"]
    page = read_blob(location)
    # profile the blob
    profile = build_stats(page)
    profile["location"] = location
    # save the blob profile to the metastore

    store = KV_store_factory("rocksdb")(location="opteryx_blobs")

    key = CityHash64(location).to_bytes(8, "big").hex()
    while True:
        doc = store.get(key)
        if doc is None:
            break
        doc = orjson.loads(doc.read())
        if doc.get("location") == location:
            break
        key = CityHash64(key + location.encode()).to_bytes(8, "big").hex()

    store.set(key, dumps(profile))

    # return the ID for the blob
    return key
