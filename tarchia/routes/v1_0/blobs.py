from io import BytesIO

import config
import models.v1_0
import numpy
import orjson
from cityhash import CityHash64
from fastapi import APIRouter, HTTPException, Path, Query, Request, Response
from fastapi.responses import ORJSONResponse
from profiler.blob_reader import read_blob
from profiler.stats_builder import build_stats

from opteryx.managers.kvstores import KV_store_factory

blobs = APIRouter(prefix="/v1.0/blobs", tags=["Blobs"])

BLOB_COLLECTION_NAME = config.BLOB_COLLECTION_NAME
METASTORE_HOST = config.METASTORE_HOST

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


@blobs.get("/{blob:path}", response_class=ORJSONResponse)
async def get_blob(blob: str, request: Request):
    """
    Get the detail for a specific blob
    """
    store = KV_store_factory(METASTORE_HOST)(location=BLOB_COLLECTION_NAME)
    if all(c in ('0123456789abcdef') for c in blob.lower()):
        # if we have a hex string, we may have been given the blob id, that's okay
        key = blob
    else:
        key = CityHash64(blob).to_bytes(8, "big").hex()

    while True:
        print("KEY", key)
        doc = store.get(key)
        if doc is None:
            raise HTTPException(status_code=404, detail="No matching blob found")
        doc = orjson.loads(doc.read())
        if doc.get("location") == blob or doc.get("blob_id") == blob:
            break
        key = CityHash64(key + blob).to_bytes(8, "big").hex()
        doc = store.get(key)

    return doc


@blobs.post("/", response_class=ORJSONResponse)
async def create_or_update_blob(blob: models.v1_0.NewBlob, request: Request
):
    """
    Add a new blob to the dataset
    """

    # read the request payload
    payload = await request.body()
    payload = orjson.loads(payload)

    # fetch the blob
    location = payload["location"]
    raw_bytes, page = read_blob(location)
    
    # profile the blob
    profile = build_stats(page)
    profile["location"] = location
    profile["raw_bytes"] = raw_bytes
    profile["data_bytes"] = page.nbytes
    profile["count"] = page.num_rows
    
    # save the blob profile to the metastore
    store = KV_store_factory(METASTORE_HOST)(location=BLOB_COLLECTION_NAME)
    key = CityHash64(location).to_bytes(8, "big").hex()
    while True:
        doc = store.get(key)
        if doc is None:
            break
        doc = orjson.loads(doc.read())
        if doc.get("location") == location:
            break
        key = CityHash64(key + location.encode()).to_bytes(8, "big").hex()
    profile["blob_id"] = key
    store.set(key, dumps(profile))

    # return the ID for the blob
    return key
