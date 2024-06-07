"""
Tarchia is the metadata store for Mabel and Opteryx.

It is inspired by the Apache Iceberg REST API.
"""

import os
import sys
from os import environ

import v1.routes
from fastapi import FastAPI
from middlewares import AuthorizationMiddleware
from uvicorn import run

import tarchia

sys.path.insert(1, os.path.join(sys.path[0], ".."))


application = FastAPI(title="Tarchia Metastore")

application.include_router(v1.routes.v1_router)
application.add_middleware(AuthorizationMiddleware)


if __name__ == "__main__":
    run("main:application", host="0.0.0.0", port=int(environ.get("PORT", 8080)))  # nosec
