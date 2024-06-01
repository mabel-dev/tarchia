"""
Tarchia is the metadata store for Mabel and Opteryx.

It is inspired by the Apache Iceberg REST API.
"""

from os import environ

import v1.routes
from middlewares import AuthorizationMiddleware
from fastapi import FastAPI
from uvicorn import run

application = FastAPI(title="Tarchia Metastore")

application.include_router(v1.routes.v1_router)
application.add_middleware(AuthorizationMiddleware)


if __name__ == "__main__":
    run("main:application", host="0.0.0.0", port=int(environ.get("PORT", 8080)))  # nosec
