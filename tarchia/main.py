# isort: skip
"""
Tarchia is the metadata store for Mabel and Opteryx.

It is inspired by the Apache Iceberg REST API.
"""

import os
import sys
from os import environ

from fastapi import FastAPI
from uvicorn import run

sys.path.insert(1, os.path.join(sys.path[0], ".."))  # isort: skip

import tarchia  # isort: skip
from tarchia.middlewares import AuthorizationMiddleware  # isort: skip
from tarchia.middlewares import AuditMiddleware  # isort: skip
from tarchia.v1 import routes as v1_routes  # isort: skip


application = FastAPI(title="Tarchia Metastore")

application.include_router(v1_routes.v1_router)
application.add_middleware(AuthorizationMiddleware)
application.add_middleware(AuditMiddleware)


if __name__ == "__main__":
    run("main:application", host="0.0.0.0", port=int(environ.get("PORT", 8080)))  # nosec
