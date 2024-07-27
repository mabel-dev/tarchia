"""
Tarchia Metastore Application

Tarchia is the metadata store for Mabel and Opteryx, inspired by the Apache Iceberg REST API.
This module sets up the FastAPI application, including routes and middleware, for the Tarchia
metadata store.

Structure:
- Imports standard library modules and third-party modules.
- Initializes the FastAPI application and includes API routes and middleware.
- Runs the application when executed as the main module.

Imports:
- environ: A mapping object representing the string environment.
- FastAPI: FastAPI framework class for creating the application instance.
- run: Function to run the ASGI application using Uvicorn.
- AuthorizationMiddleware: Custom middleware for authorization.
- AuditMiddleware: Custom middleware for auditing.
- v1_routes: API routes for version 1 of the Tarchia API.

Usage:
- To run the application, execute this module directly. The application will start and listen
  on the specified port (default is 8080).

Example:
    $ python main.py

Notes:
- The `pragma: no cover` and `nosec` comments are used to handle coverage and security warnings
  appropriately.
- Basic error handling is included for the environment variable conversion.
"""

from os import environ

from fastapi import FastAPI
from uvicorn import run

from tarchia import __version__
from tarchia.api.middlewares import audit_middleware
from tarchia.api.middlewares import authorization_middleware
from tarchia.api.middlewares import brotli_middleware
from tarchia.api.middlewares import cors_middleware
from tarchia.api.v1 import v1_router

application = FastAPI(title="Tarchia Metastore", version=__version__)

application.include_router(v1_router)
brotli_middleware.bind(application)
audit_middleware.bind(application)
authorization_middleware.bind(application)
cors_middleware.bind(application)

if __name__ == "__main__":  # pragma: no cover
    try:
        port = int(environ.get("PORT", 8080))
    except ValueError:
        port = 8080  # default to 8080 if environment variable is invalid

    run("main:application", host="0.0.0.0", port=port)  # nosec
