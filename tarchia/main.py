from os import environ

from fastapi import FastAPI
from uvicorn import run
import v1.routes

application = FastAPI(title="Tarchia Metastore")
# tarchia.include_router(v1_0.columns)
application.include_router(v1.routes.app)


if __name__ == "__main__":
    run("main:application", host="0.0.0.0", port=int(environ.get("PORT", 8080)))  # nosec
