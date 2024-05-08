from os import environ

from fastapi import FastAPI
from routes import v1_0
from uvicorn import run

application = FastAPI(title="Tarchia Metastore")
# tarchia.include_router(v1_0.columns)
application.include_router(v1_0.datasets)
application.include_router(v1_0.blobs)

if __name__ == "__main__":
    run(
        "main:application", host="0.0.0.0", port=int(environ.get("PORT", 8080))
    )  # nosec
