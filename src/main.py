from os import environ

from fastapi import FastAPI
from uvicorn import run

from routes import v1_0

tarchia = FastAPI(
    title="Tarchia Metastore"
)
tarchia.include_router(v1_0.columns)
tarchia.include_router(v1_0.datasets)

if __name__ == "__main__":
    run(
        "main:tarchia",
        host="0.0.0.0",  # nosec
        port=int(environ.get("PORT", 8080))
    )




