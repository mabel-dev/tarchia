from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

allowed_origins_regex = "http(s)?:\/\/.(localhost|.*\.run\.app)(:\d{1,5})?$"


def bind(app: FastAPI):
    app.add_middleware(
        CORSMiddleware,
        allow_origin_regex=allowed_origins_regex,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
