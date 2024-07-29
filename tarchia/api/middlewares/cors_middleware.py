from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

allowed_origins_regex = r"https://tarchia-app-cuvdwt7kra-uc\.a\.run\.app"


def bind(app: FastAPI):
    app.add_middleware(
        CORSMiddleware,
        allow_origin_regex=allowed_origins_regex,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
