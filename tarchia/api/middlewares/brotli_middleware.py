from brotli_asgi import BrotliMiddleware
from fastapi import FastAPI


def bind(app: FastAPI):
    app.add_middleware(BrotliMiddleware)
