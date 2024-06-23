"""
A very basic auth system.

If the system is running locally we don't need a token, if we're not reunning locally
then a cookie or an auth header must include the same value as an environment variable.
"""

import os
from typing import Awaitable
from typing import Callable

from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response


class AuthorizationMiddleware(BaseHTTPMiddleware):
    async def dispatch(
        self, request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        if request.url.hostname not in ("127.0.0.1", "localhost", "testserver"):
            print(request.url.hostname)

            auth_token = None
            if "AUTH_TOKEN" in request.cookies:
                auth_token = request.cookies["AUTH_TOKEN"]
            else:
                for header, value in request.headers.items():
                    if header.lower() == "authorization":
                        auth_token = value.split(" ")[1] if len(value.split(" ")) == 2 else None

            if auth_token is None:
                return Response(status_code=401)
            if os.environ.get("AUTH_TOKEN") != auth_token:
                return Response(status_code=403)

        return await call_next(request)
