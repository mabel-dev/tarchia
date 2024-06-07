"""
Audit Middleware

Record request information and handle error responses.

AuthenicationError -> 403
AuthorizationError -> 401
NotFoundError -> 404
InvalidEntryError -> 422
"""

import time
from typing import Awaitable
from typing import Callable

from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response

from tarchia.exceptions import InvalidFilterError


class AuditMiddleware(BaseHTTPMiddleware):

    async def dispatch(
        self, request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:

        try:

            start = time.monotonic_ns()
            result = await call_next(request)
            return result

        except InvalidFilterError as e:
            return Response(status_code=422, content=e)

        except Exception as e:
            from uuid import uuid4

            code = str(uuid4())
            print(f"{code}\n{e}")
            return Response(status_code=500, content=f"Unexpected Error ({code})")
        finally:
            print(time.monotonic_ns() - start)
