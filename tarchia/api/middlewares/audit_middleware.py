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

import orjson
from fastapi import FastAPI
from fastapi import HTTPException
from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response

from tarchia.exceptions import AlreadyExistsError
from tarchia.exceptions import DataEntryError
from tarchia.exceptions import NotFoundError


class AuditMiddleware(BaseHTTPMiddleware):
    async def dispatch(
        self, request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        audit_record = {
            "service": "tarchia",
            "end_point": request.url.path,
            "method": request.method,
        }

        outcome = "unknown"
        start = time.monotonic_ns()
        try:
            result = await call_next(request)
            outcome = "success"

            return result

        except HTTPException as error:
            outcome = "error"
            audit_record["message"] = str(error)
            raise error
        except DataEntryError as error:
            outcome = "error"
            audit_record["message"] = str(error)
            return Response(
                status_code=422,
                content=orjson.dumps({"fields": error.fields, "message": error.message}),
            )
        except NotFoundError as error:
            outcome = "error"
            audit_record["message"] = str(error)
            return Response(status_code=404, content=str(error))
        except AlreadyExistsError as error:
            outcome = "error"
            audit_record["message"] = str(error)
            return Response(status_code=409, content=str(error))
        except Exception as error:
            outcome = "error"
            audit_record["message"] = str(error)
            from uuid import uuid4

            code = str(uuid4())
            print(f"{code}\n{error}")
            raise error
            return Response(status_code=500, content=f"Unexpected Error ({code})")
        finally:
            audit_record["duration_ms"] = (time.monotonic_ns() - start) / 1e6
            audit_record["outcome"] = outcome

            print(orjson.dumps(audit_record).decode())


def bind(app: FastAPI):
    app.add_middleware(AuditMiddleware)
