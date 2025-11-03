import logging
import traceback
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from utils.slack_alerts import alert_with_stack
from fastapi.exceptions import RequestValidationError
from starlette.exceptions import HTTPException as StarletteHTTPException

logger = logging.getLogger(__name__)

def install_error_handlers(app: FastAPI):
    @app.exception_handler(StarletteHTTPException)
    async def http_exception_handler(request: Request, exc: StarletteHTTPException):
        request_id = getattr(request.state, "request_id", None)
        logger.warning(
            "http.exception",
            extra={
                "request_id": request_id,
                "method": request.method,
                "path": request.url.path,
                "status_code": exc.status_code,
                "error_type": "HTTPException",
            },
        )
        if exc.status_code >= 500:
            stack = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
            alert_with_stack(
                title=f"HTTPException {exc.status_code}",
                detail_fields={
                    "request_id": request_id,
                    "method": request.method,
                    "path": request.url.path,
                },
                stack=stack,
                level="error",
            )
        return JSONResponse(
            status_code=exc.status_code,
            content={
                "title": "HTTP Error",
                "detail": exc.detail,
                "status": exc.status_code,
                "request_id": request_id,
            },
        )

    @app.exception_handler(RequestValidationError)
    async def validation_exception_handler(request: Request, exc: RequestValidationError):
        request_id = getattr(request.state, "request_id", None)
        logger.warning(
            "validation.exception",
            extra={
                "request_id": request_id,
                "method": request.method,
                "path": request.url.path,
                "status_code": 422,
                "error_type": "ValidationError",
            },
        )
        alert_with_stack(
            title="ValidationError 422",
            detail_fields={"request_id": request_id, "method": request.method, "path": request.url.path},
            stack="",  # normalmente no hay stack útil
            level="warning",
        )
        return JSONResponse(
            status_code=422,
            content={
                "title": "Validation Error",
                "detail": exc.errors(),
                "status": 422,
                "request_id": request_id,
            },
        )

    @app.exception_handler(Exception)
    async def unhandled_exception_handler(request: Request, exc: Exception):
        request_id = getattr(request.state, "request_id", None)
        stack = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
        logger.error(
            "unhandled.exception",
            extra={
                "request_id": request_id,
                "method": request.method,
                "path": request.url.path,
                "status_code": 500,
                "error_type": type(exc).__name__,
                "error_stack": stack,
            },
        )
        alert_with_stack(
            title=f"Unhandled {type(exc).__name__}",
            detail_fields={
                "request_id": request_id,
                "method": request.method,
                "path": request.url.path,
            },
            stack=stack,
            level="critical",
        )
        return JSONResponse(
            status_code=500,
            content={
                "title": "Internal Server Error",
                "detail": "Unexpected error",
                "status": 500,
                "request_id": request_id,
            },
        )
