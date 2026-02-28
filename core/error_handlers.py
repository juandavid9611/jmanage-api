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
        user_id = getattr(request.state, "user_id", None)
        account_id = getattr(request.state, "account_id", None)
        
        logger.warning(
            "http.exception",
            extra={
                "request_id": request_id,
                "user_id": user_id,
                "account_id": account_id,
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
                    "user_id": user_id,
                    "account_id": account_id,
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
        user_id = getattr(request.state, "user_id", None)
        account_id = getattr(request.state, "account_id", None)
        
        # Capture request body for debugging
        request_body = None
        try:
            body_bytes = await request.body()
            if body_bytes:
                request_body = body_bytes.decode("utf-8")
                # Try to parse as JSON for better formatting
                try:
                    import json
                    parsed = json.loads(request_body)
                    request_body = json.dumps(parsed, indent=2)
                except:
                    pass  # Keep as raw string if not JSON
        except Exception as e:
            request_body = f"[Could not read body: {e}]"
        
        # Format validation errors for readability
        validation_errors = exc.errors()
        errors_formatted = []
        for err in validation_errors:
            loc = " -> ".join(str(x) for x in err.get("loc", []))
            msg = err.get("msg", "")
            err_type = err.get("type", "")
            errors_formatted.append(f"• {loc}: {msg} ({err_type})")
        
        errors_text = "\n".join(errors_formatted[:10])  # Limit to first 10 errors
        if len(validation_errors) > 10:
            errors_text += f"\n... and {len(validation_errors) - 10} more errors"
        
        logger.warning(
            "validation.exception",
            extra={
                "request_id": request_id,
                "user_id": user_id,
                "account_id": account_id,
                "method": request.method,
                "path": request.url.path,
                "status_code": 422,
                "error_type": "ValidationError",
                "validation_errors": validation_errors,
            },
        )
        
        # Build detailed alert with request data and validation errors
        detail_fields = {
            "request_id": request_id,
            "user_id": user_id,
            "account_id": account_id,
            "method": request.method,
            "path": request.url.path,
            "validation_errors": errors_text,
        }
        
        if request_body:
            # Include full request body for debugging (no truncation)
            detail_fields["request_body"] = request_body
        
        alert_with_stack(
            title="ValidationError 422",
            detail_fields=detail_fields,
            stack="",
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
        user_id = getattr(request.state, "user_id", None)
        account_id = getattr(request.state, "account_id", None)
        
        stack = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
        logger.error(
            "unhandled.exception",
            extra={
                "request_id": request_id,
                "user_id": user_id,
                "account_id": account_id,
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
                "user_id": user_id,
                "account_id": account_id,
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
