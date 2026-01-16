import logging
import time
import uuid
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response
from jose import jwt

logger = logging.getLogger(__name__)
REQUEST_ID_HEADER = "X-Request-ID"

class RequestContextMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        request_id = request.headers.get(REQUEST_ID_HEADER, str(uuid.uuid4()))
        request.state.request_id = request_id
        
        # Extract user_id and account_id for error tracking
        request.state.user_id = None
        request.state.account_id = None
        
        # Try to extract user_id from JWT token
        auth_header = request.headers.get("Authorization", "")
        if auth_header.startswith("Bearer "):
            token = auth_header[7:]
            try:
                # Decode without verification (just to extract claims for logging)
                decoded = jwt.get_unverified_claims(token)
                request.state.user_id = decoded.get("sub")
            except Exception:
                pass  # Silently fail - auth will handle invalid tokens
        
        # Extract account_id from header or query param
        account_id = request.headers.get("X-Account-Id")
        if not account_id:
            # Try to get from query params
            account_id = request.query_params.get("account_id")
        request.state.account_id = account_id

        method = request.method
        path = request.url.path
        start = time.perf_counter()

        try:
            response: Response = await call_next(request)
        except Exception:
            logger.exception(
                "Unhandled exception pre-handler",
                extra={"request_id": request_id, "method": method, "path": path},
            )
            raise
        finally:
            duration_ms = round((time.perf_counter() - start) * 1000, 2)

        logger.info(
            "request.completed",
            extra={
                "request_id": request_id,
                "method": method,
                "path": path,
                "status_code": getattr(response, "status_code", None),
                "duration_ms": duration_ms,
            },
        )

        response.headers[REQUEST_ID_HEADER] = request_id
        return response
