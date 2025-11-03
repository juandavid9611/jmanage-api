import logging
import time
import uuid
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

logger = logging.getLogger(__name__)
REQUEST_ID_HEADER = "X-Request-ID"

class RequestContextMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        request_id = request.headers.get(REQUEST_ID_HEADER, str(uuid.uuid4()))
        request.state.request_id = request_id

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
