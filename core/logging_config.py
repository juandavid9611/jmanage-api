import json
import logging
import os
import re
from datetime import datetime, timezone

PII_PATTERNS = [
    re.compile(r'("password"\s*:\s*")([^"]+)(")', re.IGNORECASE),
    re.compile(r'("token"\s*:\s*")([^"]+)(")', re.IGNORECASE),
    re.compile(r'("[\w-]*email[\w-]*"\s*:\s*")([^"]+)(")', re.IGNORECASE),
    re.compile(r'("authorization"\s*:\s*")([^"]+)(")', re.IGNORECASE),
]

def _mask_pii(text: str) -> str:
    s = text
    for pat in PII_PATTERNS:
        s = pat.sub(r'\1***\3', s)
    return s

class JSONFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        ts = datetime.now(timezone.utc).isoformat()
        payload = {
            "ts": ts,
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "service": getattr(record, "service", os.getenv("SERVICE_NAME", "sportsmanagement-api")),
            "env": getattr(record, "env", os.getenv("ENV", "local")),
            "version": os.getenv("APP_VERSION", "dev"),
            "request_id": getattr(record, "request_id", None),
            "method": getattr(record, "method", None),
            "path": getattr(record, "path", None),
            "status_code": getattr(record, "status_code", None),
            "duration_ms": getattr(record, "duration_ms", None),
            "user_id": getattr(record, "user_id", None),
            "tenant_id": getattr(record, "tenant_id", None),
            "error_type": getattr(record, "error_type", None),
            "error_stack": getattr(record, "error_stack", None),
        }
        payload = {k: v for k, v in payload.items() if v is not None}
        return _mask_pii(json.dumps(payload, ensure_ascii=False))

def configure_logging():
    level = os.getenv("LOG_LEVEL", "INFO").upper()
    root = logging.getLogger()
    root.setLevel(level)

    for h in list(root.handlers):
        root.removeHandler(h)

    handler = logging.StreamHandler()
    handler.setFormatter(JSONFormatter())
    root.addHandler(handler)

    for name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
        logger = logging.getLogger(name)
        logger.handlers = []
        logger.propagate = True
        logger.setLevel(level)
