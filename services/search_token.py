"""Opaque, account-bound pagination tokens for product search."""
import base64
import hashlib
import json
from typing import Any


class InvalidTokenError(ValueError):
    pass


def fingerprint(params: dict[str, Any]) -> str:
    raw = json.dumps(params, sort_keys=True, default=str)
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


def encode_token(account_id: str, offset: int, fp: str) -> str:
    payload = json.dumps({"v": 1, "a": account_id, "o": offset, "f": fp}, separators=(",", ":"))
    return base64.urlsafe_b64encode(payload.encode()).decode().rstrip("=")


def decode_token(token: str, account_id: str, fp: str) -> int:
    try:
        padded = token + "=" * (-len(token) % 4)
        data = json.loads(base64.urlsafe_b64decode(padded.encode()))
        offset = int(data["o"])
        if data.get("v") != 1 or offset < 0:
            raise ValueError
    except Exception:
        raise InvalidTokenError("Invalid nextToken")
    if data.get("a") != account_id:
        raise InvalidTokenError("nextToken does not belong to this account")
    if data.get("f") != fp:
        raise InvalidTokenError("nextToken does not match the search parameters")
    return offset
