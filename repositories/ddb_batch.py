"""BatchGetItem helper: chunking + unprocessed-key retry. No boto import so it is unit-testable."""
import time
from typing import Any, Callable

BATCH_GET_LIMIT = 100


def batch_get_items(
    batch_get: Callable[[dict], dict],
    table_name: str,
    keys: list[dict[str, Any]],
    *,
    chunk_size: int = BATCH_GET_LIMIT,
    max_retries: int = 6,
    sleep: Callable[[float], None] = time.sleep,
) -> list[dict[str, Any]]:
    """Fetch `keys` via `batch_get(RequestItems)` in chunks, retrying UnprocessedKeys with backoff."""
    unique: list[dict[str, Any]] = []
    seen = set()
    for key in keys:
        marker = tuple(sorted(key.items()))
        if marker not in seen:
            seen.add(marker)
            unique.append(key)

    items: list[dict[str, Any]] = []
    for start in range(0, len(unique), chunk_size):
        pending = {table_name: {"Keys": unique[start:start + chunk_size]}}
        attempt = 0
        while pending:
            resp = batch_get(pending)
            items.extend(resp.get("Responses", {}).get(table_name, []))
            pending = resp.get("UnprocessedKeys") or {}
            if pending:
                if attempt >= max_retries:
                    raise RuntimeError("BatchGetItem: unprocessed keys remain after retries")
                sleep(min(0.05 * (2 ** attempt), 1.0))
                attempt += 1
    return items
