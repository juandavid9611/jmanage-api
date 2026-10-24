"""Money helpers for the finance module: Decimal only, never float."""
import re
import uuid
from decimal import Decimal, InvalidOperation
from typing import Any

MAX_AMOUNT = Decimal("1000000000000")  # 1e12
_TWO_PLACES = Decimal("0.01")
_IDEMPOTENCY_KEY = re.compile(r"^[A-Za-z0-9_-]{8,64}$")
_FILE_NAME_BAD = ("/", "\\", "..", "\x00")


class MoneyError(ValueError):
    """A money value that is not acceptable (the API turns it into a 422)."""


def parse_money(value: Any, *, allow_zero: bool = False, allow_negative: bool = False) -> Decimal:
    """str/int/float/Decimal -> Decimal with at most 2 decimals, finite and within +-MAX_AMOUNT.

    Floats are accepted only because JSON numbers arrive as floats; they go through `str()` so
    0.1 stays 0.1 (never 0.1000000000000000055...). Booleans are rejected.
    """
    if isinstance(value, bool) or value is None:
        raise MoneyError("amount must be a number")
    try:
        amount = value if isinstance(value, Decimal) else Decimal(str(value).strip())
    except (InvalidOperation, ValueError, AttributeError):
        raise MoneyError("amount must be a number") from None
    if not amount.is_finite():
        raise MoneyError("amount must be finite")
    if abs(amount) > MAX_AMOUNT:  # checked first: quantize() raises InvalidOperation on huge values
        raise MoneyError("amount is too large")
    if amount != amount.quantize(_TWO_PLACES):
        raise MoneyError("amount can have at most 2 decimals")
    amount = amount.quantize(_TWO_PLACES)
    if abs(amount) > MAX_AMOUNT:
        raise MoneyError("amount is too large")
    if amount < 0 and not allow_negative:
        raise MoneyError("amount cannot be negative")
    if amount == 0 and not allow_zero:
        raise MoneyError("amount must be greater than zero")
    return amount


def plain_number(value: Any) -> int | float | None:
    """Decimal -> int when integral, else float (what the JSON responses carry)."""
    if value is None:
        return None
    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() else float(value)
    return value


def new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex}"


def valid_idempotency_key(key: str | None) -> bool:
    return bool(key) and bool(_IDEMPOTENCY_KEY.match(key))


def valid_file_name(name: Any, *, max_len: int = 120) -> bool:
    """A plain file name: no path parts, no control characters, bounded length."""
    return (
        isinstance(name, str)
        and 0 < len(name) <= max_len
        and not any(bad in name for bad in _FILE_NAME_BAD)
        and name.strip() == name
    )
