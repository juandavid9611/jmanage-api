"""Pure product rules (pricing, search matching). No HTTP, no DB."""
import re
import unicodedata
from decimal import Decimal
from typing import Any, Iterable, Optional

PUBLISH_VALUES = ("published", "draft")
MAX_NAME_LEN = 200


def to_decimal(value: Any) -> Decimal:
    if isinstance(value, Decimal):
        return value
    if value is None:
        return Decimal("0")
    return Decimal(str(value))


def validate_price_sale(price: Any, price_sale: Optional[Any]) -> None:
    """Contract 1: priceSale is optional; when set it must satisfy 0 <= priceSale < price."""
    if price_sale is None:
        return
    sale = to_decimal(price_sale)
    if sale < 0 or sale >= to_decimal(price):
        raise ValueError("priceSale must be >= 0 and lower than price")


def normalize_price_sale(price: Any, price_sale: Optional[Any]) -> Optional[Decimal]:
    """Defensive read-side normalisation of legacy data.

    Legacy products stored 0 (or a value >= price) to mean "no discount".
    Those are treated as no discount so they are never sold for free.
    """
    if price_sale is None:
        return None
    sale = to_decimal(price_sale)
    if sale <= 0 or sale >= to_decimal(price):
        return None
    return sale


def effective_price(price: Any, price_sale: Optional[Any]) -> Decimal:
    """Live unit price: the valid discounted price when set, else the regular price."""
    sale = normalize_price_sale(price, price_sale)
    return sale if sale is not None else to_decimal(price)


def fold(text: str) -> str:
    """Lowercase and strip accents for search comparison."""
    nfkd = unicodedata.normalize("NFKD", text or "")
    return "".join(c for c in nfkd if not unicodedata.combining(c)).lower()


def query_tokens(q: Optional[str]) -> list[str]:
    return [t for t in re.split(r"[^0-9a-z]+", fold(q or "")) if t]


def matches_query(name: str, tags: Iterable[str], q: Optional[str]) -> bool:
    """Every query token must appear in the product name or in one of its tags."""
    tokens = query_tokens(q)
    if not tokens:
        return True
    haystacks = [fold(name or "")] + [fold(str(t)) for t in (tags or [])]
    return all(any(tok in h for h in haystacks) for tok in tokens)
