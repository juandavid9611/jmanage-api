"""Pure order rules: statuses, transitions, totals. No HTTP, no DB."""
from collections import OrderedDict
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Iterable, Optional

ORDER_STATUSES = ("pending", "processing", "paid", "completed", "cancelled", "refunded")

STATUS_TRANSITIONS: dict[str, frozenset] = {
    "pending": frozenset({"processing", "paid", "cancelled"}),
    "processing": frozenset({"paid", "completed", "cancelled"}),
    "paid": frozenset({"processing", "completed", "refunded", "cancelled"}),
    "completed": frozenset({"refunded"}),
    "cancelled": frozenset(),
    "refunded": frozenset(),
}

# Transitions that give the reserved stock back.
RESTOCK_STATUSES = frozenset({"cancelled", "refunded"})

CENT = Decimal("0.01")
MAX_ORDER_LINES = 50


class ShopError(Exception):
    """Domain error. `kind` is mapped to an HTTP status by the router."""

    def __init__(self, kind: str, message: str):
        super().__init__(message)
        self.kind = kind
        self.message = message


def can_transition(old: Optional[str], new: str) -> bool:
    return new in STATUS_TRANSITIONS.get(old or "", frozenset())


def money(value: Any) -> Decimal:
    d = value if isinstance(value, Decimal) else Decimal(str(value))
    return d.quantize(CENT, rounding=ROUND_HALF_UP)


def aggregate_quantities(lines: Iterable[dict]) -> "OrderedDict[str, int]":
    """Total quantity per product id (a product may appear with several size/color lines)."""
    totals: "OrderedDict[str, int]" = OrderedDict()
    for line in lines:
        pid = line["product_id"]
        totals[pid] = totals.get(pid, 0) + int(line["quantity"])
    return totals


def compute_totals(
    unit_prices_and_qty: Iterable[tuple[Decimal, int]],
    shipping: Any = 0,
    discount: Any = 0,
) -> dict:
    """Server-side totals. shipping/discount are validated, non-negative client inputs
    (there is no server-side source for them yet); discount is capped by the subtotal."""
    subtotal = money(sum((Decimal(p) * int(q) for p, q in unit_prices_and_qty), Decimal("0")))
    shipping_d = money(shipping or 0)
    discount_d = money(discount or 0)
    if shipping_d < 0 or discount_d < 0:
        raise ShopError("invalid", "shipping and discount must be >= 0")
    if discount_d > subtotal:
        raise ShopError("invalid", "discount cannot exceed the subtotal")
    return {
        "subtotal": subtotal,
        "shipping": shipping_d,
        "discount": discount_d,
        "total_amount": money(subtotal - discount_d + shipping_d),
    }
