"""Pydantic models for the budget catalog (snake_case on the wire)."""
from typing import Any

from pydantic import BaseModel, StrictInt


class BudgetItemCreate(BaseModel):
    category: str
    name: str
    unit_cost: Any  # validated by core.finance_money.parse_money (Decimal exactness)


class BudgetItemUpdate(BaseModel):
    category: str
    name: str
    unit_cost: Any
    is_active: bool | None = None


class BudgetQuantityIn(BaseModel):
    item_id: str
    quantity: StrictInt
