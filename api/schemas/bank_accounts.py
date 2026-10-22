"""Bank accounts and their adjustments. Wire casing is snake_case (jmanage-web#175 / #191).

Money fields are accepted as int/float/str and validated with `core.finance_money.parse_money`
in the service (exact Decimal, 2 decimals max), never as float arithmetic.
"""
from pydantic import BaseModel, Field, StrictFloat, StrictInt, StrictStr

from api.schemas.finance_constants import MAX_NAME, MAX_TEXT

Money = StrictInt | StrictFloat | StrictStr  # strict: JSON `true` must not become 1


class CreateBankAccount(BaseModel):
    name: str = Field(min_length=1, max_length=MAX_NAME)
    type: str
    opening_balance: Money = 0
    opening_balance_date: str  # YYYY-MM-DD


class UpdateBankAccount(BaseModel):
    name: str | None = Field(default=None, min_length=1, max_length=MAX_NAME)
    type: str | None = None
    opening_balance: Money | None = None
    opening_balance_date: str | None = None
    is_active: bool | None = None


class CreateAdjustment(BaseModel):
    amount: Money
    date: str  # YYYY-MM-DD
    note: str = Field(default="", max_length=MAX_TEXT)
