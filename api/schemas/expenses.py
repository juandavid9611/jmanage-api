"""Expenses. Wire casing is snake_case (jmanage-web#175 / #191).

`status`, `paid_amount`, `account_id`, `requested_by` and `approved_*` are NOT writable: unknown
body fields (the frontend still sends `account_id` / `proof_images`) are ignored.
"""
from pydantic import BaseModel, Field, StrictFloat, StrictInt, StrictStr

from api.schemas.finance_constants import MAX_FILE_NAME, MAX_NAME, MAX_TEXT

Money = StrictInt | StrictFloat | StrictStr  # strict: JSON `true` must not become 1


class CreateExpense(BaseModel):
    team: str
    category: str
    subcategory: str = Field(default="", max_length=MAX_NAME)
    detail: str = Field(min_length=1, max_length=500)
    beneficiary_name: str = Field(default="", max_length=MAX_NAME)
    expense_date: str  # YYYY-MM-DD
    total_amount: Money
    budget_item_id: str | None = None
    budget_quantity: StrictInt | None = None


class UpdateExpense(BaseModel):
    """Partial update: only the fields present in the body change."""

    team: str | None = None
    category: str | None = None
    subcategory: str | None = Field(default=None, max_length=MAX_NAME)
    detail: str | None = Field(default=None, min_length=1, max_length=500)
    beneficiary_name: str | None = Field(default=None, max_length=MAX_NAME)
    expense_date: str | None = None
    total_amount: Money | None = None
    budget_item_id: str | None = None
    budget_quantity: StrictInt | None = None


class RejectExpense(BaseModel):
    reason: str = Field(min_length=1, max_length=MAX_TEXT)


class MarkPaid(BaseModel):
    amount: Money | None = None
    account_id: str  # the BANK account the money leaves from
    paid_at: str  # YYYY-MM-DD
    file_names: list[str] = Field(default_factory=list, max_length=10)
    idempotency_key: str | None = None


class ExpenseFileSpec(BaseModel):
    file_name: str = Field(max_length=MAX_FILE_NAME)
    content_type: str
