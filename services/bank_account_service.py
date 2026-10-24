"""Bank accounts and manual adjustments. Balance is always derived, never stored:

    balance = opening_balance + sum(adjustments.amount) - sum(expense payments.amount)
"""
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any

from fastapi import HTTPException

from api.schemas.finance_constants import BANK_ACCOUNT_TYPES, ID_PREFIXES
from core.finance_money import MoneyError, new_id, parse_money, plain_number
from repositories.bank_account_repo_ddb import ConditionFailed


class FinanceError(HTTPException):
    """A finance rule violation carrying its HTTP status (404 / 409 / 422 / 403)."""

    def __init__(self, status_code: int, detail: str):
        super().__init__(status_code=status_code, detail=detail)


def money(value: Any, *, field: str = "amount", **kwargs) -> Decimal:
    try:
        return parse_money(value, **kwargs)
    except MoneyError as exc:
        raise FinanceError(422, f"{field}: {exc}") from None


def iso_date(value: Any, *, field: str) -> str:
    """Strict YYYY-MM-DD (the format the frontend sends)."""
    if not isinstance(value, str) or len(value) != 10:
        raise FinanceError(422, f"{field} must be an ISO date (YYYY-MM-DD)")
    try:
        return date.fromisoformat(value).isoformat()
    except ValueError:
        raise FinanceError(422, f"{field} must be an ISO date (YYYY-MM-DD)") from None


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def account_currency(account_svc, account_id: str) -> str:
    account = account_svc.get(account_id) or {}
    return ((account.get("settings") or {}).get("currency")) or "COP"


def _num(item: dict[str, Any], *keys: str) -> dict[str, Any]:
    out = dict(item)
    for key in keys:
        if key in out:
            out[key] = plain_number(out[key])
    return out


class BankAccountService:
    def __init__(self, accounts, adjustments, payments, account_svc):
        self.accounts = accounts
        self.adjustments = adjustments
        self.payments = payments
        self.account_svc = account_svc

    # ------------------------------------------------------------------ wire
    @staticmethod
    def _wire(item: dict[str, Any], balance: Decimal | None = None) -> dict[str, Any]:
        out = _num(item, "opening_balance")
        out.pop("account_id", None)  # tenant column, not part of the contract
        if balance is not None:
            out["balance"] = plain_number(balance)
        return out

    @staticmethod
    def _wire_adjustment(item: dict[str, Any]) -> dict[str, Any]:
        out = _num(item, "amount")
        out.pop("account_id", None)
        return out

    def balance_of(self, bank: dict[str, Any], account_id: str) -> Decimal:
        total = Decimal(bank.get("opening_balance", 0))
        for adj in self.adjustments.list_by_bank_account(bank["id"], account_id):
            total += Decimal(adj["amount"])
        for pay in self.payments.list_by_bank_account(bank["id"], account_id):
            total -= Decimal(pay["amount"])
        return total

    # ------------------------------------------------------------------ reads
    def _get_or_404(self, bank_account_id: str, workspace_id: str, account_id: str) -> dict[str, Any]:
        item = self.accounts.get(bank_account_id, account_id)
        if not item or item.get("workspace_id") != workspace_id:
            raise FinanceError(404, "Bank account not found")
        return item

    def get_raw(self, bank_account_id: str, workspace_id: str, account_id: str) -> dict[str, Any]:
        return self._get_or_404(bank_account_id, workspace_id, account_id)

    def list_accounts(self, workspace_id: str, account_id: str) -> list[dict[str, Any]]:
        items = [i for i in self.accounts.list_by_account(account_id) if i.get("workspace_id") == workspace_id]
        items.sort(key=lambda i: (i.get("name", "").lower(), i["id"]))
        return [self._wire(i, self.balance_of(i, account_id)) for i in items]

    def get(self, bank_account_id: str, workspace_id: str, account_id: str) -> dict[str, Any]:
        item = self._get_or_404(bank_account_id, workspace_id, account_id)
        return self._wire(item, self.balance_of(item, account_id))

    # ------------------------------------------------------------------ writes
    def create(self, body, workspace_id: str, account_id: str, user_id: str) -> dict[str, Any]:
        if body.type not in BANK_ACCOUNT_TYPES:
            raise FinanceError(422, f"type must be one of {', '.join(BANK_ACCOUNT_TYPES)}")
        now = now_iso()
        item = {
            "id": new_id(ID_PREFIXES["bank_account"]),
            "account_id": account_id,
            "workspace_id": workspace_id,
            "name": body.name.strip(),
            "type": body.type,
            "opening_balance": money(body.opening_balance, field="opening_balance", allow_zero=True, allow_negative=True),
            "opening_balance_date": iso_date(body.opening_balance_date, field="opening_balance_date"),
            "currency": account_currency(self.account_svc, account_id),
            "is_active": True,
            "created_at": now,
            "created_by": user_id,
            "updated_at": now,
            "updated_by": user_id,
        }
        if not item["name"]:
            raise FinanceError(422, "name cannot be blank")
        self.accounts.create(item)
        return self._wire(item, item["opening_balance"])

    def update(self, bank_account_id: str, body, workspace_id: str, account_id: str, user_id: str) -> dict[str, Any]:
        self._get_or_404(bank_account_id, workspace_id, account_id)
        sets: dict[str, Any] = {}
        fields = body.model_fields_set
        if "name" in fields and body.name is not None:
            if not body.name.strip():
                raise FinanceError(422, "name cannot be blank")
            sets["name"] = body.name.strip()
        if "type" in fields and body.type is not None:
            if body.type not in BANK_ACCOUNT_TYPES:
                raise FinanceError(422, f"type must be one of {', '.join(BANK_ACCOUNT_TYPES)}")
            sets["type"] = body.type
        if "opening_balance" in fields and body.opening_balance is not None:
            sets["opening_balance"] = money(
                body.opening_balance, field="opening_balance", allow_zero=True, allow_negative=True
            )
        if "opening_balance_date" in fields and body.opening_balance_date is not None:
            sets["opening_balance_date"] = iso_date(body.opening_balance_date, field="opening_balance_date")
        if "is_active" in fields and body.is_active is not None:
            sets["is_active"] = body.is_active
        sets["updated_at"] = now_iso()
        sets["updated_by"] = user_id
        updated = self._update(bank_account_id, workspace_id, account_id, sets)
        return self._wire(updated, self.balance_of(updated, account_id))

    def _update(self, bank_account_id, workspace_id, account_id, sets):
        try:
            return self.accounts.update(bank_account_id, account_id, workspace_id, sets)
        except ConditionFailed:
            raise FinanceError(404, "Bank account not found") from None

    def deactivate(self, bank_account_id: str, workspace_id: str, account_id: str, user_id: str) -> dict[str, Any]:
        """Soft delete. Already inactive = 200 no-op."""
        current = self._get_or_404(bank_account_id, workspace_id, account_id)
        if current.get("is_active") is False:
            return self._wire(current, self.balance_of(current, account_id))
        updated = self._update(
            bank_account_id, workspace_id, account_id,
            {"is_active": False, "updated_at": now_iso(), "updated_by": user_id},
        )
        return self._wire(updated, self.balance_of(updated, account_id))

    # ------------------------------------------------------------------ adjustments
    def list_adjustments(self, bank_account_id: str, workspace_id: str, account_id: str) -> list[dict[str, Any]]:
        self._get_or_404(bank_account_id, workspace_id, account_id)
        items = self.adjustments.list_by_bank_account(bank_account_id, account_id)
        items.sort(key=lambda i: (i.get("date", ""), i.get("created_at", ""), i["id"]), reverse=True)
        return [self._wire_adjustment(i) for i in items]

    def create_adjustment(self, bank_account_id: str, body, workspace_id: str, account_id: str, user_id: str) -> dict[str, Any]:
        bank = self._get_or_404(bank_account_id, workspace_id, account_id)
        if bank.get("is_active") is False:
            raise FinanceError(409, "Bank account is inactive")
        item = {
            "id": new_id(ID_PREFIXES["adjustment"]),
            "account_id": account_id,
            "workspace_id": workspace_id,
            "bank_account_id": bank_account_id,
            "amount": money(body.amount, allow_negative=True),
            "date": iso_date(body.date, field="date"),
            "note": body.note.strip(),
            "source": "manual",  # clients can only create manual adjustments
            "payment_request_id": None,
            "submission_id": None,
            "currency": bank.get("currency", "COP"),
            "created_by": user_id,
            "created_at": now_iso(),
        }
        self.adjustments.create(item)
        return self._wire_adjustment(item)
