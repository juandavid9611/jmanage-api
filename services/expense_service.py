"""Expenses (egresos): pendiente -> aprobado -> pagado, pendiente|aprobado -> rechazado.

Money moves ONLY in `mark_paid`: one DynamoDB transaction puts the ExpensePayment (id derived from
the idempotency key, so a replay cannot duplicate it) and bumps `Expense.paid_amount` under a
condition (status approved, not over-paid, total unchanged). `paid_amount` is a stored counter
maintained only there.

Storage vs wire: the tenant column is `account_id` (like every table); the BANK account of the
latest payment is stored as `bank_account_id` and exposed on the wire as `account_id`, which is
what the frontend contract (jmanage-web#177) expects.
"""
import logging
import mimetypes
import uuid
from datetime import date, timedelta, timezone, datetime
from decimal import Decimal
from typing import Any

from api.schemas.finance_constants import EXPENSE_CATEGORIES, EXPENSE_STATUSES, EXPENSE_TEAMS, ID_PREFIXES
from core.finance_money import new_id, plain_number, valid_file_name, valid_idempotency_key
from repositories.bank_account_repo_ddb import ConditionFailed
from repositories.ddb_transactions import TransactionConflict, transact_write
from services.bank_account_service import FinanceError, account_currency, iso_date, money, now_iso
from services.finance_access import require_account_workspace_access

logger = logging.getLogger(__name__)

ALLOWED_CONTENT_TYPES = ("image/jpeg", "image/png", "image/webp", "application/pdf")
MAX_FILES = 10
PROOF_URL_TTL = 900  # seconds; proofs are private, URLs are short-lived
UPLOADS_SEGMENT = "uploads"  # S3 segment when the client sends no idempotency key (payment id unknown)
_REAPPROVAL_FIELDS = ("total_amount", "beneficiary_name", "category", "budget_item_id", "budget_quantity")


def _plain(item: dict[str, Any], *keys: str) -> dict[str, Any]:
    out = dict(item)
    for key in keys:
        if key in out:
            out[key] = plain_number(out[key])
    return out


class ExpenseService:
    def __init__(self, expenses, payments, bank_accounts, budget_items, membership, account_svc, s3, keys):
        self.expenses = expenses
        self.payments = payments
        self.bank_accounts = bank_accounts
        self.budget_items = budget_items  # .get(item_id, account_id) -> dict | None
        self.membership = membership
        self.account_svc = account_svc
        self.s3 = s3
        self.keys = keys

    # ------------------------------------------------------------------ wire
    @staticmethod
    def _wire(item: dict[str, Any]) -> dict[str, Any]:
        out = _plain(item, "total_amount", "paid_amount", "budget_quantity")
        out.pop("account_id", None)  # tenant
        out["account_id"] = out.pop("bank_account_id", None)
        out.setdefault("proof_images", [])
        return out

    @staticmethod
    def _wire_payment(item: dict[str, Any]) -> dict[str, Any]:
        out = _plain(item, "amount")
        out.pop("account_id", None)  # tenant
        out["account_id"] = out.pop("bank_account_id", None)
        out.pop("file_keys", None)  # raw keys never leave the API
        return out

    def _proof_urls(self, payments: list[dict[str, Any]]) -> list[str]:
        urls = []
        for pay in payments:
            for key in pay.get("file_keys", []):
                content_type = mimetypes.guess_type(key)[0] or "application/octet-stream"
                urls.append(self.s3.presign_get_from_explicit_key(
                    key=key, content_type=content_type, expires_in=PROOF_URL_TTL))
        return urls

    def _detail(self, item: dict[str, Any], account_id: str) -> dict[str, Any]:
        payments = sorted(
            self.payments.list_by_expense(item["id"], account_id),
            key=lambda p: (p.get("paid_at", ""), p.get("created_at", ""), p["id"]),
        )
        out = self._wire(item)
        out["payments"] = [self._wire_payment(p) for p in payments]
        out["proof_images"] = self._proof_urls(payments)
        return out

    # ------------------------------------------------------------------ helpers
    def _get_or_404(self, expense_id: str, workspace_id: str, account_id: str) -> dict[str, Any]:
        item = self.expenses.get(expense_id, account_id)
        if not item or item.get("workspace_id") != workspace_id:
            raise FinanceError(404, "Expense not found")
        return item

    @staticmethod
    def _check_enums(team: str | None, category: str | None) -> None:
        if team is not None and team not in EXPENSE_TEAMS:
            raise FinanceError(422, f"team must be one of {', '.join(EXPENSE_TEAMS)}")
        if category is not None and category not in EXPENSE_CATEGORIES:
            raise FinanceError(422, f"category must be one of {', '.join(EXPENSE_CATEGORIES)}")

    def _check_budget(self, item_id: str | None, quantity: int | None, workspace_id: str, account_id: str) -> None:
        if item_id is None:
            if quantity is not None:
                raise FinanceError(422, "budget_quantity requires a budget_item_id")
            return
        found = self.budget_items.get(item_id, account_id)
        if not found or found.get("workspace_id") != workspace_id:
            raise FinanceError(422, "budget_item_id does not exist in this workspace")
        if quantity is not None and quantity < 1:
            raise FinanceError(422, "budget_quantity must be at least 1")

    def _admin_ids(self, workspace_id: str, account_id: str) -> set[str]:
        return {
            m["user_id"]
            for m in self.membership.list_workspace_memberships(workspace_id)
            if m.get("role") == "admin" and m.get("status", "active") == "active"
            and m.get("account_id") == account_id
        }

    # ------------------------------------------------------------------ reads
    def list_expenses(self, workspace_id: str, account_id: str, *, status=None, bank_account_id=None, team=None,
             category=None, date_from=None, date_to=None) -> list[dict[str, Any]]:
        if status is not None and status not in EXPENSE_STATUSES:
            raise FinanceError(422, f"status must be one of {', '.join(EXPENSE_STATUSES)}")
        if team is not None and team not in EXPENSE_TEAMS:
            raise FinanceError(422, f"team must be one of {', '.join(EXPENSE_TEAMS)}")
        date_from = iso_date(date_from, field="date_from") if date_from else None
        date_to = iso_date(date_to, field="date_to") if date_to else None
        items = self.expenses.list_filtered(
            account_id, workspace_id, status=status, bank_account_id=bank_account_id, team=team,
            category=category, date_from=date_from, date_to=date_to,
        )
        return [self._wire(i) for i in items]

    def get(self, expense_id: str, workspace_id: str, account_id: str) -> dict[str, Any]:
        return self._detail(self._get_or_404(expense_id, workspace_id, account_id), account_id)

    # ------------------------------------------------------------------ create / update / delete
    def create(self, body, workspace_id: str, account_id: str, user_id: str) -> dict[str, Any]:
        self._check_enums(body.team, body.category)
        self._check_budget(body.budget_item_id, body.budget_quantity, workspace_id, account_id)
        now = now_iso()
        item = {
            "id": new_id(ID_PREFIXES["expense"]),
            "account_id": account_id,
            "workspace_id": workspace_id,
            "team": body.team,
            "category": body.category,
            "subcategory": body.subcategory.strip(),
            "detail": body.detail.strip(),
            "beneficiary_name": body.beneficiary_name.strip(),
            "expense_date": iso_date(body.expense_date, field="expense_date"),
            "total_amount": money(body.total_amount, field="total_amount"),
            "paid_amount": Decimal("0"),
            "currency": account_currency(self.account_svc, account_id),
            "status": "pendiente",
            "bank_account_id": None,
            "budget_item_id": body.budget_item_id,
            "budget_quantity": body.budget_quantity,
            "requested_by": user_id,
            "source": "api",
            "created_at": now,
            "updated_at": now,
            "updated_by": user_id,
        }
        if not item["detail"]:
            raise FinanceError(422, "detail cannot be blank")
        self.expenses.create(item)
        return self._detail(item, account_id)

    def update(self, expense_id: str, body, workspace_id: str, account_id: str, user_id: str) -> dict[str, Any]:
        current = self._get_or_404(expense_id, workspace_id, account_id)
        if Decimal(current.get("paid_amount", 0)) > 0 or current["status"] == "pagado":
            raise FinanceError(409, "Expense already has payments and can no longer be edited")
        if current["status"] == "rechazado":
            raise FinanceError(409, "A rejected expense cannot be edited")
        fields = body.model_fields_set
        self._check_enums(body.team if "team" in fields else None, body.category if "category" in fields else None)

        new: dict[str, Any] = {}
        for name in ("team", "category", "subcategory", "detail", "beneficiary_name"):
            if name in fields and getattr(body, name) is not None:
                new[name] = getattr(body, name).strip()
        if new.get("detail") == "" and "detail" in new:
            raise FinanceError(422, "detail cannot be blank")
        if "expense_date" in fields and body.expense_date is not None:
            new["expense_date"] = iso_date(body.expense_date, field="expense_date")
        if "total_amount" in fields and body.total_amount is not None:
            new["total_amount"] = money(body.total_amount, field="total_amount")

        final_item = body.budget_item_id if "budget_item_id" in fields else current.get("budget_item_id")
        if "budget_quantity" in fields:
            final_qty = body.budget_quantity
        elif "budget_item_id" in fields and body.budget_item_id is None:
            final_qty = None  # clearing the item clears its quantity
        else:
            final_qty = current.get("budget_quantity")
        if final_qty is not None:
            final_qty = int(final_qty)
        self._check_budget(final_item, final_qty, workspace_id, account_id)
        if final_item != current.get("budget_item_id"):
            new["budget_item_id"] = final_item
        if final_qty != (int(current["budget_quantity"]) if current.get("budget_quantity") is not None else None):
            new["budget_quantity"] = final_qty

        changed = {k: v for k, v in new.items() if current.get(k) != v}
        sets = dict(changed)
        removes: tuple[str, ...] = ()
        if current["status"] == "aprobado" and any(k in changed for k in _REAPPROVAL_FIELDS):
            # an approved expense cannot be silently rewritten: it needs approval again
            sets["status"] = "pendiente"
            removes = ("approved_by", "approved_at")
        sets["updated_at"] = now_iso()
        sets["updated_by"] = user_id
        try:
            updated = self.expenses.update_guarded(
                expense_id, account_id, workspace_id, sets=sets, removes=removes,
                extra_condition="#st = :seen AND paid_amount = :zero",
                cond_names={"#st": "status"}, cond_values={":seen": current["status"], ":zero": 0},
            )
        except ConditionFailed:
            raise FinanceError(409, "Expense changed while editing; reload and retry") from None
        return self._detail(updated, account_id)

    def delete(self, expense_id: str, workspace_id: str, account_id: str) -> dict[str, Any]:
        current = self._get_or_404(expense_id, workspace_id, account_id)
        if current["status"] != "pendiente":
            raise FinanceError(409, "Only pending expenses can be deleted")
        try:
            self.expenses.delete_pending(expense_id, account_id, workspace_id)
        except ConditionFailed:
            raise FinanceError(409, "Only pending expenses can be deleted") from None
        return {"deleted_expense_id": expense_id}

    # ------------------------------------------------------------------ approval flow
    def approve(self, expense_id: str, workspace_id: str, account_id: str, user_id: str) -> dict[str, Any]:
        current = self._get_or_404(expense_id, workspace_id, account_id)
        if current["status"] != "pendiente":
            raise FinanceError(409, "Only pending expenses can be approved")
        if current.get("requested_by") == user_id and len(self._admin_ids(workspace_id, account_id)) > 1:
            raise FinanceError(409, "Needs another admin: the requester cannot approve their own expense")
        now = now_iso()
        try:
            updated = self.expenses.update_guarded(
                expense_id, account_id, workspace_id,
                sets={"status": "aprobado", "approved_by": user_id, "approved_at": now,
                      "updated_at": now, "updated_by": user_id},
                extra_condition="#st = :pendiente", cond_names={"#st": "status"},
                cond_values={":pendiente": "pendiente"},
            )
        except ConditionFailed:
            raise FinanceError(409, "Only pending expenses can be approved") from None
        return self._detail(updated, account_id)

    def reject(self, expense_id: str, reason: str, workspace_id: str, account_id: str, user_id: str) -> dict[str, Any]:
        current = self._get_or_404(expense_id, workspace_id, account_id)
        reason = (reason or "").strip()
        if not reason:
            raise FinanceError(422, "reason is required")
        if current["status"] not in ("pendiente", "aprobado") or Decimal(current.get("paid_amount", 0)) > 0:
            raise FinanceError(409, "Only pending or approved expenses without payments can be rejected")
        now = now_iso()
        try:
            updated = self.expenses.update_guarded(
                expense_id, account_id, workspace_id,
                sets={"status": "rechazado", "rejection_reason": reason, "rejected_by": user_id,
                      "rejected_at": now, "updated_at": now, "updated_by": user_id},
                extra_condition="#st IN (:pend, :apro) AND paid_amount = :zero", cond_names={"#st": "status"},
                cond_values={":pend": "pendiente", ":apro": "aprobado", ":zero": 0},
            )
        except ConditionFailed:
            raise FinanceError(409, "Only pending or approved expenses without payments can be rejected") from None
        return self._detail(updated, account_id)

    # ------------------------------------------------------------------ money
    def _replay(self, existing: dict[str, Any], amount: Decimal | None, bank_account_id: str) -> dict[str, Any]:
        if existing.get("bank_account_id") != bank_account_id or (
            amount is not None and Decimal(existing["amount"]) != amount
        ):
            raise FinanceError(409, "This idempotency_key was already used with a different amount or account")
        return self._wire_payment(existing)

    def _file_keys(self, account_id: str, expense_id: str, segment: str, file_names: list[str]) -> list[str]:
        for name in file_names:
            if not valid_file_name(name):
                raise FinanceError(422, f"invalid file name: {name!r}")
        return [self.keys.expense_file(account_id, expense_id, segment, name) for name in file_names]

    def mark_paid(self, expense_id: str, body, workspace_id: str, account_id: str, user_id: str) -> dict[str, Any]:
        client_key = body.idempotency_key
        if client_key is not None and not valid_idempotency_key(client_key):
            raise FinanceError(422, "idempotency_key must be 8-64 characters of [A-Za-z0-9_-]")
        key = client_key or uuid.uuid4().hex  # no key: no dedupe, the overpay condition still bounds the damage
        amount = money(body.amount, field="amount") if body.amount is not None else None
        expense = self._get_or_404(expense_id, workspace_id, account_id)
        payment_id = f"{ID_PREFIXES['expense_payment']}_{expense_id}_{key}"

        existing = self.payments.get(payment_id, account_id)
        if existing:
            return self._replay(existing, amount, body.account_id)

        if expense["status"] != "aprobado":
            raise FinanceError(409, "Expense must be approved before it can be paid")
        total, paid = Decimal(expense["total_amount"]), Decimal(expense.get("paid_amount", 0))
        remaining = total - paid
        if amount is None:
            amount = remaining
        if amount <= 0 or amount > remaining:
            raise FinanceError(422, f"amount must be greater than 0 and at most the remaining balance ({remaining})")

        paid_at = iso_date(body.paid_at, field="paid_at")
        if date.fromisoformat(paid_at) > datetime.now(timezone.utc).date() + timedelta(days=1):
            raise FinanceError(422, "paid_at cannot be in the future")

        bank = self.bank_accounts.get(body.account_id, account_id)
        if not bank:
            raise FinanceError(422, "Bank account not found")
        if bank.get("is_active") is False:
            raise FinanceError(422, "Bank account is inactive")
        if bank.get("currency", "COP") != expense.get("currency", "COP"):
            raise FinanceError(422, "Bank account currency differs from the expense currency")
        require_account_workspace_access(
            self.membership, user_id=user_id, account_id=account_id,
            request_workspace_id=workspace_id, bank_account=bank,
        )

        segment = payment_id if client_key else UPLOADS_SEGMENT
        file_keys = self._file_keys(account_id, expense_id, segment, body.file_names)
        now = now_iso()
        payment = {
            "id": payment_id,
            "account_id": account_id,
            "workspace_id": workspace_id,
            "expense_id": expense_id,
            "bank_account_id": bank["id"],
            "amount": amount,
            "currency": expense.get("currency", "COP"),
            "paid_at": paid_at,
            "file_names": list(body.file_names),
            "file_keys": file_keys,
            "idempotency_key": key,
            "source": "api",
            "created_by": user_id,
            "created_at": now,
        }
        new_status = "pagado" if paid + amount >= total else "aprobado"
        try:
            transact_write([
                self.payments.tx_create(payment),
                self.expenses.tx_register_payment(
                    expense, amount=amount, bank_account_id=bank["id"], new_status=new_status,
                    now=now, user_id=user_id,
                ),
            ])
        except TransactionConflict:
            return self._after_conflict(payment_id, expense_id, workspace_id, account_id, amount, body.account_id)
        return self._wire_payment(payment)

    def _after_conflict(self, payment_id, expense_id, workspace_id, account_id, amount, bank_account_id):
        existing = self.payments.get(payment_id, account_id)
        if existing:  # a concurrent submit of the same key won: same result, nothing duplicated
            return self._replay(existing, amount, bank_account_id)
        fresh = self.expenses.get(expense_id, account_id)
        if not fresh or fresh["status"] != "aprobado":
            raise FinanceError(409, "Expense must be approved before it can be paid")
        if amount > Decimal(fresh["total_amount"]) - Decimal(fresh.get("paid_amount", 0)):
            raise FinanceError(409, "amount exceeds the remaining balance (the expense was paid concurrently)")
        raise FinanceError(409, "Expense changed while paying; reload and retry")

    # ------------------------------------------------------------------ files
    def presign_uploads(self, expense_id: str, files: list, idempotency_key: str | None,
                        workspace_id: str, account_id: str) -> dict[str, dict[str, str]]:
        self._get_or_404(expense_id, workspace_id, account_id)
        if not files or len(files) > MAX_FILES:
            raise FinanceError(422, f"send between 1 and {MAX_FILES} files")
        if idempotency_key is not None and not valid_idempotency_key(idempotency_key):
            raise FinanceError(422, "idempotency_key must be 8-64 characters of [A-Za-z0-9_-]")
        segment = f"{ID_PREFIXES['expense_payment']}_{expense_id}_{idempotency_key}" if idempotency_key else UPLOADS_SEGMENT
        urls: dict[str, str] = {}
        for spec in files:
            if spec.content_type not in ALLOWED_CONTENT_TYPES:
                raise FinanceError(422, f"content_type must be one of {', '.join(ALLOWED_CONTENT_TYPES)}")
            key = self._file_keys(account_id, expense_id, segment, [spec.file_name])[0]
            urls[spec.file_name] = self.s3._presign_put(key=key, content_type=spec.content_type)["url"]
        return {"urls": urls}
