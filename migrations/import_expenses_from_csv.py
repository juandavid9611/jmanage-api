#!/usr/bin/env python3
"""Import the expense history (Excel exported to CSV) as Expense + ExpensePayment items.

Dry run by default: parses and validates every row and prints what WOULD be written.
`--confirm` writes. Table names come from the environment (EXPENSE_TABLE_NAME,
EXPENSE_PAYMENT_TABLE_NAME, BANK_ACCOUNT_TABLE_NAME); anything that does not look like a dev/test
table is refused unless `--allow-prod` is also given.

CSV columns (header required, UTF-8):
    expense_date (YYYY-MM-DD), team, category, subcategory, detail, beneficiary_name,
    total_amount, paid_amount, paid_at (YYYY-MM-DD, default expense_date), bank_account_id (optional)

Rules
    * an expense is imported as `pagado` when paid_amount >= total_amount, otherwise `aprobado`
      (history is already decided; nothing goes through the approval flow);
    * paid_amount > 0 creates ONE ExpensePayment (source='import') from `bank_account_id`
      (row value, else --bank-account-id); paid_amount > total_amount is an error;
    * ids are deterministic (hash of the row), so re-running the same file skips what exists
      instead of duplicating it. A row repeated N times in the file yields N distinct expenses;
    * each row is written in ONE transaction (Expense + its payment).

Usage
    python migrations/import_expenses_from_csv.py history.csv --account-id acc_x --workspace-id ws_y \\
        --bank-account-id bac_z                 # dry run
    python migrations/import_expenses_from_csv.py history.csv ... --confirm
"""
import argparse
import csv
import hashlib
import os
import sys
from collections import Counter
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from api.schemas.finance_constants import EXPENSE_CATEGORIES, EXPENSE_TEAMS  # noqa: E402
from core.finance_money import MoneyError, parse_money  # noqa: E402

SAFE_TABLE_HINTS = ("dev", "test", "local", "staging")
REQUIRED_COLUMNS = ("expense_date", "team", "category", "detail", "total_amount")


class RowError(ValueError):
    pass


def _iso(value: str, field: str) -> str:
    try:
        return datetime.strptime(value.strip(), "%Y-%m-%d").date().isoformat()
    except ValueError:
        raise RowError(f"{field} must be YYYY-MM-DD (got {value!r})") from None


def _money(value: str, field: str, *, allow_zero=False) -> Decimal:
    try:
        return parse_money((value or "0").strip().replace(",", ""), allow_zero=allow_zero)
    except MoneyError as exc:
        raise RowError(f"{field}: {exc}") from None


def row_id(account_id: str, workspace_id: str, row: dict[str, str], occurrence: int) -> str:
    canonical = "|".join([account_id, workspace_id, str(occurrence)] + [(row.get(c) or "").strip() for c in sorted(row)])
    return "exp_imp_" + hashlib.sha1(canonical.encode()).hexdigest()[:24]


def build_items(row: dict[str, str], *, account_id: str, workspace_id: str, default_bank: str | None,
                currency: str, user: str, expense_id: str, now: str) -> tuple[dict[str, Any], dict[str, Any] | None]:
    team, category = (row.get("team") or "").strip(), (row.get("category") or "").strip()
    if team not in EXPENSE_TEAMS:
        raise RowError(f"team must be one of {EXPENSE_TEAMS} (got {team!r})")
    if category not in EXPENSE_CATEGORIES:
        raise RowError(f"category must be one of {EXPENSE_CATEGORIES} (got {category!r})")
    detail = (row.get("detail") or "").strip()
    if not detail:
        raise RowError("detail is required")
    expense_date = _iso(row.get("expense_date") or "", "expense_date")
    total = _money(row.get("total_amount") or "", "total_amount")
    paid = _money(row.get("paid_amount") or "0", "paid_amount", allow_zero=True)
    if paid > total:
        raise RowError("paid_amount cannot exceed total_amount")
    bank = (row.get("bank_account_id") or "").strip() or default_bank
    if paid > 0 and not bank:
        raise RowError("paid rows need bank_account_id (column or --bank-account-id)")
    paid_at = _iso(row.get("paid_at") or expense_date, "paid_at")

    expense = {
        "id": expense_id, "account_id": account_id, "workspace_id": workspace_id, "team": team,
        "category": category, "subcategory": (row.get("subcategory") or "").strip(), "detail": detail,
        "beneficiary_name": (row.get("beneficiary_name") or "").strip(), "expense_date": expense_date,
        "total_amount": total, "paid_amount": paid, "currency": currency,
        "status": "pagado" if paid >= total else "aprobado",
        "bank_account_id": bank if paid > 0 else None, "budget_item_id": None, "budget_quantity": None,
        "requested_by": user, "approved_by": user, "approved_at": now, "source": "import",
        "created_at": now, "updated_at": now, "updated_by": user,
    }
    payment = None
    if paid > 0:
        payment = {
            "id": f"epy_{expense_id}_import", "account_id": account_id, "workspace_id": workspace_id,
            "expense_id": expense_id, "bank_account_id": bank, "amount": paid, "currency": currency,
            "paid_at": paid_at, "file_names": [], "file_keys": [], "idempotency_key": "import",
            "source": "import", "created_by": user, "created_at": now,
        }
    return expense, payment


def check_tables(allow_prod: bool) -> list[str]:
    names = {
        "EXPENSE_TABLE_NAME": os.getenv("EXPENSE_TABLE_NAME"),
        "EXPENSE_PAYMENT_TABLE_NAME": os.getenv("EXPENSE_PAYMENT_TABLE_NAME"),
        "BANK_ACCOUNT_TABLE_NAME": os.getenv("BANK_ACCOUNT_TABLE_NAME"),
    }
    missing = [k for k, v in names.items() if not v]
    if missing:
        raise SystemExit(f"Missing environment variables: {', '.join(missing)}")
    if not allow_prod:
        unsafe = [v for v in names.values() if not any(h in v.lower() for h in SAFE_TABLE_HINTS)]
        if unsafe:
            raise SystemExit(f"Refusing to run against non-dev tables {unsafe}; pass --allow-prod to override.")
    return list(names.values())


def load_rows(path: str) -> list[dict[str, str]]:
    with open(path, newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        missing = [c for c in REQUIRED_COLUMNS if c not in (reader.fieldnames or [])]
        if missing:
            raise SystemExit(f"CSV is missing columns: {', '.join(missing)}")
        return list(reader)


def run(args) -> dict[str, int]:
    from repositories import ddb_session
    from repositories.ddb_transactions import TransactionConflict, transact_write, tx_put

    check_tables(args.allow_prod)
    expense_table, payment_table = ddb_session.expense_table(), ddb_session.expense_payment_table()
    if args.bank_account_id:
        bank = ddb_session.bank_account_table().get_item(Key={"id": args.bank_account_id}).get("Item")
        if not bank or bank.get("account_id") != args.account_id:
            raise SystemExit(f"bank account {args.bank_account_id} not found for account {args.account_id}")

    now = datetime.now(timezone.utc).isoformat()
    seen: Counter = Counter()
    stats = {"rows": 0, "errors": 0, "written": 0, "skipped_existing": 0}
    for line, row in enumerate(load_rows(args.csv), start=2):
        stats["rows"] += 1
        fingerprint = tuple(sorted((k, (v or "").strip()) for k, v in row.items()))
        seen[fingerprint] += 1
        expense_id = row_id(args.account_id, args.workspace_id, row, seen[fingerprint])
        try:
            expense, payment = build_items(
                row, account_id=args.account_id, workspace_id=args.workspace_id, default_bank=args.bank_account_id,
                currency=args.currency, user=args.user, expense_id=expense_id, now=now)
        except RowError as exc:
            stats["errors"] += 1
            print(f"line {line}: ERROR {exc}")
            continue
        label = f"line {line}: {expense['expense_date']} {expense['category']} {expense['total_amount']} ({expense['status']})"
        if not args.confirm:
            print(f"[dry-run] would import {label}" + (f" + payment {payment['amount']}" if payment else ""))
            continue
        items = [tx_put(expense_table, expense, condition="attribute_not_exists(id)")]
        if payment:
            items.append(tx_put(payment_table, payment, condition="attribute_not_exists(id)"))
        try:
            transact_write(items)
            stats["written"] += 1
        except TransactionConflict:
            stats["skipped_existing"] += 1
            print(f"{label}: already imported, skipped")
    print(f"{'WROTE' if args.confirm else 'DRY RUN (nothing written)'}: {stats}")
    return stats


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("csv")
    parser.add_argument("--account-id", required=True)
    parser.add_argument("--workspace-id", required=True)
    parser.add_argument("--bank-account-id", help="default bank account for paid rows without bank_account_id")
    parser.add_argument("--currency", default="COP")
    parser.add_argument("--user", default="import", help="value for requested_by / approved_by / created_by")
    parser.add_argument("--confirm", action="store_true", help="write (default is a dry run)")
    parser.add_argument("--allow-prod", action="store_true", help="allow table names that do not look like dev/test")
    return parser.parse_args(argv)


def main(argv=None) -> int:
    stats = run(parse_args(argv))
    return 1 if stats["errors"] else 0


if __name__ == "__main__":
    sys.exit(main())
