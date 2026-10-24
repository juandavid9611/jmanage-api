import re
from datetime import datetime, timezone
from typing import Any

from api.schemas.finance_constants import EXPENSE_CATEGORIES, ID_PREFIXES, MAX_NAME
from core.finance_money import MoneyError, new_id, parse_money, plain_number
from repositories.budget_item_repo_ddb import BudgetItemRepo
from repositories.budget_quantity_repo_ddb import BudgetQuantityRepo, quantity_id

MONTH_RE = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")
MAX_QUANTITY = 100000


class BudgetNotFound(Exception):
    pass


class BudgetConflict(Exception):
    pass


class BudgetValidationError(Exception):
    pass


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _item_out(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": item["id"],
        "workspace_id": item["workspace_id"],
        "category": item["category"],
        "name": item["name"],
        "unit_cost": plain_number(item["unit_cost"]),
        "is_active": bool(item.get("is_active", True)),
        "created_at": item.get("created_at"),
        "updated_at": item.get("updated_at"),
    }


def _name_key(name: str) -> str:
    return " ".join(name.split()).casefold()


def validate_month(month: str) -> str:
    if not isinstance(month, str) or not MONTH_RE.match(month):
        raise BudgetValidationError("month must be YYYY-MM")
    return month


class BudgetService:
    def __init__(self, item_repo: BudgetItemRepo, quantity_repo: BudgetQuantityRepo):
        self._items = item_repo
        self._quantities = quantity_repo

    # ------------------------------------------------------------------ items
    def list_items(self, account_id: str, workspace_id: str) -> list[dict[str, Any]]:
        items = self._items.list_by_workspace(account_id, workspace_id)
        items.sort(key=lambda i: (i["category"], _name_key(i["name"]), i["id"]))
        return [_item_out(i) for i in items]

    def _clean(self, category: Any, name: Any, unit_cost: Any) -> tuple[str, str, Any]:
        if category not in EXPENSE_CATEGORIES:
            raise BudgetValidationError(f"category must be one of: {', '.join(EXPENSE_CATEGORIES)}")
        name = " ".join(name.split()) if isinstance(name, str) else ""
        if not name or len(name) > MAX_NAME:
            raise BudgetValidationError(f"name is required and at most {MAX_NAME} characters")
        try:
            cost = parse_money(unit_cost)
        except MoneyError as exc:
            raise BudgetValidationError(f"unit_cost: {exc}") from None
        return category, name, cost

    def _ensure_unique(self, account_id: str, workspace_id: str, category: str, name: str, exclude_id: str | None):
        key = _name_key(name)
        for other in self._items.list_by_workspace(account_id, workspace_id):
            if (
                other["id"] != exclude_id
                and other.get("is_active", True)
                and other["category"] == category
                and _name_key(other["name"]) == key
            ):
                raise BudgetConflict("An active budget item with this name already exists in this category")

    def create_item(self, account_id: str, workspace_id: str, category: Any, name: Any, unit_cost: Any) -> dict[str, Any]:
        category, name, cost = self._clean(category, name, unit_cost)
        self._ensure_unique(account_id, workspace_id, category, name, None)
        now = _now()
        item = {
            "id": new_id(ID_PREFIXES["budget_item"]),
            "account_id": account_id,
            "workspace_id": workspace_id,
            "category": category,
            "name": name,
            "unit_cost": cost,
            "is_active": True,
            "created_at": now,
            "updated_at": now,
        }
        self._items.create(item)
        return _item_out(item)

    def update_item(self, account_id: str, workspace_id: str, item_id: str, category: Any, name: Any,
                    unit_cost: Any, is_active: bool | None) -> dict[str, Any]:
        current = self._items.get_for_workspace(item_id, account_id, workspace_id)
        if not current:
            raise BudgetNotFound("Budget item not found")
        category, name, cost = self._clean(category, name, unit_cost)
        active = current.get("is_active", True) if is_active is None else is_active
        if active:
            self._ensure_unique(account_id, workspace_id, category, name, item_id)
        updated = {**current, "category": category, "name": name, "unit_cost": cost,
                   "is_active": active, "updated_at": _now()}
        self._items.replace(updated)
        return _item_out(updated)

    def delete_item(self, account_id: str, workspace_id: str, item_id: str) -> dict[str, Any]:
        """Soft delete (idempotent). Quantities of past months stay readable."""
        current = self._items.get_for_workspace(item_id, account_id, workspace_id)
        if not current:
            raise BudgetNotFound("Budget item not found")
        if not current.get("is_active", True):
            return _item_out(current)
        updated = {**current, "is_active": False, "updated_at": _now()}
        self._items.replace(updated)
        return _item_out(updated)

    # ------------------------------------------------------------- quantities
    def list_quantities(self, account_id: str, workspace_id: str, month: str) -> list[dict[str, Any]]:
        validate_month(month)
        rows = self._quantities.list_for_month(account_id, workspace_id, month)
        rows.sort(key=lambda r: r["item_id"])
        return [{"item_id": r["item_id"], "quantity": int(r["quantity"])} for r in rows]

    def set_quantities(self, account_id: str, workspace_id: str, month: str,
                       entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Upsert the listed items for the month; unlisted ones are untouched; quantity 0 deletes.

        Everything is validated before the first write.
        """
        validate_month(month)
        seen: set[str] = set()
        duplicates: list[str] = []
        for entry in entries:
            item_id = entry["item_id"]
            quantity = entry["quantity"]
            if isinstance(quantity, bool) or not isinstance(quantity, int) or not 0 <= quantity <= MAX_QUANTITY:
                raise BudgetValidationError(f"quantity must be an integer between 0 and {MAX_QUANTITY}")
            if item_id in seen and item_id not in duplicates:
                duplicates.append(item_id)
            seen.add(item_id)
        if duplicates:
            raise BudgetValidationError(f"Duplicate item_id in body: {', '.join(duplicates)}")

        known = {i["id"] for i in self._items.list_by_workspace(account_id, workspace_id)}
        unknown = sorted(seen - known)
        if unknown:
            raise BudgetValidationError(f"Unknown item_id for this workspace: {', '.join(unknown)}")

        puts, deletes = [], []
        for entry in entries:
            row_id = quantity_id(entry["item_id"], month)
            if entry["quantity"] == 0:
                deletes.append(row_id)
            else:
                puts.append({
                    "id": row_id,
                    "account_id": account_id,
                    "workspace_id": workspace_id,
                    "item_id": entry["item_id"],
                    "month": month,
                    "quantity": entry["quantity"],
                })
        self._quantities.apply(puts, deletes)
        return self.list_quantities(account_id, workspace_id, month)
