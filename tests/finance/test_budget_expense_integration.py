"""Cross-module check: an expense validates its budget item through the REAL budget repository."""
import pytest
from fastapi import HTTPException

from repositories.budget_item_repo_ddb import BudgetItemRepo
from repositories.budget_quantity_repo_ddb import BudgetQuantityRepo
from services.budget_service import BudgetService

from .test_expenses_support import ACC, OTHER_WS, WS, env, make_expense  # noqa: F401


def test_expense_accepts_a_real_budget_item_and_rejects_foreign_or_unknown_ones(env):  # noqa: F811
    env.expenses.budget_items = BudgetItemRepo()  # the real thing, not the test fake
    budget = BudgetService(BudgetItemRepo(), BudgetQuantityRepo())

    item = budget.create_item(ACC, WS, "Cancha", "Cancha Servita", 525000)
    other_ws_item = budget.create_item(ACC, OTHER_WS, "Cancha", "Cancha Norte", 400000)
    other_account_item = budget.create_item("acc2", WS, "Cancha", "Cancha X", 100000)

    ok = make_expense(env, budget_item_id=item["id"], budget_quantity=2)
    assert ok["budget_item_id"] == item["id"] and ok["budget_quantity"] == 2

    for bad_id in ("bgi_unknown", other_ws_item["id"], other_account_item["id"]):
        with pytest.raises(HTTPException) as exc:
            make_expense(env, budget_item_id=bad_id, budget_quantity=1)
        assert exc.value.status_code == 422, bad_id

    # a soft-deleted catalog item still exists, so history that points at it stays valid
    budget.delete_item(ACC, WS, item["id"])
    still = make_expense(env, budget_item_id=item["id"], budget_quantity=1)
    assert still["budget_item_id"] == item["id"]
