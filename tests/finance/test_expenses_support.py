"""Shared builders for the bank-account / expense tests (no tests in here)."""
from dataclasses import dataclass
from decimal import Decimal

import pytest

from api.schemas.bank_accounts import CreateAdjustment, CreateBankAccount
from api.schemas.expenses import CreateExpense, MarkPaid
from repositories.bank_account_repo_ddb import BankAccountRepo
from repositories.bank_adjustment_repo_ddb import BankAdjustmentRepo
from repositories.expense_payment_repo_ddb import ExpensePaymentRepo
from repositories.expense_repo_ddb import ExpenseRepo
from repositories.s3_adapter import S3Adapter
from repositories.s3_keys import KeyBuilder
from services.bank_account_service import BankAccountService
from services.expense_service import ExpenseService

ACC, WS, OTHER_WS = "acc1", "ws1", "ws2"


class FakeMembership:
    """roles: {(user_id, workspace_id): role}"""

    def __init__(self, roles):
        self.roles = roles

    def get_user_role_in_workspace(self, user_id, account_id, workspace_id):
        return self.roles.get((user_id, workspace_id))

    def list_workspace_memberships(self, workspace_id):
        return [
            {"user_id": u, "account_id": ACC, "workspace_id": w, "role": r, "status": "active"}
            for (u, w), r in self.roles.items() if w == workspace_id
        ]


class FakeAccountSvc:
    def __init__(self, currency="COP"):
        self.currency = currency

    def get(self, account_id):
        return {"id": account_id, "settings": {"account_type": "club", "currency": self.currency}}


class FakeBudget:
    def __init__(self):
        self.items = {}

    def get(self, item_id, account_id):
        item = self.items.get(item_id)
        return item if item and item["account_id"] == account_id else None


@dataclass
class Env:
    banks: BankAccountService
    expenses: ExpenseService
    membership: FakeMembership
    budget: FakeBudget
    bank_repo: BankAccountRepo
    expense_repo: ExpenseRepo
    payment_repo: ExpensePaymentRepo
    adjustment_repo: BankAdjustmentRepo


def build_env(roles=None) -> Env:
    membership = FakeMembership(roles or {("u1", WS): "admin", ("u2", WS): "admin", ("u1", OTHER_WS): "admin"})
    budget = FakeBudget()
    bank_repo, expense_repo = BankAccountRepo(), ExpenseRepo()
    payment_repo, adjustment_repo = ExpensePaymentRepo(), BankAdjustmentRepo()
    accounts = FakeAccountSvc()
    return Env(
        banks=BankAccountService(bank_repo, adjustment_repo, payment_repo, accounts),
        expenses=ExpenseService(expense_repo, payment_repo, bank_repo, budget, membership, accounts,
                                S3Adapter(), KeyBuilder(env="dev")),
        membership=membership, budget=budget, bank_repo=bank_repo, expense_repo=expense_repo,
        payment_repo=payment_repo, adjustment_repo=adjustment_repo,
    )


@pytest.fixture
def env(finance_db):
    return build_env()


def make_bank(env, *, ws=WS, acct=ACC, user="u1", opening=1000, name="Caja", type="cash"):
    return env.banks.create(
        CreateBankAccount(name=name, type=type, opening_balance=opening, opening_balance_date="2026-01-01"),
        ws, acct, user,
    )


def make_adjustment(env, bank_id, amount, *, ws=WS, acct=ACC, user="u1", date="2026-02-01", note=""):
    return env.banks.create_adjustment(bank_id, CreateAdjustment(amount=amount, date=date, note=note), ws, acct, user)


def make_expense(env, *, ws=WS, acct=ACC, user="u1", total=100000, **overrides):
    data = dict(team="general", category="Cancha", subcategory="", detail="Cancha mayo",
                beneficiary_name="Servita", expense_date="2026-05-10", total_amount=total)
    data.update(overrides)
    return env.expenses.create(CreateExpense(**data), ws, acct, user)


def approved_expense(env, *, approver="u2", **kwargs):
    exp = make_expense(env, **kwargs)
    env.expenses.approve(exp["id"], kwargs.get("ws", WS), kwargs.get("acct", ACC), approver)
    return exp


def pay(env, expense_id, bank_id, *, amount=None, key="key-00000001", paid_at="2026-05-11", files=(),
        ws=WS, acct=ACC, user="u1"):
    return env.expenses.mark_paid(
        expense_id,
        MarkPaid(amount=amount, account_id=bank_id, paid_at=paid_at, file_names=list(files), idempotency_key=key),
        ws, acct, user,
    )


def D(value):
    return Decimal(str(value))


# ------------------------------------------------------------------ router smoke helpers
def build_client(env, *, user="u1", workspace_roles=None, club=True):
    """FastAPI app with the finance routers; Cognito/membership dependencies are overridden."""
    import os
    from unittest import mock

    from fastapi import FastAPI, Query
    from fastapi.testclient import TestClient

    os.environ.setdefault("USER_POOL_ID", "test-pool")
    os.environ.setdefault("USER_POOL_API_CLIENT_ID", "test-client")
    with mock.patch("requests.get") as fake_get:  # auth.py downloads the JWKS at import time
        fake_get.return_value.json.return_value = {"keys": []}
        import auth
    import di
    from api.bank_accounts import router as bank_router
    from api.expenses import router as expense_router

    roles = workspace_roles if workspace_roles is not None else {WS: "admin", OTHER_WS: "admin"}
    app = FastAPI()
    app.include_router(bank_router)
    app.include_router(expense_router)

    def workspace_role(workspace_id: str | None = Query(None)):
        return roles.get(workspace_id)

    class ClubSvc:
        def get(self, account_id):
            return {"id": account_id, "settings": {"account_type": "club" if club else "tournament"}}

    app.dependency_overrides[auth.get_account_id] = lambda: ACC
    app.dependency_overrides[auth.get_account_role] = lambda: "admin"
    app.dependency_overrides[auth.get_workspace_role] = workspace_role
    app.dependency_overrides[auth.get_current_user] = lambda: {"sub": user}
    app.dependency_overrides[di.get_account_service] = lambda: ClubSvc()
    app.dependency_overrides[di.get_bank_account_service] = lambda: env.banks
    app.dependency_overrides[di.get_expense_service] = lambda: env.expenses
    return TestClient(app)
