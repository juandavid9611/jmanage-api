"""Bank accounts + adjustments service on moto (real conditions, GSIs and pagination)."""
from decimal import Decimal

import pytest
from fastapi import HTTPException

from api.schemas.bank_accounts import CreateAdjustment, CreateBankAccount, UpdateBankAccount

from .test_expenses_support import (ACC, OTHER_WS, WS, approved_expense, env, make_adjustment,  # noqa: F401
                                    make_bank, make_expense, pay)


def status_of(exc_info) -> int:
    return exc_info.value.status_code


def test_create_get_list_with_derived_balance_and_currency(env):
    bank = make_bank(env, opening=1000)
    assert bank["balance"] == 1000 and bank["currency"] == "COP" and bank["is_active"] is True
    assert bank["id"].startswith("bac_") and "account_id" not in bank  # tenant column is not part of the wire
    assert env.banks.get(bank["id"], WS, ACC)["balance"] == 1000
    assert [b["id"] for b in env.banks.list_accounts(WS, ACC)] == [bank["id"]]


def test_balance_is_opening_plus_adjustments_minus_expense_payments(env):
    bank = make_bank(env, opening=1000)
    make_adjustment(env, bank["id"], 500)
    make_adjustment(env, bank["id"], -200)
    exp = approved_expense(env, total=300)
    pay(env, exp["id"], bank["id"], amount=250)
    assert env.banks.get(bank["id"], WS, ACC)["balance"] == 1000 + 500 - 200 - 250


def test_balance_decimal_exactness(env):
    bank = make_bank(env, opening=0)
    make_adjustment(env, bank["id"], 0.1)
    make_adjustment(env, bank["id"], 0.2)
    balance = env.banks.get(bank["id"], WS, ACC)["balance"]
    assert Decimal(str(balance)) == Decimal("0.3")  # never 0.30000000000000004


def test_balance_reads_every_page_of_both_sets(env):
    bank = make_bank(env, opening=0)
    # adjustments: 3 rows of ~350 KB => > 1 MB, DynamoDB itself splits the query in pages
    for i in range(3):
        env.adjustment_repo.table.put_item(Item={
            "id": f"adj_big{i}", "account_id": ACC, "bank_account_id": bank["id"], "workspace_id": WS,
            "amount": Decimal("10"), "date": "2026-01-0%d" % (i + 1), "note": "x" * 350_000,
        })
    # payments: forced small pages
    env.payment_repo.page_limit = 2
    for i in range(7):
        env.payment_repo.table.put_item(Item={
            "id": f"epy_p{i}", "account_id": ACC, "bank_account_id": bank["id"], "expense_id": "exp_x",
            "amount": Decimal("1"),
        })
    resp = env.adjustment_repo.table.query(
        IndexName="bank_account_id_index",
        KeyConditionExpression="bank_account_id = :b", ExpressionAttributeValues={":b": bank["id"]})
    assert "LastEvaluatedKey" in resp  # proves the adjustments really span more than one page
    assert env.banks.get(bank["id"], WS, ACC)["balance"] == 30 - 7


def test_tenant_isolation_and_wrong_workspace_are_404(env):
    bank = make_bank(env)
    with pytest.raises(HTTPException) as other_tenant:
        env.banks.get(bank["id"], WS, "acc2")
    with pytest.raises(HTTPException) as other_ws:
        env.banks.get(bank["id"], OTHER_WS, ACC)
    assert status_of(other_tenant) == status_of(other_ws) == 404
    assert env.banks.list_accounts(OTHER_WS, ACC) == [] and env.banks.list_accounts(WS, "acc2") == []
    for call in (
        lambda: env.banks.update(bank["id"], UpdateBankAccount(name="x"), WS, "acc2", "u1"),
        lambda: env.banks.deactivate(bank["id"], OTHER_WS, ACC, "u1"),
        lambda: env.banks.list_adjustments(bank["id"], WS, "acc2"),
        lambda: env.banks.create_adjustment(bank["id"], CreateAdjustment(amount=1, date="2026-01-01"), OTHER_WS, ACC, "u1"),
    ):
        with pytest.raises(HTTPException) as exc:
            call()
        assert exc.value.status_code == 404


def test_validation(env):
    bad = [
        dict(name="a", type="crypto", opening_balance=0, opening_balance_date="2026-01-01"),
        dict(name="a", type="cash", opening_balance=1.234, opening_balance_date="2026-01-01"),
        dict(name="a", type="cash", opening_balance="abc", opening_balance_date="2026-01-01"),
        dict(name="a", type="cash", opening_balance=0, opening_balance_date="01/01/2026"),
        dict(name="   ", type="cash", opening_balance=0, opening_balance_date="2026-01-01"),
    ]
    for payload in bad:
        with pytest.raises(HTTPException) as exc:
            env.banks.create(CreateBankAccount(**payload), WS, ACC, "u1")
        assert exc.value.status_code == 422, payload
    bank = make_bank(env)
    for amount in (0, float("nan"), 10**13, 1.005):
        with pytest.raises(HTTPException) as exc:
            env.banks.create_adjustment(bank["id"], CreateAdjustment(amount=amount, date="2026-01-01"), WS, ACC, "u1")
        assert exc.value.status_code == 422
    assert make_bank(env, opening=-50)["balance"] == -50  # an overdrawn opening balance is legitimate


def test_update_and_soft_delete_semantics(env):
    bank = make_bank(env, opening=100)
    updated = env.banks.update(bank["id"], UpdateBankAccount(name="Bancolombia", opening_balance=250.5), WS, ACC, "u2")
    assert updated["name"] == "Bancolombia" and updated["balance"] == 250.5 and updated["updated_by"] == "u2"
    assert updated["currency"] == "COP" and updated["type"] == "cash"
    gone = env.banks.deactivate(bank["id"], WS, ACC, "u1")
    assert gone["is_active"] is False
    again = env.banks.deactivate(bank["id"], WS, ACC, "u2")  # already inactive: 200 no-op
    assert again["is_active"] is False and again["updated_by"] == "u1"
    assert env.bank_repo.get(bank["id"], ACC) is not None  # never hard-deleted
    with pytest.raises(HTTPException) as exc:  # no new money into an inactive account
        make_adjustment(env, bank["id"], 5)
    assert exc.value.status_code == 409


def test_adjustments_manual_only_sorted_and_visible_with_payment_request_ones(env):
    bank = make_bank(env)
    first = make_adjustment(env, bank["id"], -30, date="2026-02-01", note="  retiro ")
    assert first["source"] == "manual" and first["note"] == "retiro" and first["amount"] == -30
    assert first["payment_request_id"] is None and first["created_by"] == "u1"
    # an adjustment created by a payment request (other branch) shows up and counts
    env.adjustment_repo.create({
        "id": "adj_pr_pr1_sub1", "account_id": ACC, "workspace_id": WS, "bank_account_id": bank["id"],
        "amount": Decimal("70"), "date": "2026-03-01", "note": "Abono", "source": "payment_request",
        "payment_request_id": "pr1", "submission_id": "sub1", "created_by": "u1", "created_at": "t",
    })
    listed = env.banks.list_adjustments(bank["id"], WS, ACC)
    assert [a["date"] for a in listed] == ["2026-03-01", "2026-02-01"]
    assert env.banks.get(bank["id"], WS, ACC)["balance"] == 1000 - 30 + 70
    with pytest.raises(HTTPException) as exc:
        env.banks.create_adjustment(bank["id"], CreateAdjustment(amount=5, date="2026-13-45"), WS, ACC, "u1")
    assert exc.value.status_code == 422
