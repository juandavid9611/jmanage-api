"""Expense service on moto: state machine, PUT lock, filters, ledger. Money paths use real transactions."""
import threading
from decimal import Decimal

import pytest
from fastapi import HTTPException

from api.schemas.expenses import CreateExpense, ExpenseFileSpec, MarkPaid, UpdateExpense
from repositories.ddb_transactions import TransactionConflict

from .test_expenses_support import (ACC, OTHER_WS, WS, approved_expense, env, make_bank,  # noqa: F401
                                    make_expense, pay)


def fails(status, call):
    with pytest.raises(HTTPException) as exc:
        call()
    assert exc.value.status_code == status, exc.value.detail
    return exc.value.detail


# ------------------------------------------------------------------ create / read
def test_create_defaults_and_validation(env):
    exp = make_expense(env, total=1234.5)
    assert exp["status"] == "pendiente" and exp["paid_amount"] == 0 and exp["total_amount"] == 1234.5
    assert exp["requested_by"] == "u1" and exp["account_id"] is None and exp["currency"] == "COP"
    assert exp["payments"] == [] and exp["proof_images"] == [] and exp["id"].startswith("exp_")
    for bad in (
        dict(team="otro"), dict(category="Nada"), dict(total=0), dict(total=-1), dict(total=1.234),
        dict(total=10**13), dict(expense_date="10/05/2026"), dict(budget_quantity=2),
    ):
        fails(422, lambda bad=bad: make_expense(env, **bad))


def test_client_cannot_write_protected_fields(env):
    body = CreateExpense(team="general", category="Cancha", detail="x", expense_date="2026-05-10",
                         total_amount=10, **{"status": "pagado", "paid_amount": 99, "account_id": "bac_x"})
    exp = env.expenses.create(body, WS, ACC, "u1")
    assert exp["status"] == "pendiente" and exp["paid_amount"] == 0 and exp["account_id"] is None
    upd = env.expenses.update(exp["id"], UpdateExpense(detail="y", **{"status": "pagado", "requested_by": "z"}), WS, ACC, "u1")
    assert upd["status"] == "pendiente" and upd["requested_by"] == "u1" and upd["detail"] == "y"


def test_budget_item_must_exist_in_this_account_and_workspace(env):
    env.budget.items["bgi_1"] = {"id": "bgi_1", "account_id": ACC, "workspace_id": WS}
    env.budget.items["bgi_other_ws"] = {"id": "bgi_other_ws", "account_id": ACC, "workspace_id": OTHER_WS}
    env.budget.items["bgi_other_acc"] = {"id": "bgi_other_acc", "account_id": "acc2", "workspace_id": WS}
    ok = make_expense(env, budget_item_id="bgi_1", budget_quantity=3)
    assert ok["budget_item_id"] == "bgi_1" and ok["budget_quantity"] == 3
    for item in ("bgi_missing", "bgi_other_ws", "bgi_other_acc"):
        fails(422, lambda item=item: make_expense(env, budget_item_id=item))
    fails(422, lambda: make_expense(env, budget_item_id="bgi_1", budget_quantity=0))


def test_tenant_isolation_and_wrong_workspace(env):
    exp = make_expense(env)
    calls = [
        lambda ws, acc: env.expenses.get(exp["id"], ws, acc),
        lambda ws, acc: env.expenses.update(exp["id"], UpdateExpense(detail="x"), ws, acc, "u1"),
        lambda ws, acc: env.expenses.delete(exp["id"], ws, acc),
        lambda ws, acc: env.expenses.approve(exp["id"], ws, acc, "u2"),
        lambda ws, acc: env.expenses.reject(exp["id"], "no", ws, acc, "u2"),
        lambda ws, acc: pay(env, exp["id"], "bac_x", ws=ws, acct=acc),
    ]
    for call in calls:
        fails(404, lambda call=call: call(WS, "acc2"))
        fails(404, lambda call=call: call(OTHER_WS, ACC))
    assert env.expenses.list_expenses(WS, "acc2") == [] and env.expenses.list_expenses(OTHER_WS, ACC) == []


# ------------------------------------------------------------------ PUT
def test_put_rules(env):
    exp = make_expense(env, total=100)
    upd = env.expenses.update(exp["id"], UpdateExpense(total_amount=250, beneficiary_name="  Nuevo "), WS, ACC, "u2")
    assert upd["total_amount"] == 250 and upd["beneficiary_name"] == "Nuevo" and upd["updated_by"] == "u2"
    assert upd["status"] == "pendiente"
    fails(422, lambda: env.expenses.update(exp["id"], UpdateExpense(category="Nada"), WS, ACC, "u1"))
    fails(422, lambda: env.expenses.update(exp["id"], UpdateExpense(budget_quantity=2), WS, ACC, "u1"))


def test_put_after_approval_sends_it_back_to_pending_only_for_sensitive_changes(env):
    exp = approved_expense(env, total=100)
    harmless = env.expenses.update(exp["id"], UpdateExpense(detail="otro detalle"), WS, ACC, "u1")
    assert harmless["status"] == "aprobado" and harmless["approved_by"] == "u2"
    same_amount = env.expenses.update(exp["id"], UpdateExpense(total_amount=100), WS, ACC, "u1")
    assert same_amount["status"] == "aprobado"  # nothing really changed
    changed = env.expenses.update(exp["id"], UpdateExpense(total_amount=999), WS, ACC, "u1")
    assert changed["status"] == "pendiente" and "approved_by" not in changed and "approved_at" not in changed
    again = env.expenses.update(exp["id"], UpdateExpense(beneficiary_name="Otro"), WS, ACC, "u1")
    assert again["status"] == "pendiente"
    env.expenses.approve(exp["id"], WS, ACC, "u2")
    assert env.expenses.update(exp["id"], UpdateExpense(category="Arbitraje"), WS, ACC, "u1")["status"] == "pendiente"


def test_put_is_locked_once_there_is_any_payment_or_it_is_rejected(env):
    bank = make_bank(env)
    exp = approved_expense(env, total=100)
    pay(env, exp["id"], bank["id"], amount=40)
    fails(409, lambda: env.expenses.update(exp["id"], UpdateExpense(total_amount=10), WS, ACC, "u1"))
    fails(409, lambda: env.expenses.update(exp["id"], UpdateExpense(detail="x"), WS, ACC, "u1"))
    rejected = make_expense(env)
    env.expenses.reject(rejected["id"], "duplicado", WS, ACC, "u2")
    fails(409, lambda: env.expenses.update(rejected["id"], UpdateExpense(detail="x"), WS, ACC, "u1"))


def test_clearing_the_budget_item_clears_the_quantity(env):
    env.budget.items["bgi_1"] = {"id": "bgi_1", "account_id": ACC, "workspace_id": WS}
    exp = make_expense(env, budget_item_id="bgi_1", budget_quantity=2)
    upd = env.expenses.update(exp["id"], UpdateExpense(budget_item_id=None), WS, ACC, "u1")
    assert upd["budget_item_id"] is None and upd["budget_quantity"] is None


# ------------------------------------------------------------------ delete / approve / reject
def test_delete_only_pending(env):
    pending = make_expense(env)
    assert env.expenses.delete(pending["id"], WS, ACC) == {"deleted_expense_id": pending["id"]}
    fails(404, lambda: env.expenses.get(pending["id"], WS, ACC))
    for make in (lambda: approved_expense(env), lambda: _rejected(env)):
        exp = make()
        fails(409, lambda exp=exp: env.expenses.delete(exp["id"], WS, ACC))


def _rejected(env):
    exp = make_expense(env)
    env.expenses.reject(exp["id"], "no", WS, ACC, "u2")
    return exp


def test_approve_rules(env):
    exp = make_expense(env)  # requested by u1; the workspace has two admins (u1, u2)
    fails(409, lambda: env.expenses.approve(exp["id"], WS, ACC, "u1"))  # needs another admin
    approved = env.expenses.approve(exp["id"], WS, ACC, "u2")
    assert approved["status"] == "aprobado" and approved["approved_by"] == "u2" and approved["approved_at"]
    fails(409, lambda: env.expenses.approve(exp["id"], WS, ACC, "u2"))  # only from pendiente


def test_single_admin_may_approve_own_expense(env):
    env.membership.roles = {("u1", WS): "admin", ("u3", WS): "user"}
    exp = make_expense(env)
    assert env.expenses.approve(exp["id"], WS, ACC, "u1")["approved_by"] == "u1"


def test_reject_rules(env):
    bank = make_bank(env)
    fails(422, lambda: env.expenses.reject(make_expense(env)["id"], "   ", WS, ACC, "u2"))
    pending = make_expense(env)
    rej = env.expenses.reject(pending["id"], "no corresponde", WS, ACC, "u2")
    assert rej["status"] == "rechazado" and rej["rejection_reason"] == "no corresponde"
    fails(409, lambda: env.expenses.reject(pending["id"], "otra vez", WS, ACC, "u2"))
    fails(409, lambda: env.expenses.approve(pending["id"], WS, ACC, "u2"))
    approved = approved_expense(env)
    assert env.expenses.reject(approved["id"], "error", WS, ACC, "u1")["status"] == "rechazado"
    partial = approved_expense(env, total=100)
    pay(env, partial["id"], bank["id"], amount=10)
    fails(409, lambda: env.expenses.reject(partial["id"], "tarde", WS, ACC, "u2"))
    fails(409, lambda: pay(env, approved["id"], bank["id"], key="key-rejected-1"))


# ------------------------------------------------------------------ mark-paid
def test_pending_expense_cannot_be_paid(env):
    bank = make_bank(env)
    exp = make_expense(env)
    fails(409, lambda: pay(env, exp["id"], bank["id"]))
    assert env.payment_repo.list_by_expense(exp["id"], ACC) == []


def test_mark_paid_full_balance_creates_payment_and_closes_expense(env):
    bank = make_bank(env, opening=1000)
    exp = approved_expense(env, total=100000)
    payment = pay(env, exp["id"], bank["id"], files=["comprobante.png"])
    assert payment["amount"] == 100000 and payment["account_id"] == bank["id"]
    assert payment["id"] == f"epy_{exp['id']}_key-00000001" and "file_keys" not in payment
    detail = env.expenses.get(exp["id"], WS, ACC)
    assert detail["status"] == "pagado" and detail["paid_amount"] == 100000 and detail["account_id"] == bank["id"]
    assert [p["id"] for p in detail["payments"]] == [payment["id"]]
    assert len(detail["proof_images"]) == 1 and detail["proof_images"][0].startswith("https://")
    assert f"/expenses/{exp['id']}/" in detail["proof_images"][0] and "Signature" in detail["proof_images"][0]
    stored = env.payment_repo.get(payment["id"], ACC)
    assert stored["file_keys"] == [f"dev/accounts/{ACC}/expenses/{exp['id']}/{payment['id']}/comprobante.png"]
    assert env.banks.get(bank["id"], WS, ACC)["balance"] == 1000 - 100000


def test_installments_from_different_accounts_and_boundaries(env):
    bank_a, bank_b = make_bank(env, name="A"), make_bank(env, name="B")
    exp = approved_expense(env, total=100)
    pay(env, exp["id"], bank_a["id"], amount=30, key="key-first-001")
    mid = env.expenses.get(exp["id"], WS, ACC)
    assert mid["status"] == "aprobado" and mid["paid_amount"] == 30 and mid["account_id"] == bank_a["id"]
    fails(422, lambda: pay(env, exp["id"], bank_b["id"], amount=70.01, key="key-over-0001"))  # overpay by a cent
    fails(422, lambda: pay(env, exp["id"], bank_b["id"], amount=0, key="key-zero-0001"))
    fails(422, lambda: pay(env, exp["id"], bank_b["id"], amount=-5, key="key-neg-00001"))
    pay(env, exp["id"], bank_b["id"], amount=70, key="key-second-01")  # exactly the remainder
    done = env.expenses.get(exp["id"], WS, ACC)
    assert done["status"] == "pagado" and done["paid_amount"] == 100 and done["account_id"] == bank_b["id"]
    assert len(done["payments"]) == 2
    fails(409, lambda: pay(env, exp["id"], bank_b["id"], amount=1, key="key-after-001"))
    assert env.banks.get(bank_a["id"], WS, ACC)["balance"] == 1000 - 30
    assert env.banks.get(bank_b["id"], WS, ACC)["balance"] == 1000 - 70


def test_decimal_installments_are_exact(env):
    bank = make_bank(env, opening=0)
    exp = approved_expense(env, total=0.3)
    pay(env, exp["id"], bank["id"], amount=0.1, key="key-dec-0001")
    pay(env, exp["id"], bank["id"], amount=0.2, key="key-dec-0002")
    done = env.expenses.get(exp["id"], WS, ACC)
    assert done["status"] == "pagado" and Decimal(str(done["paid_amount"])) == Decimal("0.3")


def test_replay_same_key_returns_stored_payment_and_changes_nothing(env):
    bank = make_bank(env)
    exp = approved_expense(env, total=100)
    first = pay(env, exp["id"], bank["id"], amount=40, key="key-replay-01")
    again = pay(env, exp["id"], bank["id"], amount=40, key="key-replay-01")
    omitted = pay(env, exp["id"], bank["id"], amount=None, key="key-replay-01")  # amount omitted on retry
    assert first == again == omitted
    detail = env.expenses.get(exp["id"], WS, ACC)
    assert detail["paid_amount"] == 40 and len(detail["payments"]) == 1
    fails(409, lambda: pay(env, exp["id"], bank["id"], amount=41, key="key-replay-01"))  # same key, other amount
    other = make_bank(env, name="Otra")
    fails(409, lambda: pay(env, exp["id"], other["id"], amount=40, key="key-replay-01"))  # same key, other account
    assert env.expenses.get(exp["id"], WS, ACC)["paid_amount"] == 40


def test_replay_after_the_expense_is_fully_paid_still_returns_200_state(env):
    bank = make_bank(env)
    exp = approved_expense(env, total=100)
    first = pay(env, exp["id"], bank["id"], key="key-full-0001")
    assert env.expenses.get(exp["id"], WS, ACC)["status"] == "pagado"
    assert pay(env, exp["id"], bank["id"], key="key-full-0001") == first


def test_concurrent_double_submit_same_key_one_payment(env):
    bank = make_bank(env)
    exp = approved_expense(env, total=100)
    results, errors = [], []

    def go():
        try:
            results.append(pay(env, exp["id"], bank["id"], key="key-double-01"))
        except HTTPException as exc:  # pragma: no cover - must not happen
            errors.append(exc)

    threads = [threading.Thread(target=go) for _ in range(8)]
    [t.start() for t in threads]
    [t.join() for t in threads]
    assert not errors and len(results) == 8 and all(r == results[0] for r in results)
    detail = env.expenses.get(exp["id"], WS, ACC)
    assert detail["paid_amount"] == 100 and len(detail["payments"]) == 1


def test_concurrent_different_keys_cannot_overpay(env):
    """Both read remaining=100 and try to pay it all: the conditional update lets exactly one win."""
    bank = make_bank(env)
    exp = approved_expense(env, total=100)
    stale = env.expense_repo.get(exp["id"], ACC)  # what a second request saw before the first committed
    pay(env, exp["id"], bank["id"], key="key-winner-01")
    from repositories.ddb_transactions import transact_write
    loser_payment = {"id": f"epy_{exp['id']}_key-loser-001", "account_id": ACC, "expense_id": exp["id"],
                     "bank_account_id": bank["id"], "amount": Decimal("100")}
    with pytest.raises(TransactionConflict) as caught:
        transact_write([
            env.payment_repo.tx_create(loser_payment),
            env.expense_repo.tx_register_payment(stale, amount=Decimal("100"), bank_account_id=bank["id"],
                                                 new_status="pagado", now="t", user_id="u1"),
        ])
    assert caught.value.conditional_failed == [1]  # the expense condition rejected it; payment row rolled back
    assert env.payment_repo.get(loser_payment["id"], ACC) is None
    assert env.expenses.get(exp["id"], WS, ACC)["paid_amount"] == 100
    # and through the service, with a stale snapshot (status/paid changed under it) the answer is a clean 409
    fails(409, lambda: pay(env, exp["id"], bank["id"], amount=1, key="key-loser-002"))


def test_concurrent_threads_with_different_keys_total_never_exceeds(env):
    bank = make_bank(env)
    exp = approved_expense(env, total=100)
    outcomes = []

    def go(i):
        try:
            pay(env, exp["id"], bank["id"], amount=60, key=f"key-thread-{i:03d}")
            outcomes.append("ok")
        except HTTPException as exc:
            outcomes.append(exc.status_code)

    threads = [threading.Thread(target=go, args=(i,)) for i in range(6)]
    [t.start() for t in threads]
    [t.join() for t in threads]
    assert outcomes.count("ok") == 1  # 60 + 60 > 100
    assert env.expenses.get(exp["id"], WS, ACC)["paid_amount"] == 60


def test_mark_paid_account_and_input_validation(env):
    bank = make_bank(env)
    exp = approved_expense(env, total=100)
    fails(422, lambda: pay(env, exp["id"], "bac_missing"))
    other_tenant = make_bank(env, acct="acc2")
    fails(422, lambda: pay(env, exp["id"], other_tenant["id"]))  # other tenant's account looks like it does not exist
    env.banks.deactivate(bank["id"], WS, ACC, "u1")
    fails(422, lambda: pay(env, exp["id"], bank["id"]))
    live = make_bank(env, name="Viva")
    fails(422, lambda: pay(env, exp["id"], live["id"], paid_at="2999-01-01"))
    fails(422, lambda: pay(env, exp["id"], live["id"], paid_at="ayer"))
    fails(422, lambda: pay(env, exp["id"], live["id"], key="corta"))
    fails(422, lambda: pay(env, exp["id"], live["id"], key="has space in key"))
    fails(422, lambda: pay(env, exp["id"], live["id"], files=["../secret.png"]))
    fails(422, lambda: pay(env, exp["id"], live["id"], files=["a/b.png"]))
    fails(422, lambda: pay(env, exp["id"], live["id"], amount=1.234))
    assert env.expenses.get(exp["id"], WS, ACC)["paid_amount"] == 0  # nothing leaked through
    assert pay(env, exp["id"], live["id"], paid_at="2026-05-11")["amount"] == 100


def test_cross_workspace_account_requires_admin_of_its_workspace(env):
    foreign = make_bank(env, ws=OTHER_WS)  # lives in another workspace
    exp = approved_expense(env, total=100)
    # u2 is admin of WS only -> 403; u1 is admin of both -> allowed
    fails(403, lambda: pay(env, exp["id"], foreign["id"], user="u2"))
    assert env.expenses.get(exp["id"], WS, ACC)["paid_amount"] == 0
    assert pay(env, exp["id"], foreign["id"], user="u1", key="key-xws-0001")["account_id"] == foreign["id"]
    assert env.banks.get(foreign["id"], OTHER_WS, ACC)["balance"] == 1000 - 100


def test_mark_paid_without_idempotency_key_still_bounded(env):
    bank = make_bank(env)
    exp = approved_expense(env, total=100)
    body = lambda: MarkPaid(amount=60, account_id=bank["id"], paid_at="2026-05-11", file_names=["a.png"])  # noqa: E731
    first = env.expenses.mark_paid(exp["id"], body(), WS, ACC, "u1")
    assert first["idempotency_key"] and "/uploads/" in env.payment_repo.get(first["id"], ACC)["file_keys"][0]
    fails(422, lambda: env.expenses.mark_paid(exp["id"], body(), WS, ACC, "u1"))  # no dedupe, but never over-pays
    assert env.expenses.get(exp["id"], WS, ACC)["paid_amount"] == 60


def test_currency_mismatch_between_bank_and_expense_is_rejected(env):
    bank = make_bank(env)
    env.bank_repo.table.update_item(Key={"id": bank["id"]}, UpdateExpression="SET currency = :c",
                                    ExpressionAttributeValues={":c": "USD"})
    exp = approved_expense(env, total=100)
    fails(422, lambda: pay(env, exp["id"], bank["id"]))


# ------------------------------------------------------------------ list filters
def test_list_filters_are_server_side_paginated_and_sorted(env):
    env.expense_repo.page_limit = 2  # force several pages under the filter
    bank = make_bank(env)
    rows = [
        ("general", "Cancha", "2026-05-01"), ("masculino", "Cancha", "2026-05-20"),
        ("femenino", "Arbitraje", "2026-04-10"), ("general", "Arbitraje", "2026-05-20"),
        ("master", "Otros", "2026-06-01"), ("general", "Cancha", "2026-03-15"),
    ]
    made = [make_expense(env, team=t, category=c, expense_date=d, total=10) for t, c, d in rows]
    env.expenses.approve(made[0]["id"], WS, ACC, "u2")
    pay(env, made[0]["id"], bank["id"], key="key-filter-01")  # pagado, account = bank
    make_expense(env, ws=OTHER_WS, expense_date="2026-05-20")  # other workspace never shows up

    everything = env.expenses.list_expenses(WS, ACC)
    assert [e["expense_date"] for e in everything] == sorted((r[2] for r in rows), reverse=True)
    ids_same_day = [e["id"] for e in everything if e["expense_date"] == "2026-05-20"]
    assert ids_same_day == sorted(ids_same_day)  # then by id
    assert {e["status"] for e in env.expenses.list_expenses(WS, ACC, status="pagado")} == {"pagado"}
    assert len(env.expenses.list_expenses(WS, ACC, status="pendiente")) == 5
    assert [e["id"] for e in env.expenses.list_expenses(WS, ACC, bank_account_id=bank["id"])] == [made[0]["id"]]
    assert len(env.expenses.list_expenses(WS, ACC, team="general")) == 3
    assert len(env.expenses.list_expenses(WS, ACC, category="Arbitraje")) == 2
    assert len(env.expenses.list_expenses(WS, ACC, date_from="2026-05-01", date_to="2026-05-20")) == 3
    assert len(env.expenses.list_expenses(WS, ACC, team="general", category="Cancha", date_to="2026-04-30")) == 1
    fails(422, lambda: env.expenses.list_expenses(WS, ACC, status="otro"))
    fails(422, lambda: env.expenses.list_expenses(WS, ACC, date_from="x"))


# ------------------------------------------------------------------ files
def test_presigned_uploads_are_scoped_validated_and_match_mark_paid_keys(env):
    exp = make_expense(env)
    out = env.expenses.presign_uploads(
        exp["id"], [ExpenseFileSpec(file_name="foto.png", content_type="image/png")], "key-upload-01", WS, ACC)
    url = out["urls"]["foto.png"]
    assert f"dev/accounts/{ACC}/expenses/{exp['id']}/epy_{exp['id']}_key-upload-01/foto.png" in url
    legacy = env.expenses.presign_uploads(
        exp["id"], [ExpenseFileSpec(file_name="foto.png", content_type="image/png")], None, WS, ACC)
    assert f"/expenses/{exp['id']}/uploads/foto.png" in legacy["urls"]["foto.png"]
    for bad in (
        [ExpenseFileSpec(file_name="../x.png", content_type="image/png")],
        [ExpenseFileSpec(file_name="x.exe", content_type="application/x-msdownload")],
        [],
    ):
        fails(422, lambda bad=bad: env.expenses.presign_uploads(exp["id"], bad, None, WS, ACC))
    fails(404, lambda: env.expenses.presign_uploads(exp["id"], [ExpenseFileSpec(file_name="a.png", content_type="image/png")], None, OTHER_WS, ACC))
    fails(404, lambda: env.expenses.presign_uploads(exp["id"], [ExpenseFileSpec(file_name="a.png", content_type="image/png")], None, WS, "acc2"))
