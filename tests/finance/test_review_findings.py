"""Adversarial review of the finance backend: one test per finding (moto, real transactions).

A test that fails today is a confirmed defect; fixed ones are plain tests, the rest are
`xfail(strict=True, reason=<finding id>)`.
"""
import threading
from decimal import Decimal

import pytest
from fastapi import HTTPException

from api.schemas.payments import BulkPutPaymentRequest
from tests.finance.installments_support import (
    ACC, ADMIN, PAYEE, WS, FakeMembership, FakeNotifier, FakeOrderRepo, get_pr, make_service, put_bank, put_pr, review,
    submit,
)
from tests.finance.test_installments_api import api  # noqa: F401  (fixture)
from tests.finance.test_expenses_support import (  # noqa: F401
    approved_expense, env, make_bank, make_expense, pay,
)

PAST = "2000-01-01T00:00:00"


def status_of(call):
    with pytest.raises(HTTPException) as exc:
        call()
    return exc.value.status_code


# --------------------------------------------------------------------------- F1
def test_f1_stale_concurrent_expense_payments_never_leave_a_fully_paid_expense_approved(env):
    """Two installments that each read paid=0 (60 + 40 of 100): the second used to be applied on top of the
    first with the status computed from the stale read, leaving paid == total but status 'aprobado'
    (stuck: nothing left to pay, cannot be rejected nor edited)."""
    exp = approved_expense(env, total=100)
    bank = make_bank(env)
    stale = env.expense_repo.get(exp["id"], "acc1")
    env.expenses._get_or_404 = lambda *a, **k: dict(stale)  # both calls read the same old snapshot
    pay(env, exp["id"], bank["id"], amount=60, key="key-A-000001")
    try:
        pay(env, exp["id"], bank["id"], amount=40, key="key-B-000001")
    except HTTPException as e:
        assert e.status_code == 409  # rejecting the stale write is acceptable
    final = env.expense_repo.get(exp["id"], "acc1")
    assert not (final["paid_amount"] >= final["total_amount"] and final["status"] != "pagado"), final
    assert final["paid_amount"] <= final["total_amount"]


# --------------------------------------------------------------------------- F2
@pytest.mark.parametrize("status,paid", [("overdue", "40000"), ("approval_pending", "40000")])
def test_f2_cancel_for_order_never_cancels_a_request_with_approved_payments(finance_db, status, paid):
    put_pr("pr1", status=status, total="100000", paid=paid, order_id="ord1")
    svc = make_service()
    assert svc.cancel_for_order("pr1", ACC) is False
    assert get_pr("pr1")["payment_status"] == status


# --------------------------------------------------------------------------- F3
def _approved_installment(svc, bank=None):
    put_pr("pr1", total="100000", allow=True, max_installments=3)
    out = submit(svc, amount=Decimal("30000"), bank=bank)
    review(svc, out["submissionId"], bank=bank)


def test_f3_cannot_delete_a_request_with_approved_payments(finance_db):
    put_bank(None)
    svc = make_service()
    _approved_installment(svc, bank="bac_1")
    assert status_of(lambda: svc.delete("pr1", ACC)) == 409
    assert get_pr("pr1")


def test_f3b_can_still_delete_an_untouched_request(finance_db):
    put_pr("pr1")
    make_service().delete("pr1", ACC)


# --------------------------------------------------------------------------- F4
def test_f4_one_failing_account_notification_does_not_abort_the_batch(finance_db):
    put_pr("a1", account="accA", due=PAST, created_time=1)
    put_pr("b1", account="accB", due=PAST, created_time=2)

    class Flaky(FakeNotifier):
        def overdue_payments_processed(self, **kw):
            super().overdue_payments_processed(**kw)
            if kw["account_id"] == "accA":
                raise RuntimeError("courier down")

    svc = make_service(notifier=Flaky())
    svc.process_overdue_payments()  # accA's notifier blows up; accB must still be processed
    assert get_pr("a1")["payment_status"] == "overdue" and get_pr("b1")["payment_status"] == "overdue"
    svc.process_overdue_payments()  # idempotent
    assert get_pr("b1")["payment_status"] == "overdue"


# --------------------------------------------------------------------------- verified-solid probes
def test_f5b_scheduler_handler_runs_with_only_its_own_env(finance_db, monkeypatch):
    import scheduled_handler

    for var in ("BANK_ACCOUNT_TABLE_NAME", "EXPENSE_TABLE_NAME", "EXPENSE_PAYMENT_TABLE_NAME",
                "BUDGET_ITEM_TABLE_NAME", "BUDGET_QUANTITY_TABLE_NAME", "ACCOUNT_TABLE_NAME", "ORDER_TABLE_NAME"):
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setenv("NOTIFICATION_TABLE_NAME", "test-PaymentRequest")  # never touched without a notification
    monkeypatch.setenv("USER_TABLE_NAME", "x")
    monkeypatch.setenv("MEMBERSHIPS_TABLE_NAME", "x")
    put_pr("a", due=PAST, overdue_price="120000")
    put_pr("p", status="partial", paid="30000", due=PAST, overdue_price="130000", created_time=2)
    # the notifier talks to Courier/OneSignal: stub the network edge only
    import di
    monkeypatch.setattr(di, "get_notification_orchestator", lambda: FakeNotifier())
    out = scheduled_handler.handler({}, None)
    assert out["processed"] == 2
    assert get_pr("p")["payment_status"] == "overdue" and get_pr("p")["paid_amount"] == Decimal("30000")
    assert get_pr("a")["original_amount"] == Decimal("100000")
    assert scheduled_handler.handler({}, None)["processed"] == 0


def test_double_review_threads_count_once(finance_db):
    put_bank(None)
    svc = make_service()
    put_pr("pr1", total="100000", allow=True, max_installments=3)
    out = submit(svc, amount=Decimal("30000"), bank="bac_1")
    results, errors = [], []

    def run():
        try:
            results.append(review(svc, out["submissionId"], bank="bac_1"))
        except HTTPException as e:
            errors.append(e.status_code)

    threads = [threading.Thread(target=run) for _ in range(6)]
    [t.start() for t in threads]
    [t.join() for t in threads]
    assert get_pr("pr1")["paid_amount"] == Decimal("30000")
    from repositories.ddb_session import bank_adjustment_table
    assert len(bank_adjustment_table().scan()["Items"]) == 1
    assert set(errors) <= {409}


def test_old_frontend_flow_request_approval_then_put_paid(finance_db):
    from tests.finance.installments_support import upload
    put_pr("pr1", total="100000")
    svc = make_service()
    upload(svc, "r.png")
    svc.request_payment_request_approval("pr1", ACC, ["r.png"], user_id=PAYEE)
    assert get_pr("pr1")["payment_status"] == "approval_pending"
    body = BulkPutPaymentRequest(status="paid", createDate="x", dueDate="2999-01-01T00:00:00", concept="Mensualidad",
                                 description="d", category="monthly", group=WS, paymentRequestTo=[], userPrice=Decimal("100000"))
    # old UI sends a full body; the pending submission was created by the alias, so the admin must review it
    assert status_of(lambda: svc.update("pr1", ACC, body, updated_by=ADMIN)) == 409
    sub = svc.get_detail("pr1", ACC, viewer_is_admin=True)["submissions"][0]
    review(svc, sub["id"])
    assert get_pr("pr1")["payment_status"] == "paid"


# --------------------------------------------------------------------------- F5
def test_f5_scheduler_service_needs_no_order_or_finance_tables(finance_db, monkeypatch):
    """The PaymentScheduler lambda has no ORDER/PRODUCT/BANK_ACCOUNT/EXPENSE/BUDGET table env vars:
    building the service must not touch them (OrderRepo() used to raise at construction)."""
    import di

    for var in ("ORDER_TABLE_NAME", "PRODUCT_TABLE_NAME", "BANK_ACCOUNT_TABLE_NAME", "EXPENSE_TABLE_NAME",
                "EXPENSE_PAYMENT_TABLE_NAME", "BUDGET_ITEM_TABLE_NAME", "BUDGET_QUANTITY_TABLE_NAME"):
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setattr(di, "get_notification_orchestator", lambda: FakeNotifier())
    put_pr("a", due=PAST, overdue_price="120000")
    put_pr("p", status="partial", paid="30000", due=PAST, overdue_price="130000", created_time=2)
    svc = di.get_payment_request_service()
    assert len(svc.process_overdue_payments()) == 2
    assert get_pr("p")["paid_amount"] == Decimal("30000") and get_pr("a")["original_amount"] == Decimal("100000")
    assert svc.process_overdue_payments() == []


# --------------------------------------------------------------------------- F6
@pytest.mark.parametrize("value", [10**30, "1e400", "9" * 40, Decimal("1E+30")])
def test_f6_huge_amounts_are_a_validation_error_not_a_500(value):
    from core.finance_money import MoneyError, parse_money

    with pytest.raises(MoneyError):
        parse_money(value)


# --------------------------------------------------------------------------- F7
@pytest.mark.xfail(strict=True, reason="F7 pre-existing: GET /payment_requests/:id lets any account member read anyone's request")
def test_f7_another_member_cannot_read_someones_payment_request(api):  # noqa: F811
    put_pr("pr1", total="100000", images=["dev/accounts/acc1/users/user_payee/invoices/pr1/a.png"])
    api.state["user"] = "other"  # same account, plain member, not the payee, not an admin
    assert api.get("/payment_requests/pr1").status_code in (403, 404)


# --------------------------------------------------------------------------- F8
@pytest.mark.xfail(strict=True, reason="F8 legacy receipts (no submission row) can be overwritten by the payee")
def test_f8_payee_cannot_overwrite_receipts_of_a_request_under_review_or_paid(finance_db):
    from api.schemas.files import FileSpec

    key = "dev/accounts/acc1/users/user_payee/invoices/pr1/a.png"
    put_pr("pr1", status="approval_pending", total="100000", images=[key])
    svc = make_service()
    assert status_of(lambda: svc.generate_put_presigned_urls("pr1", ACC, [FileSpec(file_name="a.png", content_type="image/png")])) == 409


# --------------------------------------------------------------------------- F9
@pytest.mark.xfail(strict=True, reason="F9 fixed in the following commit")
def test_f9_put_guard_uses_the_strongly_consistent_paid_amount_not_only_the_gsi(finance_db):
    """`_has_approved_submission` reads a GSI (eventually consistent). Right after an approval it may still
    return nothing; the PUT must then still refuse to move a request with approved money out of partial."""
    put_pr("pr1", status="partial", total="100000", paid="30000")
    svc = make_service()
    svc._stored_submissions = lambda *a, **k: []  # stale GSI
    body = BulkPutPaymentRequest(status="pending", createDate="x", dueDate="2999-01-01T00:00:00", concept="c",
                                 description="d", category="m", group=WS, paymentRequestTo=[], userPrice=Decimal("100000"))
    assert status_of(lambda: svc.update("pr1", ACC, body, updated_by=ADMIN)) == 409
    assert get_pr("pr1")["paid_amount"] == Decimal("30000")


# --------------------------------------------------------------------------- F10
@pytest.mark.xfail(strict=True, reason="F10 raising the price of a paid request leaves status=paid with paid_amount < total (design decision)")
def test_f10_raising_the_price_of_a_paid_request_keeps_paid_equal_to_total(finance_db):
    put_pr("pr1", status="paid", total="100000", paid="100000")
    svc = make_service()
    body = BulkPutPaymentRequest(createDate="x", dueDate="2999-01-01T00:00:00", concept="c", description="d",
                                 category="m", group=WS, paymentRequestTo=[], userPrice=Decimal("150000"))
    try:
        svc.update("pr1", ACC, body, updated_by=ADMIN)
    except HTTPException:
        return
    raw = get_pr("pr1")
    assert raw["payment_status"] != "paid" or raw["paid_amount"] >= raw["user_price"]
