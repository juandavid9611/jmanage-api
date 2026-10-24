"""Existing payment requests (rows without any installment attribute) and the current frontend must keep behaving as before.

Found by replaying the same operations on the 2,357 real production rows through `main` and through this
branch (see the PR): these are the cases where the first version of this change differed.
"""
from decimal import Decimal

import pytest
from fastapi import HTTPException

from api.schemas.payments import BulkPutPaymentRequest
from tests.finance.installments_support import (ACC, ADMIN, WS, get_pr, make_service, put_pr, review, submit)


def body(**over):
    base = dict(createDate="x", dueDate="2999-01-01T00:00:00", concept="Mensualidad", description="d",
                category="monthly", group=WS, paymentRequestTo=[], userPrice=Decimal("100000"))
    base.update(over)
    return BulkPutPaymentRequest(**base)


def test_correcting_the_price_of_a_paid_request_works_as_before_when_no_payment_is_tracked(finance_db):
    svc = make_service()
    put_pr("legacy", status="paid", total="100000")
    svc.update("legacy", ACC, body(userPrice=Decimal("110000")), updated_by=ADMIN)  # raise
    assert get_pr("legacy")["payment_status"] == "paid" and get_pr("legacy")["user_price"] == Decimal("110000")
    assert get_pr("legacy")["paid_amount"] == Decimal("110000")  # paid in full follows the corrected price
    svc.update("legacy", ACC, body(userPrice=Decimal("90000")), updated_by=ADMIN)  # lower
    assert get_pr("legacy")["payment_status"] == "paid" and get_pr("legacy")["paid_amount"] == Decimal("90000")


def test_paid_request_with_tracked_installments_cannot_be_repriced_by_hand(finance_db):
    svc = make_service()
    put_pr("tracked", total="100000", allow=True, max_installments=2)
    sub = submit(svc, pr_id="tracked", amount="100000")
    review(svc, sub["submissionId"], pr_id="tracked")
    for price in ("120000", "80000"):
        with pytest.raises(HTTPException) as e:
            svc.update("tracked", ACC, body(userPrice=Decimal(price)), updated_by=ADMIN)
        assert e.value.status_code == 409
    assert get_pr("tracked")["user_price"] == Decimal("100000")


def test_manual_paid_then_price_fix_and_revert_still_work(finance_db):
    """Luis marks it paid by hand (PUT status=paid) and later corrects a typo or undoes it: as before."""
    svc = make_service()
    put_pr("m", total="100000")
    svc.update("m", ACC, body(status="paid"), updated_by=ADMIN)
    assert get_pr("m")["payment_status"] == "paid"
    svc.update("m", ACC, body(userPrice=Decimal("105000")), updated_by=ADMIN)
    assert get_pr("m")["paid_amount"] == Decimal("105000")
    svc.update("m", ACC, body(status="pending", userPrice=Decimal("105000")), updated_by=ADMIN)
    assert get_pr("m")["payment_status"] == "pending" and get_pr("m")["paid_amount"] == 0


def test_legacy_approval_pending_with_receipt_can_be_rejected_or_approved_by_editing(finance_db):
    """The 10 real rows waiting today: a receipt on the request itself, no submission."""
    svc = make_service()
    key = "dev/accounts/acc1/users/user_payee/invoices/pr1/a.png"
    put_pr("ok", status="approval_pending", total="100000", images=[key], created_time=1)
    svc.update("ok", ACC, body(status="paid"), updated_by=ADMIN)
    assert get_pr("ok")["payment_status"] == "paid"
    put_pr("no", status="approval_pending", total="100000", images=[key], created_time=2)
    svc.update("no", ACC, body(status="pending"), updated_by=ADMIN)
    assert get_pr("no")["payment_status"] == "pending"


def test_delete_is_blocked_only_for_tracked_money(finance_db):
    svc = make_service()
    put_pr("manual", total="100000")
    svc.update("manual", ACC, body(status="paid"), updated_by=ADMIN)  # paid by hand: paid_amount stored, nothing tracked
    svc.delete("manual", ACC)
    assert svc.repo.get("manual", ACC) is None

    put_pr("plain", total="100000", created_time=2)
    plain = submit(svc, pr_id="plain")
    review(svc, plain["submissionId"], pr_id="plain")  # full payment, no installments, no bank account
    svc.delete("plain", ACC)
    assert svc.repo.get("plain", ACC) is None

    put_pr("tracked", total="100000", allow=True, max_installments=2, created_time=3)
    tracked = submit(svc, pr_id="tracked", amount="30000")
    review(svc, tracked["submissionId"], pr_id="tracked")
    with pytest.raises(HTTPException) as e:
        svc.delete("tracked", ACC)
    assert e.value.status_code == 409 and svc.repo.get("tracked", ACC) is not None
