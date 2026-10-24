"""PUT guards, legacy rows, the request_approval alias, bulk_create and read defaults (moto)."""
from decimal import Decimal

import pytest
from fastapi import HTTPException

from api.schemas.payments import BulkPutPaymentRequest
from repositories.bank_adjustment_repo_ddb import BankAdjustmentRepo
from tests.finance.installments_support import (
    ACC, ADMIN, PAYEE, WS, FakeNotifier, FakeOrderRepo, get_pr, make_service, put_bank, put_pr, review, submit,
    upload,
)


def body(**kw):
    base = dict(createDate="2026-01-01", dueDate="2999-01-01", concept="Mensualidad", description="d",
                category="monthly", group=WS, paymentRequestTo=[{"id": PAYEE, "name": "Jugador", "email": "j@example.com"}],
                userPrice=Decimal("100000"))
    base.update(kw)
    return BulkPutPaymentRequest(**base)


def code(exc_info):
    return exc_info.value.status_code


# ------------------------------------------------------------------ backward compatible reads
def test_legacy_rows_read_with_the_documented_defaults(finance_db):
    put_pr("paid1", status="paid", total="50000")
    put_pr("overdue1", status="overdue", total="70000", created_time=2)
    put_pr("pending1", status="pending", total="80000", created_time=3)
    svc = make_service()
    paid, overdue, pending = (svc.get(i, ACC) for i in ("paid1", "overdue1", "pending1"))
    assert (paid["allowInstallments"], paid["maxInstallments"], paid["paidAmount"], paid["originalAmount"]) == (False, None, 50000, 50000)
    assert (overdue["paidAmount"], overdue["originalAmount"]) == (0, 70000)
    assert pending["paidAmount"] == 0 and pending["totalAmount"] == Decimal("80000")
    listed = {i["id"]: i for i in svc.list_payment_requests(ACC)}
    assert listed["paid1"]["paidAmount"] == 50000 and "submissions" not in listed["paid1"]
    assert "status_before_review" not in paid and "paid_amount" not in paid


def test_stored_values_win_over_the_defaults(finance_db):
    put_pr("p", status="overdue", total="150", paid="40", original="100", allow=True, max_installments=3)
    got = make_service().get("p", ACC)
    assert (got["paidAmount"], got["originalAmount"], got["allowInstallments"], got["maxInstallments"]) == (40, 100, True, 3)


def test_detail_submissions_visibility(finance_db):
    put_pr(total="100000", allow=True, max_installments=3)
    svc = make_service()
    submit(svc, amount="1000")
    as_admin = svc.get_detail("pr1", ACC, viewer_id=ADMIN, viewer_is_admin=False)  # workspace admin via membership
    as_payee = svc.get_detail("pr1", ACC, viewer_id=PAYEE)
    assert len(as_admin["submissions"]) == 1 and len(as_payee["submissions"]) == 1
    # a member who is neither the payee nor an admin does not even learn that it exists (404, F7)
    assert svc.get_detail("pr1", ACC, viewer_id="stranger") is None
    assert svc.get_detail("pr1", ACC, viewer_id=None) is None
    sub = as_payee["submissions"][0]
    assert set(sub) >= {"id", "amount", "accountId", "fileNames", "status", "submittedAt", "images", "source"}
    assert sub["fileNames"] == ["r1.png"] and sub["images"][0].startswith("https://")
    assert svc.get_detail("pr1", "other_account") is None


# ------------------------------------------------------------------ alias + legacy approval_pending
def test_request_approval_alias_pays_the_full_remaining_balance(finance_db):
    put_pr(total="100000", status="partial", paid="30000", allow=True, max_installments=3)
    svc = make_service()
    upload(svc, "legacy.png")
    assert svc.request_payment_request_approval("pr1", ACC, ["legacy.png"], user_id=PAYEE) == "pr1"
    sub = svc.submissions.list_by_payment_request("pr1", ACC)[0]
    assert sub["amount"] == Decimal("70000")
    with pytest.raises(HTTPException) as e:  # still payee-only (security fix)
        svc.request_payment_request_approval("pr1", ACC, ["legacy.png"], user_id="stranger")
    assert code(e) == 403


def test_request_approval_alias_rejects_other_members(finance_db):
    put_pr()
    svc = make_service()
    upload(svc, "x.png")
    with pytest.raises(HTTPException) as e:
        svc.request_payment_request_approval("pr1", ACC, ["x.png"], user_id="stranger")
    assert code(e) == 403 and get_pr()["payment_status"] == "pending"


def legacy_pr(**kw):
    key = f"dev/accounts/{ACC}/users/{PAYEE}/invoices/pr1/old1.png"
    return put_pr(status="approval_pending", images=[key], **kw)


def test_legacy_approval_pending_shows_a_virtual_submission_and_is_reviewable(finance_db):
    legacy_pr(total="100000")
    put_bank(finance_db)
    svc = make_service(order_repo=FakeOrderRepo())
    detail = svc.get_detail("pr1", ACC, viewer_is_admin=True)
    virtual = detail["submissions"][0]
    assert (virtual["id"], virtual["source"], virtual["status"], virtual["amount"]) == ("legacy", "legacy", "pending_review", 100000)
    assert virtual["fileNames"] == ["old1.png"]
    assert svc.submissions.list_by_payment_request("pr1", ACC) == []  # nothing persisted by reading

    out = review(svc, "legacy", bank="bac_1")
    assert out["decision"] == "approved"
    raw = get_pr()
    assert raw["payment_status"] == "paid" and raw["paid_amount"] == Decimal("100000")
    stored = svc.submissions.list_by_payment_request("pr1", ACC)
    assert [(s["id"], s["source"], s["status"]) for s in stored] == [("sub_legacy_pr1", "legacy", "approved")]
    assert len(BankAdjustmentRepo().table.scan()["Items"]) == 1
    assert review(svc, "legacy")["replayed"] is True  # replaying 'legacy' maps to the persisted submission
    with pytest.raises(HTTPException) as e:
        review(svc, "legacy", approve=False)
    assert code(e) == 409


def test_legacy_reject_restores_the_state_without_status_before_review(finance_db):
    legacy_pr(total="100000", due="2000-01-01T00:00:00")  # past due: it was overdue before
    svc = make_service()
    review(svc, "legacy", approve=False)
    assert get_pr()["payment_status"] == "overdue"


def test_legacy_without_images_is_not_reviewable(finance_db):
    put_pr(status="approval_pending")
    with pytest.raises(HTTPException) as e:
        review(make_service(), "legacy")
    assert code(e) == 404


# ------------------------------------------------------------------ bulk_create
def test_bulk_create_copies_installment_settings_and_sets_amounts(finance_db):
    svc = make_service()
    users = [{"id": "u1", "name": "A", "email": "a@x.com"}, {"id": "u2", "name": "B", "email": "b@x.com"}]
    out = svc.bulk_create(body(paymentRequestTo=users, allowInstallments=True, maxInstallments=3, userPrice=Decimal("90000")), ACC)
    assert len(out) == 2
    for item in out:
        assert (item["allowInstallments"], item["maxInstallments"], item["paidAmount"], item["originalAmount"]) == (True, 3, 0, 90000)
    raw = get_pr(out[0]["id"])
    assert raw["allow_installments"] is True and raw["max_installments"] == 3
    assert raw["paid_amount"] == 0 and raw["original_amount"] == Decimal("90000")


def test_bulk_create_without_installments_and_validation(finance_db):
    svc = make_service()
    plain = svc.bulk_create(body(), ACC)[0]
    assert plain["allowInstallments"] is False and plain["maxInstallments"] is None
    for bad in (dict(allowInstallments=True), dict(allowInstallments=True, maxInstallments=1),
                dict(allowInstallments=True, maxInstallments=1000)):
        with pytest.raises(HTTPException) as e:
            svc.bulk_create(body(**bad), ACC)
        assert code(e) == 422
    for bad_price in (Decimal("-1"), Decimal("1.234")):
        with pytest.raises(HTTPException) as e:
            svc.bulk_create(body(userPrice=bad_price), ACC)
        assert code(e) == 422


# ------------------------------------------------------------------ PUT guards
def test_put_cannot_set_partial_but_the_form_may_send_the_current_status_back(finance_db):
    put_pr(status="pending")
    svc = make_service()
    with pytest.raises(HTTPException) as e:
        svc.update("pr1", ACC, body(status="partial"))
    assert code(e) == 422
    with pytest.raises(HTTPException) as e:
        svc.update("pr1", ACC, body(status="approval_pending"))
    assert code(e) == 422
    put_pr("p2", status="partial", paid="30000", total="100000", created_time=2)
    svc.update("p2", ACC, body(status="partial", concept="Nuevo"))
    assert get_pr("p2")["concept"] == "Nuevo" and get_pr("p2")["payment_status"] == "partial"


def test_put_total_below_paid_is_409(finance_db):
    put_pr(status="partial", paid="60000", total="100000")
    svc = make_service()
    with pytest.raises(HTTPException) as e:
        svc.update("pr1", ACC, body(userPrice=Decimal("59999")))
    assert code(e) == 409
    assert get_pr()["user_price"] == Decimal("100000")
    svc.update("pr1", ACC, body(userPrice=Decimal("60000")))  # exactly what was paid: the request is settled
    assert get_pr()["payment_status"] == "paid" and get_pr()["user_price"] == Decimal("60000")


def test_put_total_change_updates_original_amount_unless_overdue_replaced_it(finance_db):
    put_pr("a", total="100", original="100")
    put_pr("b", total="150", original="100", status="overdue", created_time=2)
    legacy = put_pr("c", total="70", created_time=3)  # no original_amount stored
    svc = make_service()
    svc.update("a", ACC, body(userPrice=Decimal("120")))
    svc.update("b", ACC, body(userPrice=Decimal("160")))
    svc.update("c", ACC, body(userPrice=Decimal("75")))
    assert get_pr("a")["original_amount"] == 120
    assert get_pr("b")["original_amount"] == 100
    assert get_pr("c")["original_amount"] == 75 and legacy["user_price"] == 70


def test_put_manual_paid_records_an_approved_manual_submission_and_no_adjustment(finance_db):
    put_pr(status="overdue", total="100000", status_before_review=None, order_id="ord1")
    orders = FakeOrderRepo()
    svc = make_service(order_repo=orders)
    svc.update("pr1", ACC, body(status="paid"), updated_by=ADMIN)
    raw = get_pr()
    assert raw["payment_status"] == "paid" and raw["paid_amount"] == Decimal("100000")
    subs = svc.submissions.list_by_payment_request("pr1", ACC)
    assert [(s["source"], s["status"], s["amount"], s["reviewed_by"]) for s in subs] == [("manual", "approved", Decimal("100000"), ADMIN)]
    assert BankAdjustmentRepo().table.scan()["Items"] == []
    assert orders.transitions == [("pending", "paid")]  # order event on paid


def test_put_manual_paid_after_abonos_records_only_the_remaining_amount(finance_db):
    put_pr(total="100000", allow=True, max_installments=3)
    svc = make_service()
    a = submit(svc, amount="30000")
    review(svc, a["submissionId"])
    svc.update("pr1", ACC, body(status="paid"), updated_by=ADMIN)
    manual = [s for s in svc.submissions.list_by_payment_request("pr1", ACC) if s["source"] == "manual"]
    assert [s["amount"] for s in manual] == [Decimal("70000")]
    assert get_pr()["paid_amount"] == Decimal("100000")


def test_put_regress_is_blocked_only_for_tracked_money(finance_db):
    # tracked = paid in installments (or credited to a bank account): reverting would desync the ledger
    put_pr("tracked", total="100000", allow=True, max_installments=3, created_time=1)
    svc = make_service()
    sub = submit(svc, pr_id="tracked", amount="100000")
    review(svc, sub["submissionId"], pr_id="tracked")
    assert get_pr("tracked")["payment_status"] == "paid" and get_pr("tracked")["has_reviewed_payments"] is True
    for target in ("pending", "overdue", "canceled"):
        with pytest.raises(HTTPException) as e:
            svc.update("tracked", ACC, body(status=target))
        assert code(e) == 409
    assert get_pr("tracked")["payment_status"] == "paid" and get_pr("tracked")["paid_amount"] == Decimal("100000")

    # a plain full payment (no installments, no bank account) is as revertible as it always was
    put_pr("plain", total="100000", created_time=3)
    plain = submit(svc, pr_id="plain")
    review(svc, plain["submissionId"], pr_id="plain")
    assert get_pr("plain")["payment_status"] == "paid" and "has_reviewed_payments" not in get_pr("plain")
    svc.update("plain", ACC, body(status="pending"))
    assert get_pr("plain")["payment_status"] == "pending" and get_pr("plain")["paid_amount"] == 0

    put_pr("legacy_paid", status="paid", total="100000", created_time=2)  # no submissions at all
    svc.update("legacy_paid", ACC, body(status="pending"))
    assert get_pr("legacy_paid")["payment_status"] == "pending" and get_pr("legacy_paid")["paid_amount"] == 0


def test_admin_edit_while_a_submission_is_waiting_is_a_review_decision(finance_db):
    """The current admin screen approves/rejects a receipt by editing the request: that must keep working."""
    svc = make_service()
    put_pr("approve", total="100000")
    submit(svc, pr_id="approve")
    svc.update("approve", ACC, body(status="paid"), updated_by=ADMIN)
    assert get_pr("approve")["payment_status"] == "paid" and get_pr("approve")["paid_amount"] == Decimal("100000")
    assert [x["status"] for x in svc.submissions.list_by_payment_request("approve", ACC)] == ["approved"]

    put_pr("reject", status="overdue", total="100000", created_time=2)
    submit(svc, pr_id="reject")
    assert get_pr("reject")["payment_status"] == "approval_pending"
    svc.update("reject", ACC, body(status="overdue"), updated_by=ADMIN)
    assert get_pr("reject")["payment_status"] == "overdue" and get_pr("reject").get("paid_amount", 0) == 0  # untouched rows store nothing
    assert [x["status"] for x in svc.submissions.list_by_payment_request("reject", ACC)] == ["rejected"]

    put_pr("cancel", total="100000", created_time=3)
    submit(svc, pr_id="cancel")
    svc.update("cancel", ACC, body(status="canceled"), updated_by=ADMIN)
    assert get_pr("cancel")["payment_status"] == "canceled"
    assert [x["status"] for x in svc.submissions.list_by_payment_request("cancel", ACC)] == ["rejected"]

    put_pr("hand", total="100000", created_time=4)
    submit(svc, pr_id="hand")
    with pytest.raises(HTTPException) as e:  # 'partial' is calculated, never settable by hand
        svc.update("hand", ACC, body(status="partial"), updated_by=ADMIN)
    assert code(e) == 422
    svc.update("hand", ACC, body(status="approval_pending"), updated_by=ADMIN)  # the form echoing the current status
    assert get_pr("hand")["payment_status"] == "approval_pending"
    assert [x["status"] for x in svc.submissions.list_by_payment_request("hand", ACC)] == ["pending_review"]


def test_admin_marks_paid_a_request_with_a_partial_submission_waiting(finance_db):
    put_pr(total="100000", allow=True, max_installments=3)
    svc = make_service()
    submit(svc, amount="30000")
    svc.update("pr1", ACC, body(status="paid"), updated_by=ADMIN)  # approve the 30k, then close the rest by hand
    raw = get_pr()
    assert raw["payment_status"] == "paid" and raw["paid_amount"] == Decimal("100000")
    subs = svc.submissions.list_by_payment_request("pr1", ACC)
    assert sorted((x["source"], x["status"], x["amount"]) for x in subs) == [
        ("manual", "approved", Decimal("70000")), ("player", "approved", Decimal("30000"))]


def test_put_installment_settings(finance_db):
    put_pr(total="100000")
    svc = make_service()
    for bad in (dict(allowInstallments=True), dict(allowInstallments=True, maxInstallments=1)):
        with pytest.raises(HTTPException) as e:
            svc.update("pr1", ACC, body(**bad))
        assert code(e) == 422
    svc.update("pr1", ACC, body(allowInstallments=True, maxInstallments=4))
    assert get_pr()["allow_installments"] is True and get_pr()["max_installments"] == 4
    svc.update("pr1", ACC, body(allowInstallments=False))
    assert get_pr()["allow_installments"] is False and get_pr()["max_installments"] is None


def test_put_without_installment_fields_leaves_them_alone(finance_db):
    put_pr(total="100000", allow=True, max_installments=3)
    make_service().update("pr1", ACC, body(concept="Otro"))
    assert get_pr()["allow_installments"] is True and get_pr()["max_installments"] == 3


def test_put_notifies_with_the_new_partial_status_label(finance_db):
    notifier = FakeNotifier()
    put_pr(total="100000", allow=True, max_installments=3)
    svc = make_service(notifier=notifier)
    a = submit(svc, amount="1000")
    review(svc, a["submissionId"])
    assert notifier.updated[-1]["changes"][0]["new_value"] == "partial"
    from services.notification_orchestator import Notifications
    formatted = Notifications._get_formatted_notification_field(
        object.__new__(Notifications), {"name": "status", "old_value": "approval_pending", "new_value": "partial"})
    assert formatted["new_value"] == "Pago parcial"


def test_order_event_only_when_paid_is_reached_via_review(finance_db):
    put_pr(total="100000", allow=True, max_installments=3, order_id="ord1")
    orders = FakeOrderRepo()
    svc = make_service(order_repo=orders)
    a = submit(svc, amount="30000")
    assert orders.events == ["payment_approval_pending"]
    review(svc, a["submissionId"])
    assert orders.events == ["payment_approval_pending"] and orders.transitions == []
    b = submit(svc, amount="70000", files=("r2.png",))
    review(svc, b["submissionId"])
    assert "payment_paid" in orders.events and orders.transitions == [("pending", "paid")]
