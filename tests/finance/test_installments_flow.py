"""submit_payment / review_payment on moto (real transactions, conditions and GSIs)."""
import threading
from decimal import Decimal

import pytest
from fastapi import HTTPException

from repositories.bank_adjustment_repo_ddb import BankAdjustmentRepo
from tests.finance.installments_support import (
    ACC, ADMIN, PAYEE, WS, FakeAccounts, FakeMembership, FakeNotifier, FakeOrderRepo,
    get_pr, make_service, put_bank, put_pr, review, submit, upload,
)


def code(exc_info):
    return exc_info.value.status_code


# ------------------------------------------------------------------ submit
def test_submit_creates_pending_submission_and_moves_request_under_review(finance_db):
    put_pr(status="overdue", total="100000")
    svc = make_service()
    out = submit(svc)
    assert out["amount"] == 100000 and out["remaining"] == 100000
    raw = get_pr()
    assert raw["payment_status"] == "approval_pending" and raw["status_before_review"] == "overdue"
    assert len(raw["images"]) == 1 and raw["images"][0].endswith("/r1.png")
    subs = svc.submissions.list_by_payment_request("pr1", ACC)
    assert [(s["status"], s["source"], s["amount"]) for s in subs] == [("pending_review", "player", Decimal("100000"))]
    detail = svc.get_detail("pr1", ACC, viewer_is_admin=True)
    assert detail["status"] == "approval_pending" and detail["submissions"][0]["status"] == "pending_review"
    assert detail["submissions"][0]["amount"] == 100000


def test_only_the_payee_can_submit(finance_db):
    put_pr()
    svc = make_service()
    upload(svc, "r1.png")
    with pytest.raises(HTTPException) as e:
        svc.submit_payment("pr1", ACC, user_id="someone_else", amount=None, file_names=["r1.png"])
    assert code(e) == 403
    assert get_pr()["payment_status"] == "pending"


def test_tenant_isolation_is_a_404(finance_db):
    put_pr()
    svc = make_service()
    with pytest.raises(HTTPException) as e:
        svc.submit_payment("pr1", "other_account", user_id=PAYEE, amount=None, file_names=["r1.png"])
    assert code(e) == 404
    with pytest.raises(HTTPException) as e:
        svc.review_payment("pr1", "other_account", reviewer_id=ADMIN, submission_id="sub_x", approve=True)
    assert code(e) == 404


@pytest.mark.parametrize("status", ["paid", "canceled"])
def test_closed_requests_do_not_accept_payments(finance_db, status):
    put_pr(status=status, paid="100000" if status == "paid" else None)
    with pytest.raises(HTTPException) as e:
        submit(make_service())
    assert code(e) == 409


def test_one_submission_under_review_at_a_time(finance_db):
    put_pr(allow=True, max_installments=3)
    svc = make_service()
    submit(svc, amount="1000")
    with pytest.raises(HTTPException) as e:
        submit(svc, amount="1000", files=("r2.png",))
    assert code(e) == 409 and "under review" in e.value.detail
    assert len(svc.submissions.list_by_payment_request("pr1", ACC)) == 1


def test_concurrent_submits_only_one_wins(finance_db):
    put_pr(allow=True, max_installments=3)
    svc = make_service()
    for f in ("a.png", "b.png"):
        upload(svc, f)
    results, barrier = [], threading.Barrier(2)

    def go(name):
        barrier.wait()
        try:
            svc.submit_payment("pr1", ACC, user_id=PAYEE, amount=Decimal("1000"), file_names=[name])
            results.append("ok")
        except HTTPException as exc:
            results.append(exc.status_code)

    threads = [threading.Thread(target=go, args=(n,)) for n in ("a.png", "b.png")]
    [t.start() for t in threads]
    [t.join() for t in threads]
    assert sorted(results, key=str) == [409, "ok"]
    assert len(svc.submissions.list_by_payment_request("pr1", ACC)) == 1


def test_amount_is_forced_to_the_balance_without_installments(finance_db):
    put_pr(total="100000")  # legacy-shaped row: no installment fields at all
    svc = make_service()
    out = submit(svc, amount="5")
    assert out["amount"] == 100000


def test_overpay_and_underpay_boundaries(finance_db):
    put_pr(total="100000", allow=True, max_installments=5)
    svc = make_service()
    for bad in ("0", "-5", "100000.01", "abc", "1.234"):
        with pytest.raises(HTTPException) as e:
            submit(svc, amount=bad)
        assert code(e) == 422, bad
    out = submit(svc, amount="100000")  # exactly the balance is fine
    assert out["amount"] == 100000


def test_decimal_amounts_are_exact(finance_db):
    put_pr(total="0.30", allow=True, max_installments=5)
    svc = make_service()
    submit(svc, amount="0.1")
    sub = svc.submissions.list_by_payment_request("pr1", ACC)[0]
    review(svc, sub["id"])
    submit(svc, amount="0.2", files=("r2.png",))
    sub2 = [s for s in svc.submissions.list_by_payment_request("pr1", ACC) if s["status"] == "pending_review"][0]
    review(svc, sub2["id"])
    assert get_pr()["paid_amount"] == Decimal("0.30") and get_pr()["payment_status"] == "paid"


def test_max_installments_and_the_last_one_must_cover_the_balance(finance_db):
    put_pr(total="90000", allow=True, max_installments=3)
    svc = make_service()
    first = submit(svc, amount="30000")
    review(svc, first["submissionId"])
    second = submit(svc, amount="30000", files=("r2.png",))
    review(svc, second["submissionId"])
    # third (= last allowed) must be the whole remaining 30000
    with pytest.raises(HTTPException) as e:
        submit(svc, amount="10000", files=("r3.png",))
    assert code(e) == 422 and "last allowed" in e.value.detail
    third = submit(svc, amount="30000", files=("r3.png",))
    review(svc, third["submissionId"])
    assert get_pr()["payment_status"] == "paid"


def test_rejected_submissions_do_not_count_against_the_maximum(finance_db):
    put_pr(total="100000", allow=True, max_installments=2)
    svc = make_service()
    first = submit(svc, amount="10000")
    review(svc, first["submissionId"], approve=False)
    assert get_pr()["payment_status"] == "pending"
    again = submit(svc, amount="10000", files=("r2.png",))
    review(svc, again["submissionId"])
    with pytest.raises(HTTPException) as e:  # 2 non-rejected would be needed... the last one must pay it all
        submit(svc, amount="1000", files=("r3.png",))
    assert code(e) == 422


def test_max_reached_is_a_409(finance_db):
    put_pr(total="100000", allow=True, max_installments=2)
    svc = make_service()
    a = submit(svc, amount="10000")
    review(svc, a["submissionId"])
    b = submit(svc, amount="90000", files=("r2.png",))  # last: whole balance
    review(svc, b["submissionId"])
    assert get_pr()["payment_status"] == "paid"
    with pytest.raises(HTTPException) as e:
        submit(svc, amount="1000", files=("r3.png",))
    assert code(e) == 409


def test_turning_installments_off_forces_the_remaining_balance(finance_db):
    put_pr(total="100000", allow=False, paid="40000", status="partial")
    out = submit(make_service(), amount="1")
    assert out["amount"] == 60000


def test_file_rules(finance_db):
    put_pr()
    svc = make_service()
    for bad in ([], ["../x.png"], ["a/b.png"], ["a.png", "a.png"], ["x" * 200]):
        with pytest.raises(HTTPException) as e:
            submit(svc, files=bad, upload_files=False)
        assert code(e) == 422
    with pytest.raises(HTTPException) as e:  # never uploaded
        submit(svc, files=("ghost.png",), upload_files=False)
    assert code(e) == 422 and "ghost.png" in e.value.detail
    assert get_pr()["payment_status"] == "pending"


def test_receipts_are_appended_never_overwritten(finance_db):
    put_pr(total="100000", allow=True, max_installments=4)
    svc = make_service()
    a = submit(svc, amount="10000", files=("r1.png",))
    # a file name used by a payment in review/approved cannot be reused
    review(svc, a["submissionId"])
    upload(svc, "r1.png")
    with pytest.raises(HTTPException) as e:
        svc.submit_payment("pr1", ACC, user_id=PAYEE, amount=Decimal("1000"), file_names=["r1.png"])
    assert code(e) == 422
    submit(svc, amount="10000", files=("r2.png",))
    assert [k.rsplit("/", 1)[-1] for k in get_pr()["images"]] == ["r1.png", "r2.png"]


# ------------------------------------------------------------------ account link
def test_account_validation_at_submit(finance_db):
    put_pr()
    put_bank(finance_db, "bac_ok")
    put_bank(finance_db, "bac_off", active=False)
    put_bank(finance_db, "bac_alien", account_id="other")
    svc = make_service()
    for bad in ("bac_missing", "bac_off", "bac_alien"):
        with pytest.raises(HTTPException) as e:
            submit(svc, bank=bad)
        assert code(e) == 422
    assert get_pr()["payment_status"] == "pending"
    submit(svc, bank="bac_ok")
    assert svc.submissions.list_by_payment_request("pr1", ACC)[0]["bank_account_id"] == "bac_ok"


def test_tournament_account_rejects_account_id_but_installments_work(finance_db):
    put_pr(total="100000", allow=True, max_installments=3)
    put_bank(finance_db)
    svc = make_service(accounts=FakeAccounts("tournament"))
    with pytest.raises(HTTPException) as e:
        submit(svc, amount="1000", bank="bac_1")
    assert code(e) == 422
    out = submit(svc, amount="1000")
    review(svc, out["submissionId"])
    assert get_pr()["payment_status"] == "partial"
    assert BankAdjustmentRepo().table.scan()["Items"] == []


# ------------------------------------------------------------------ review
def test_approve_full_amount_pays_the_request_and_creates_the_adjustment_atomically(finance_db):
    put_pr(total="100000", order_id="ord1")
    put_bank(finance_db)
    orders, notifier = FakeOrderRepo(), FakeNotifier()
    svc = make_service(order_repo=orders, notifier=notifier)
    sub = submit(svc, bank="bac_1")
    res = review(svc, sub["submissionId"])
    raw = get_pr()
    assert raw["payment_status"] == "paid" and raw["paid_amount"] == Decimal("100000")
    assert "status_before_review" not in raw
    adj = BankAdjustmentRepo().get(f"adj_pr_pr1_{sub['submissionId']}", ACC)
    assert adj["amount"] == Decimal("100000") and adj["bank_account_id"] == "bac_1"
    assert adj["source"] == "payment_request" and adj["payment_request_id"] == "pr1"
    assert adj["workspace_id"] == WS and len(adj["note"]) <= 280
    assert res["decision"] == "approved" and res["replayed"] is False
    assert res["paymentRequest"]["status"] == "paid" and res["paymentRequest"]["paidAmount"] == 100000
    assert orders.transitions == [("pending", "paid")]
    assert notifier.updated[-1]["changes"][0]["new_value"] == "paid"


def test_partial_then_overdue_stays_overdue_and_does_not_move_the_order(finance_db):
    put_pr(total="100000", allow=True, max_installments=3, order_id="ord1", status="overdue")
    orders = FakeOrderRepo()
    svc = make_service(order_repo=orders)
    a = submit(svc, amount="30000")
    review(svc, a["submissionId"])
    raw = get_pr()
    assert raw["payment_status"] == "overdue" and raw["paid_amount"] == Decimal("30000")
    assert orders.transitions == []  # abonos never move the order


def test_partial_status_after_an_abono_on_a_pending_request(finance_db):
    put_pr(total="100000", allow=True, max_installments=3, order_id="ord1")
    orders = FakeOrderRepo()
    svc = make_service(order_repo=orders)
    a = submit(svc, amount="30000")
    review(svc, a["submissionId"])
    assert get_pr()["payment_status"] == "partial"
    assert "payment_paid" not in orders.events and orders.transitions == []
    b = submit(svc, amount="70000", files=("r2.png",))
    review(svc, b["submissionId"])
    assert get_pr()["payment_status"] == "paid" and orders.transitions == [("pending", "paid")]


def test_reject_restores_the_previous_status_and_notifies(finance_db):
    put_pr(status="overdue", total="100000")
    notifier = FakeNotifier()
    svc = make_service(notifier=notifier)
    sub = submit(svc)
    notifier.updated.clear()
    res = review(svc, sub["submissionId"], approve=False)
    raw = get_pr()
    assert raw["payment_status"] == "overdue" and "status_before_review" not in raw
    assert raw.get("paid_amount", Decimal("0")) == 0
    assert res["decision"] == "rejected"
    assert len(notifier.updated) == 1 and notifier.updated[0]["email"] == "j@example.com"
    assert BankAdjustmentRepo().table.scan()["Items"] == []
    # the player can try again
    submit(svc, files=("r2.png",))
    assert get_pr()["payment_status"] == "approval_pending"


def test_replay_same_decision_is_idempotent_and_the_opposite_is_409(finance_db):
    put_pr(total="100000", allow=True, max_installments=3)
    put_bank(finance_db)
    svc = make_service()
    sub = submit(svc, amount="40000", bank="bac_1")
    review(svc, sub["submissionId"])
    again = review(svc, sub["submissionId"])
    assert again["replayed"] is True and again["paymentRequest"]["paidAmount"] == 40000
    assert get_pr()["paid_amount"] == Decimal("40000")
    assert len(BankAdjustmentRepo().table.scan()["Items"]) == 1
    with pytest.raises(HTTPException) as e:
        review(svc, sub["submissionId"], approve=False)
    assert code(e) == 409
    assert get_pr()["paid_amount"] == Decimal("40000")


def test_reject_replay_and_opposite(finance_db):
    put_pr()
    svc = make_service()
    sub = submit(svc)
    review(svc, sub["submissionId"], approve=False)
    assert review(svc, sub["submissionId"], approve=False)["replayed"] is True
    with pytest.raises(HTTPException) as e:
        review(svc, sub["submissionId"], approve=True)
    assert code(e) == 409
    assert get_pr()["payment_status"] == "pending"


def test_two_concurrent_reviews_of_the_same_submission_count_once(finance_db):
    put_pr(total="100000", allow=True, max_installments=3)
    put_bank(finance_db)
    svc = make_service()
    sub = submit(svc, amount="40000", bank="bac_1")
    outcomes, barrier = [], threading.Barrier(2)

    def go():
        barrier.wait()
        try:
            outcomes.append(review(svc, sub["submissionId"])["replayed"])
        except HTTPException as exc:
            outcomes.append(exc.status_code)

    threads = [threading.Thread(target=go) for _ in range(2)]
    [t.start() for t in threads]
    [t.join() for t in threads]
    assert sorted(outcomes, key=str) == [False, True]  # one did the work, the other saw it done
    assert get_pr()["paid_amount"] == Decimal("40000")
    assert len(BankAdjustmentRepo().table.scan()["Items"]) == 1


def test_stale_second_reviewer_conflicts_without_double_counting(finance_db, monkeypatch):
    """The second admin read request and submission before the first approval landed: its transaction fails."""
    put_pr(total="100000", allow=True, max_installments=3)
    svc = make_service()
    sub = submit(svc, amount="40000")
    stale_pr = svc.repo.get("pr1", ACC)
    stale_sub = svc.submissions.get(sub["submissionId"], ACC)
    review(svc, sub["submissionId"])  # the first admin wins

    real_pr_get, real_sub_get = svc.repo.get, svc.submissions.get
    served = {"pr": False, "sub": False}

    def pr_get(*args):
        if not served["pr"]:
            served["pr"] = True
            return dict(stale_pr)
        return real_pr_get(*args)

    def sub_get(*args):
        if not served["sub"]:
            served["sub"] = True
            return dict(stale_sub)
        return real_sub_get(*args)

    monkeypatch.setattr(svc.repo, "get", pr_get)
    monkeypatch.setattr(svc.submissions, "get", sub_get)
    out = review(svc, sub["submissionId"])
    assert out["replayed"] is True  # the transaction conflicted, the re-read showed the same decision
    assert get_pr()["paid_amount"] == Decimal("40000")

    # same stale view but the opposite decision: 409
    served.update(pr=False, sub=False)
    with pytest.raises(HTTPException) as e:
        review(svc, sub["submissionId"], approve=False)
    assert code(e) == 409


def test_review_when_the_total_dropped_below_the_paid_plus_abono_is_a_409(finance_db):
    put_pr(total="100000", allow=True, max_installments=3)
    svc = make_service()
    sub = submit(svc, amount="90000")
    from repositories.ddb_session import payment_request_table
    payment_request_table().update_item(Key={"id": "pr1"}, UpdateExpression="SET user_price = :p",
                                        ExpressionAttributeValues={":p": Decimal("50000")})
    with pytest.raises(HTTPException) as e:
        review(svc, sub["submissionId"])
    assert code(e) == 409
    review(svc, sub["submissionId"], approve=False)  # rejecting is still possible
    assert get_pr()["payment_status"] == "pending"


def test_submission_must_belong_to_the_payment_request(finance_db):
    put_pr("pr1")
    put_pr("pr2", user=PAYEE)
    svc = make_service()
    sub = submit(svc, pr_id="pr2")
    with pytest.raises(HTTPException) as e:
        review(svc, sub["submissionId"], pr_id="pr1")
    assert code(e) == 404
    with pytest.raises(HTTPException) as e:
        review(svc, "sub_nope")
    assert code(e) == 404


def test_cross_workspace_account_is_403_before_anything_is_written(finance_db):
    put_pr(total="100000")
    put_bank(finance_db, "bac_other_ws", workspace_id="ws_other")
    svc = make_service()  # ADMIN is admin of WS only
    sub = submit(svc, bank="bac_other_ws")
    with pytest.raises(HTTPException) as e:
        review(svc, sub["submissionId"])
    assert code(e) == 403
    assert get_pr()["payment_status"] == "approval_pending" and "paid_amount" not in get_pr()
    assert svc.submissions.get(sub["submissionId"], ACC)["status"] == "pending_review"
    assert BankAdjustmentRepo().table.scan()["Items"] == []

    svc2 = make_service(membership=FakeMembership({(ADMIN, WS): "admin", (ADMIN, "ws_other"): "admin"}))
    review(svc2, sub["submissionId"])  # admin of both workspaces: allowed
    assert len(BankAdjustmentRepo().table.scan()["Items"]) == 1


def test_admin_can_choose_or_override_the_account_at_review(finance_db):
    put_pr(total="100000")
    put_bank(finance_db, "bac_1")
    put_bank(finance_db, "bac_2")
    svc = make_service()
    sub = submit(svc, bank="bac_1")
    review(svc, sub["submissionId"], bank="bac_2")
    adj = BankAdjustmentRepo().get(f"adj_pr_pr1_{sub['submissionId']}", ACC)
    assert adj["bank_account_id"] == "bac_2"
    assert svc.submissions.get(sub["submissionId"], ACC)["bank_account_id"] == "bac_2"


def test_approving_without_any_account_creates_no_adjustment(finance_db):
    put_pr(total="100000")
    svc = make_service()
    sub = submit(svc)
    review(svc, sub["submissionId"])
    assert get_pr()["payment_status"] == "paid"
    assert BankAdjustmentRepo().table.scan()["Items"] == []


def test_inactive_or_missing_account_at_review_is_422_and_writes_nothing(finance_db):
    put_pr(total="100000")
    put_bank(finance_db, "bac_1")
    svc = make_service()
    sub = submit(svc, bank="bac_1")
    from repositories.ddb_session import bank_account_table
    bank_account_table().update_item(Key={"id": "bac_1"}, UpdateExpression="SET is_active = :f",
                                     ExpressionAttributeValues={":f": False})
    with pytest.raises(HTTPException) as e:
        review(svc, sub["submissionId"])
    assert code(e) == 422
    assert get_pr()["payment_status"] == "approval_pending"
    review(svc, sub["submissionId"], approve=False)  # reject needs no account


def test_a_failing_adjustment_rolls_back_submission_and_counter(finance_db):
    put_pr(total="100000")
    put_bank(finance_db)
    svc = make_service()
    sub = submit(svc, bank="bac_1")
    # a stray adjustment already holds the deterministic id: the whole transaction must fail
    BankAdjustmentRepo().create({"id": f"adj_pr_pr1_{sub['submissionId']}", "account_id": ACC,
                                 "bank_account_id": "bac_1", "amount": Decimal("1")})
    with pytest.raises(HTTPException) as e:
        review(svc, sub["submissionId"])
    assert code(e) == 409
    assert svc.submissions.get(sub["submissionId"], ACC)["status"] == "pending_review"
    assert get_pr()["payment_status"] == "approval_pending" and "paid_amount" not in get_pr()


def test_notifier_failures_never_block_the_money_path(finance_db):
    class Boom(FakeNotifier):
        def payment_updated(self, **kw):
            raise RuntimeError("courier down")

    put_pr(total="100000")
    svc = make_service(notifier=Boom())
    sub = submit(svc)
    review(svc, sub["submissionId"])
    assert get_pr()["payment_status"] == "paid"
