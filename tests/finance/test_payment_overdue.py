"""The overdue job over pending + partial requests (moto: status GSI, conditional writes)."""
from decimal import Decimal

from tests.finance.installments_support import (
    ACC, FakeNotifier, FakeOrderRepo, get_pr, make_service, put_pr,
)

PAST = "2000-01-01T00:00:00"


def test_pending_past_due_becomes_overdue_and_stores_original_amount(finance_db):
    put_pr("a", total="100000", overdue_price="120000", due=PAST)
    notifier = FakeNotifier()
    out = make_service(notifier=notifier).process_overdue_payments()
    raw = get_pr("a")
    assert raw["payment_status"] == "overdue" and raw["user_price"] == Decimal("120000")
    assert raw["original_amount"] == Decimal("100000")
    assert out == [{"id": "a", "concept": "Mensualidad", "user_price": 120000, "to_name": "Jugador", "to_email": "j@example.com"}]
    assert notifier.overdue[0]["account_id"] == ACC and notifier.overdue[0]["pending_count"] == 1


def test_original_amount_is_preserved_when_already_stored(finance_db):
    put_pr("a", total="100000", overdue_price="120000", due=PAST, original="90000")
    make_service().process_overdue_payments()
    assert get_pr("a")["original_amount"] == Decimal("90000")


def test_no_overdue_price_keeps_the_price(finance_db):
    put_pr("a", total="100000", due=PAST)
    make_service().process_overdue_payments()
    raw = get_pr("a")
    assert raw["user_price"] == Decimal("100000") and raw["original_amount"] == Decimal("100000")


def test_partial_past_due_becomes_overdue_keeping_paid_amount(finance_db):
    put_pr("p", status="partial", total="100000", paid="40000", overdue_price="130000", due=PAST)
    out = make_service().process_overdue_payments()
    raw = get_pr("p")
    assert raw["payment_status"] == "overdue" and raw["paid_amount"] == Decimal("40000")
    assert raw["user_price"] == Decimal("130000") and raw["original_amount"] == Decimal("100000")
    assert [o["id"] for o in out] == ["p"]


def test_partial_already_covered_by_the_new_total_becomes_paid(finance_db):
    put_pr("p", status="partial", total="100000", paid="50000", overdue_price="40000", due=PAST, order_id="ord1")
    orders = FakeOrderRepo()
    out = make_service(order_repo=orders).process_overdue_payments()
    assert get_pr("p")["payment_status"] == "paid" and out == []
    assert orders.transitions == [("pending", "paid")]


def test_not_yet_due_and_other_statuses_are_untouched(finance_db):
    put_pr("future", due="2999-01-01T00:00:00", created_time=1)
    put_pr("future_partial", status="partial", paid="1", due="2999-01-01T00:00:00", created_time=2)
    put_pr("paid", status="paid", due=PAST, created_time=3)
    put_pr("review", status="approval_pending", due=PAST, created_time=4)
    put_pr("already", status="overdue", due=PAST, created_time=5)
    assert make_service().process_overdue_payments() == []
    assert [get_pr(i)["payment_status"] for i in ("future", "future_partial", "paid", "review", "already")] == [
        "pending", "partial", "paid", "approval_pending", "overdue"]


def test_second_run_changes_nothing(finance_db):
    put_pr("a", total="100000", overdue_price="120000", due=PAST, created_time=1)
    put_pr("p", status="partial", total="100000", paid="40000", overdue_price="130000", due=PAST, created_time=2)
    svc = make_service()
    assert len(svc.process_overdue_payments()) == 2
    snapshot = (get_pr("a"), get_pr("p"))
    assert svc.process_overdue_payments() == []
    assert (get_pr("a"), get_pr("p")) == snapshot


def test_a_request_that_went_under_review_meanwhile_is_left_alone(finance_db, monkeypatch):
    put_pr("a", total="100000", due=PAST)
    svc = make_service()
    stale = svc.repo.list_by_status_all_accounts("pending")
    # a player submits after the job read its list
    from repositories.ddb_session import payment_request_table
    payment_request_table().update_item(Key={"id": "a"}, UpdateExpression="SET payment_status = :s",
                                        ExpressionAttributeValues={":s": "approval_pending"})
    monkeypatch.setattr(svc.repo, "list_by_status_all_accounts", lambda status: stale if status == "pending" else [])
    assert svc.process_overdue_payments() == []
    assert get_pr("a")["payment_status"] == "approval_pending"


def test_pagination_of_the_status_index_is_followed(finance_db):
    for i in range(30):
        put_pr(f"p{i:02d}", due=PAST, created_time=i + 1)
    svc = make_service()
    original_query = svc.repo._table.query
    pages = {"n": 0}

    def small_pages(**kw):
        pages["n"] += 1
        return original_query(Limit=7, **kw)

    svc.repo._table.query = small_pages
    assert len(svc.process_overdue_payments()) == 30
    assert pages["n"] >= 5
