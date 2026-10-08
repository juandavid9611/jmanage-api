import os
import unittest
from datetime import datetime, timedelta

os.environ.setdefault("AWS_DEFAULT_REGION", "us-west-2")

from services.payment_request_service import PaymentRequestService


class FakePRRepo:
    def __init__(self, items):
        self.items = {i["id"]: i for i in items}
        self.updates = []

    def list_by_status_all_accounts(self, status):
        return [i for i in self.items.values() if i["payment_status"] == status]

    def get(self, pid, account_id):
        i = self.items.get(pid)
        return i if i and i["account_id"] == account_id else None

    def update(self, pid, account_id, updates):
        self.updates.append((pid, updates))
        self.items[pid].update(updates)


class FakeNotifier:
    def __init__(self):
        self.processed = []

    def overdue_payments_processed(self, **kw):
        self.processed.append(kw["account_id"])


class FakeOrderRepo:
    def __init__(self, status="pending"):
        self.order = {"id": "o1", "status": status}
        self.events, self.transitions = [], []

    def append_event(self, oid, account_id, event):
        self.events.append(event["type"])

    def get_by_id(self, oid, account_id):
        return self.order

    def transition_status(self, oid, account_id, old, new, event, restock=None):
        self.transitions.append((old, new))


def pr(i, account, due_delta_days, status="pending", order_id=None):
    due = (datetime.now() + timedelta(days=due_delta_days)).isoformat()
    return {"id": i, "account_id": account, "payment_status": status, "due_date": due, "user_price": 100,
            "concept": "c", "order_id": order_id, "payment_request_to": {"name": "n", "email": "e"}}


class TestOverdue(unittest.TestCase):
    def test_iterates_every_account_with_pending_requests(self):
        repo = FakePRRepo([pr("a", "club1", -1), pr("b", "club2", -2), pr("c", "club2", +5), pr("d", "club3", -1, "paid")])
        notifier = FakeNotifier()
        svc = PaymentRequestService(repo, None, notifier, FakeOrderRepo())
        out = svc.process_overdue_payments()
        self.assertEqual({o["id"] for o in out}, {"a", "b"})
        self.assertEqual(set(notifier.processed), {"club1", "club2"})
        self.assertEqual(repo.items["c"]["payment_status"], "pending")


class TestOrderSync(unittest.TestCase):
    def test_paid_payment_moves_order_to_paid(self):
        orders = FakeOrderRepo("pending")
        svc = PaymentRequestService(FakePRRepo([]), None, FakeNotifier(), orders)
        svc._append_order_event("o1", "acc", "paid")
        self.assertEqual(orders.events, ["payment_paid"])
        self.assertEqual(orders.transitions, [("pending", "paid")])

    def test_paid_payment_does_not_resurrect_cancelled_order(self):
        orders = FakeOrderRepo("cancelled")
        svc = PaymentRequestService(FakePRRepo([]), None, FakeNotifier(), orders)
        svc._append_order_event("o1", "acc", "paid")
        self.assertEqual(orders.transitions, [])

    def test_other_statuses_do_not_change_order(self):
        orders = FakeOrderRepo("pending")
        svc = PaymentRequestService(FakePRRepo([]), None, FakeNotifier(), orders)
        svc._append_order_event("o1", "acc", "overdue")
        self.assertEqual(orders.transitions, [])


class TestCancelForOrder(unittest.TestCase):
    def test_only_open_requests_are_cancelled(self):
        repo = FakePRRepo([pr("a", "acc", 1), pr("b", "acc", 1, "paid")])
        svc = PaymentRequestService(repo, None, FakeNotifier(), FakeOrderRepo())
        self.assertTrue(svc.cancel_for_order("a", "acc"))
        self.assertFalse(svc.cancel_for_order("b", "acc"))
        self.assertFalse(svc.cancel_for_order("a", "other"))
        self.assertEqual(repo.items["a"]["payment_status"].value, "canceled")


if __name__ == "__main__":
    unittest.main()
