import os
import unittest
from decimal import Decimal

os.environ.setdefault("AWS_DEFAULT_REGION", "us-west-2")
os.environ.setdefault("AWS_ACCESS_KEY_ID", "x")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "x")

import boto3
from botocore.stub import Stubber

from api.schemas.orders import OrderCreate, OrderUpdate
from repositories.order_repo_ddb import (
    OrderRepo, StatusConflictError, StockConflictError, build_create_transaction,
    build_status_transaction, build_stock_decrement, build_stock_restore,
)
from services.order_rules import ShopError
from services.order_service import OrderService


class TestTransactionBuilders(unittest.TestCase):
    def test_decrement_is_conditional_and_bumps_total_sold(self):
        u = build_stock_decrement("Prod", "p1", "acc", 3)["Update"]
        self.assertIn("available >= :q", u["ConditionExpression"])
        self.assertIn("account_id = :acc", u["ConditionExpression"])
        self.assertIn("publish = :pub", u["ConditionExpression"])
        self.assertEqual(u["ExpressionAttributeValues"][":negq"], -3)
        self.assertIn("total_sold :q", u["UpdateExpression"])
        self.assertEqual(u["Key"], {"pk": "PRODUCT#p1", "sk": "PRODUCT"})

    def test_restore_reverses(self):
        u = build_stock_restore("Prod", "p1", "acc", 3)["Update"]
        self.assertIn("ADD available :q", u["UpdateExpression"])
        self.assertIn("total_sold :negq", u["UpdateExpression"])

    def test_create_transaction_has_one_put_and_one_update_per_product(self):
        tx = build_create_transaction("Ord", "Prod", {"id": "o1"}, "acc", {"a": 1, "b": 2})
        self.assertEqual([next(iter(t)) for t in tx], ["Put", "Update", "Update"])
        self.assertEqual(tx[0]["Put"]["ConditionExpression"], "attribute_not_exists(id)")

    def test_requests_are_valid_for_the_dynamodb_api_model(self):
        """Run the built items through boto3 parameter validation/serialisation (no network)."""
        client = boto3.resource("dynamodb").meta.client
        event = {"type": "order_status_changed", "title": "t", "time": "now"}
        order = {"id": "o1", "total_amount": Decimal("10.50"), "items": [{"id": "a", "quantity": 1}]}
        txs = [
            build_create_transaction("Ord", "Prod", order, "acc", {"a": 1}),
            build_status_transaction("Ord", "Prod", "o1", "acc", "paid", "refunded", event, {"a": 1}),
        ]
        with Stubber(client) as stub:
            for tx in txs:
                stub.add_response("transact_write_items", {}, {"TransactItems": _Any()})
                client.transact_write_items(TransactItems=tx)
            stub.assert_no_pending_responses()


class _Any:
    def __eq__(self, other):
        return True


class FakeOrderRepo:
    def __init__(self):
        self.orders = {}
        self.created = []
        self.transitions = []
        self.events = []
        self.fail_stock = False
        self.alive = {"p1"}

    @staticmethod
    def new_order_number():
        return "#T-1"

    def create_with_stock(self, item, account_id, quantities):
        if self.fail_stock:
            raise StockConflictError("x")
        self.created.append((item, quantities))
        self.orders[item["id"]] = dict(item)

    def get_by_id(self, oid, account_id):
        o = self.orders.get(oid)
        return o if o and o["account_id"] == account_id else None

    def existing_product_ids(self, account_id, ids):
        return {i for i in ids if i in self.alive}

    def transition_status(self, oid, account_id, old, new, event, restock=None):
        o = self.orders[oid]
        if o["status"] != old:
            raise StatusConflictError()
        o["status"] = new
        self.transitions.append((old, new, restock))

    def set_payment_request_id(self, oid, account_id, prid):
        self.orders[oid]["payment_request_id"] = prid

    def append_event(self, oid, account_id, event):
        self.events.append(event["type"])

    def set_delivery(self, *a):
        pass

    def list_all(self, account_id, workspace_id=None, customer_id=None):
        return [o for o in self.orders.values() if customer_id in (None, o["customer"]["id"])]


class FakeProducts:
    def __init__(self, items):
        self.items = items

    def get_by_id(self, pid, account_id):
        return self.items.get(pid)


class FakePR:
    def __init__(self, fail=False):
        self.fail = fail
        self.created = []
        self.cancelled = []

    def bulk_create(self, bulk, account_id):
        if self.fail:
            raise RuntimeError("boom")
        self.created.append(bulk)
        return [{"id": "pr1"}]

    def cancel_for_order(self, prid, account_id):
        self.cancelled.append(prid)


class FakeNotifier:
    def __init__(self, fail=False):
        self.fail = fail
        self.calls = 0

    def order_created(self, **kw):
        self.calls += 1
        if self.fail:
            raise RuntimeError("courier down")

    def order_status_changed(self, **kw):
        pass


class FakeMembership:
    def get_user_workspaces(self, user_id, account_id):
        return ["w1"]


class FakeS3:
    def get_s3_public_url(self, key):
        return "https://img/" + key


def product(**kw):
    base = {"id": "p1", "name": "Gorra", "price": Decimal("100.50"), "price_sale": Decimal("80.25"),
            "available": 5, "publish": "published", "colors": ["red"], "sizes": ["M"], "images": ["k/a.png"]}
    base.update(kw)
    return base


BODY = {
    "workspaceId": "w1",
    "items": [{"productId": "p1", "quantity": 2, "color": "red", "size": "M"}],
    "shipping": 10, "discount": 0,
    "customer": {"id": "someone-else", "name": "Ana", "email": "ana@x.test", "phoneNumber": "1"},
    "shippingAddress": {"fullAddress": "x", "addressType": "Home", "company": ""},
    "payment": {"payment": "cash"},
    "subtotal": 1, "totalAmount": 1,
}


def make(products=None, pr_fail=False, notifier_fail=False):
    repo = FakeOrderRepo()
    pr = FakePR(pr_fail)
    notifier = FakeNotifier(notifier_fail)
    svc = OrderService(repo, pr, notifier, FakeProducts({"p1": product()} if products is None else products), FakeMembership(), FakeS3())
    return svc, repo, pr, notifier


def body(**kw):
    return OrderCreate.model_validate({**BODY, **kw})


class TestCreateOrder(unittest.TestCase):
    def test_server_computes_totals_and_binds_customer(self):
        svc, repo, pr, _ = make()
        order = svc.create_order(body(), "acc", "sub-1")
        self.assertEqual(order.subtotal, 160.5)  # 80.25 live price * 2, client value ignored
        self.assertEqual(order.total_amount, 170.5)
        self.assertEqual(order.total_quantity, 2)
        self.assertEqual(order.customer.id, "sub-1")
        self.assertEqual(order.items[0].price, 80.25)
        self.assertEqual(order.items[0].colors, ["red"])
        self.assertEqual(order.payment_request_id, "pr1")
        self.assertEqual(repo.created[0][1], {"p1": 2})
        self.assertEqual(pr.created[0].userPrice, Decimal("170.50"))

    def test_token_email_wins(self):
        svc, *_ = make()
        order = svc.create_order(body(), "acc", "sub-1", token_email="real@x.test")
        self.assertEqual(order.customer.email, "real@x.test")

    def test_workspace_membership_required(self):
        svc, *_ = make()
        with self.assertRaises(ShopError) as cm:
            svc.create_order(body(workspaceId="other"), "acc", "sub-1")
        self.assertEqual(cm.exception.kind, "forbidden")

    def test_unpublished_or_missing_product(self):
        svc, *_ = make({"p1": product(publish="draft")})
        with self.assertRaises(ShopError) as cm:
            svc.create_order(body(), "acc", "s")
        self.assertEqual(cm.exception.kind, "not_found")
        svc, *_ = make({})
        with self.assertRaises(ShopError):
            svc.create_order(body(), "acc", "s")

    def test_quantity_above_available_rejected_including_split_lines(self):
        svc, *_ = make({"p1": product(available=3)})
        items = [{"productId": "p1", "quantity": 2, "color": "red", "size": "M"}] * 2
        with self.assertRaises(ShopError) as cm:
            svc.create_order(body(items=items), "acc", "s")
        self.assertEqual(cm.exception.kind, "out_of_stock")

    def test_stock_race_maps_to_out_of_stock(self):
        svc, repo, *_ = make()
        repo.fail_stock = True
        with self.assertRaises(ShopError) as cm:
            svc.create_order(body(), "acc", "s")
        self.assertEqual(cm.exception.kind, "out_of_stock")

    def test_invalid_color_or_size(self):
        svc, *_ = make()
        with self.assertRaises(ShopError):
            svc.create_order(body(items=[{"productId": "p1", "quantity": 1, "color": "blue", "size": "M"}]), "acc", "s")
        with self.assertRaises(ShopError):
            svc.create_order(body(items=[{"productId": "p1", "quantity": 1, "color": "red"}]), "acc", "s")

    def test_payment_request_failure_cancels_and_restocks(self):
        svc, repo, *_ = make(pr_fail=True)
        with self.assertRaises(ShopError) as cm:
            svc.create_order(body(), "acc", "s")
        self.assertEqual(cm.exception.kind, "payment_failed")
        self.assertEqual(repo.transitions, [("pending", "cancelled", {"p1": 2})])

    def test_notifier_failure_does_not_fail_the_order(self):
        svc, repo, _, notifier = make(notifier_fail=True)
        order = svc.create_order(body(), "acc", "s")
        self.assertEqual(notifier.calls, 1)
        self.assertEqual(order.status, "pending")


class TestOrderLifecycle(unittest.TestCase):
    def setUp(self):
        self.svc, self.repo, self.pr, _ = make()
        self.order = self.svc.create_order(body(), "acc", "sub-1")

    def test_invalid_transition_rejected(self):
        with self.assertRaises(ShopError) as cm:
            self.svc.update_order(self.order.id, "acc", OrderUpdate(status="completed"))
        self.assertEqual(cm.exception.kind, "conflict")

    def test_paid_then_refund_restocks(self):
        self.svc.update_order(self.order.id, "acc", OrderUpdate(status="paid"))
        self.svc.update_order(self.order.id, "acc", OrderUpdate(status="refunded"))
        self.assertEqual(self.repo.transitions[-1], ("paid", "refunded", {"p1": 2}))

    def test_restock_skips_deleted_products(self):
        self.repo.alive = set()
        self.svc.update_order(self.order.id, "acc", OrderUpdate(status="cancelled"))
        self.assertEqual(self.repo.transitions[-1], ("pending", "cancelled", {}))

    def test_delete_is_soft_cancel_with_restock_and_payment_cancel(self):
        self.assertTrue(self.svc.delete_order(self.order.id, "acc"))
        self.assertEqual(self.repo.transitions[-1], ("pending", "cancelled", {"p1": 2}))
        self.assertEqual(self.pr.cancelled, ["pr1"])
        self.assertTrue(self.svc.delete_order(self.order.id, "acc"))  # idempotent
        self.assertEqual(len(self.repo.transitions), 1)
        self.assertFalse(self.svc.delete_order("nope", "acc"))

    def test_visibility(self):
        self.assertIsNotNone(self.svc.get_order(self.order.id, "acc", user_id="sub-1", is_admin=False))
        self.assertIsNone(self.svc.get_order(self.order.id, "acc", user_id="sub-2", is_admin=False))
        self.assertIsNotNone(self.svc.get_order(self.order.id, "acc", user_id="sub-2", is_admin=True))
        self.assertEqual(len(self.svc.list_orders("acc", user_id="sub-2", is_admin=False)), 0)
        self.assertEqual(len(self.svc.list_orders("acc", user_id="sub-1", is_admin=False)), 1)
        self.assertEqual(len(self.svc.list_orders("acc", user_id="sub-2", is_admin=True)), 1)


if __name__ == "__main__":
    unittest.main()
