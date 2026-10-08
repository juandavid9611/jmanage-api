import os
import unittest
from decimal import Decimal

os.environ.setdefault("AWS_DEFAULT_REGION", "us-west-2")

from api.schemas.products import ProductUpdate
from repositories.product_repo_ddb import ProductRepo, _build_gsi_attrs
from repositories.s3_adapter import key_from_public_url, public_url_for
from services.product_service import ProductService
from services.search_token import InvalidTokenError


class FakeS3:
    class _KB:
        @staticmethod
        def product_image(account, product, name):
            return f"dev/accounts/{account}/products/{product}/{name}"

    _kb = _KB()

    def __init__(self):
        self.deleted = []

    def get_s3_public_url(self, key):
        return public_url_for("b", "us-west-2", key)

    def key_from_url(self, value):
        return key_from_public_url("b", value)

    def delete_file(self, key):
        self.deleted.append(key)


class FakeRepo:
    def __init__(self, items):
        self.items = {i["id"]: i for i in items}
        self.updates = []
        self.appended = []

    def list_all(self, account_id):
        return [i for i in self.items.values() if i["account_id"] == account_id]

    def get_by_id(self, pid, account_id):
        i = self.items.get(pid)
        return i if i and i["account_id"] == account_id else None

    def update(self, pid, account_id, patch):
        self.updates.append(patch)
        self.items[pid] = {**self.items[pid], **{k: v for k, v in patch.items() if v is not None}}
        return self.items[pid]

    def append_images(self, pid, account_id, keys):
        self.appended.append(keys)
        return True


def prod(i, name, price, sale=None, publish="published", category="Hats", sold=0, tags=(), **kw):
    return {
        "id": f"p{i}", "account_id": "acc", "name": name, "price": Decimal(price),
        "price_sale": None if sale is None else Decimal(sale), "publish": publish,
        "category": category, "created_at": f"2026-01-{i:02d}T00:00:00+00:00", "total_sold": sold,
        "tags": list(tags), "available": 5, "quantity": 5, "images": [], **kw,
    }


class TestSearch(unittest.TestCase):
    def setUp(self):
        self.repo = FakeRepo([
            prod(1, "Gorra roja", 100, 80, sold=3, tags=["verano"]),
            prod(2, "Camiseta", 50, sold=9),
            prod(3, "Gorra draft", 70, publish="draft", sold=1),
            prod(4, "Zapatos", 200, category="Shoes", sold=5),
        ])
        self.svc = ProductService(self.repo, FakeS3())

    def search(self, **kw):
        args = dict(account_id="acc", query=None, filters={}, sort_by=None, limit=20, next_token=None)
        args.update(kw)
        return self.svc.search_products(**args)

    def ids(self, res):
        return [p.id for p in res["results"]]

    def test_users_only_see_published(self):
        self.assertNotIn("p3", self.ids(self.search()))
        self.assertIn("p3", self.ids(self.search(include_drafts=True)))
        self.assertEqual(self.ids(self.search(include_drafts=True, publish="draft")), ["p3"])

    def test_newest_without_category_works(self):
        self.assertEqual(self.ids(self.search(sort_by="newest")), ["p4", "p2", "p1"])

    def test_featured_default_orders_by_total_sold(self):
        self.assertEqual(self.ids(self.search()), ["p2", "p4", "p1"])

    def test_q_matches_name_and_tags(self):
        self.assertEqual(self.ids(self.search(query="gorra")), ["p1"])
        self.assertEqual(self.ids(self.search(query="verano")), ["p1"])

    def test_category_and_price_range_use_live_price(self):
        self.assertEqual(self.ids(self.search(filters={"category": "shoes"})), ["p4"])
        res = self.search(filters={"min_price": 60, "max_price": 90}, sort_by="priceAsc")
        self.assertEqual(self.ids(res), ["p1"])  # live price 80 (not 100)

    def test_price_sort(self):
        self.assertEqual(self.ids(self.search(sort_by="priceAsc")), ["p2", "p1", "p4"])
        self.assertEqual(self.ids(self.search(sort_by="priceDesc")), ["p4", "p1", "p2"])

    def test_pagination_with_opaque_token(self):
        first = self.search(limit=2, sort_by="priceAsc")
        self.assertEqual(self.ids(first), ["p2", "p1"])
        self.assertIsInstance(first["nextToken"], str)
        second = self.search(limit=2, sort_by="priceAsc", next_token=first["nextToken"])
        self.assertEqual(self.ids(second), ["p4"])
        self.assertIsNone(second["nextToken"])

    def test_token_rejected_for_other_account_or_params(self):
        tok = self.search(limit=1)["nextToken"]
        with self.assertRaises(InvalidTokenError):
            self.search(limit=1, next_token=tok, account_id="other")
        with self.assertRaises(InvalidTokenError):
            self.search(limit=1, next_token=tok, query="x")

    def test_output_hides_invalid_legacy_sale(self):
        self.repo.items["p2"]["price_sale"] = Decimal("0")
        p = next(p for p in self.search()["results"] if p.id == "p2")
        self.assertIsNone(p.price_sale)
        self.assertFalse(p.sale_label.enabled)


class TestUpdate(unittest.TestCase):
    def setUp(self):
        self.s3 = FakeS3()
        k1, k2 = "dev/accounts/acc/products/p1/a.png", "dev/accounts/acc/products/p1/b.png"
        self.repo = FakeRepo([prod(1, "Gorra", 100, 80, images=[k1, k2], cover_url=k1)])
        self.svc = ProductService(self.repo, self.s3)
        self.k1, self.k2 = k1, k2

    def upd(self, body):
        return self.svc.update_product("p1", "acc", ProductUpdate.model_validate(body))

    def test_can_remove_images_by_url_and_clears_cover(self):
        out = self.upd({"images": [self.s3.get_s3_public_url(self.k2)]})
        self.assertEqual(self.repo.updates[-1]["images"], [self.k2])
        self.assertEqual(self.repo.updates[-1]["cover_url"], None)
        self.assertEqual(self.s3.deleted, [self.k1])
        self.assertEqual(len(out.images), 1)

    def test_cannot_add_unknown_images(self):
        with self.assertRaises(ValueError):
            self.upd({"images": ["dev/accounts/acc/products/p1/evil.png"]})

    def test_sale_validated_against_stored_price(self):
        with self.assertRaises(ValueError):
            self.upd({"priceSale": 100})
        with self.assertRaises(ValueError):
            self.upd({"price": 70})  # stored sale 80 would be >= new price
        self.upd({"priceSale": None})
        self.assertIn("price_sale", self.repo.updates[-1])
        self.assertIsNone(self.repo.updates[-1]["price_sale"])

    def test_unknown_product(self):
        self.assertIsNone(self.svc.update_product("zz", "acc", ProductUpdate.model_validate({"name": "x"})))

    def test_add_images_is_atomic_and_dedupes(self):
        added = self.svc.add_images("p1", "acc", ["a.png", "c.png"])
        self.assertEqual(added, ["dev/accounts/acc/products/p1/c.png"])
        self.assertEqual(self.repo.appended, [added])


class TestRepoBuilders(unittest.TestCase):
    def test_build_update_sets_and_removes(self):
        cur = {"id": "p1", "account_id": "a", "category": "c", "price": Decimal(10), "created_at": "t"}
        b = ProductRepo.build_update(cur, {"name": "N", "price_sale": None, "price": 20})
        self.assertIn("SET", b["UpdateExpression"])
        self.assertIn("REMOVE #_price_sale", b["UpdateExpression"])
        self.assertEqual(b["ExpressionAttributeValues"][":price"], Decimal("20"))
        self.assertEqual(b["ExpressionAttributeValues"][":gsi3_sk"], Decimal("20"))

    def test_build_update_empty(self):
        self.assertIsNone(ProductRepo.build_update({}, {"total_sold": 5}))

    def test_gsi3_uses_live_price(self):
        self.assertEqual(_build_gsi_attrs({"price": 100, "price_sale": 60})["gsi3_sk"], Decimal("60"))


class TestUrls(unittest.TestCase):
    def test_roundtrip_and_passthrough(self):
        key = "dev/accounts/a b/products/p/x y.png"
        url = public_url_for("bkt", "us-west-2", key)
        self.assertEqual(url, "https://bkt.s3.us-west-2.amazonaws.com/dev/accounts/a%20b/products/p/x%20y.png")
        self.assertEqual(key_from_public_url("bkt", url), key)
        self.assertEqual(public_url_for("bkt", "r", "https://x.test/a.png"), "https://x.test/a.png")
        self.assertEqual(key_from_public_url("bkt", "https://x.test/a.png"), "https://x.test/a.png")


if __name__ == "__main__":
    unittest.main()
