import importlib.util
import os
import unittest
from decimal import Decimal

os.environ.setdefault("AWS_DEFAULT_REGION", "us-west-2")

_spec = importlib.util.spec_from_file_location(
    "fix_product_price_sale",
    os.path.join(os.path.dirname(__file__), "..", "..", "migrations", "fix_product_price_sale.py"),
)
mig = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(mig)


def item(**kw):
    base = {"pk": "PRODUCT#p1", "sk": "PRODUCT", "id": "p1", "account_id": "a", "price": Decimal(100),
            "category": "c", "created_at": "t", "publish": "published", "available": 3, "quantity": 3}
    base.update(kw)
    base.update({k: v for k, v in mig._build_gsi_attrs(base).items() if k not in kw})
    return base


class TestPlan(unittest.TestCase):
    def test_clean_item_has_no_changes(self):
        self.assertEqual(mig.plan_changes(item()), ({}, [], []))

    def test_zero_and_high_sale_removed(self):
        for sale in (Decimal(0), Decimal(100), Decimal(150)):
            sets, removes, _ = mig.plan_changes(item(price_sale=sale))
            self.assertEqual(removes, ["price_sale"])
            self.assertEqual(sets["gsi3_sk"], Decimal(100))

    def test_valid_sale_kept(self):
        sets, removes, _ = mig.plan_changes(item(price_sale=Decimal(80), gsi3_sk=Decimal(80)))
        self.assertEqual((sets, removes), ({}, []))

    def test_backfills_id_publish_available_and_gsi(self):
        raw = {"pk": "PRODUCT#p9", "sk": "PRODUCT", "account_id": "a", "price": Decimal(5),
               "category": "c", "quantity": 4, "created_at": "t"}
        sets, removes, problems = mig.plan_changes(raw)
        self.assertEqual(sets["id"], "p9")
        self.assertEqual(sets["publish"], "published")
        self.assertEqual(sets["available"], 4)
        self.assertEqual(sets["gsi3_sk"], Decimal(5))
        self.assertEqual(problems, [])

    def test_missing_account_is_reported(self):
        _, _, problems = mig.plan_changes({"pk": "PRODUCT#p", "sk": "PRODUCT", "price": 1})
        self.assertTrue(problems)


if __name__ == "__main__":
    unittest.main()
