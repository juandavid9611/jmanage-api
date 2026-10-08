import unittest
from decimal import Decimal

from services.order_rules import (
    ShopError, aggregate_quantities, can_transition, compute_totals, money, ORDER_STATUSES, STATUS_TRANSITIONS,
)
from services.product_rules import (
    effective_price, matches_query, normalize_price_sale, query_tokens, validate_price_sale,
)
from services.search_token import InvalidTokenError, decode_token, encode_token, fingerprint


class TestPricing(unittest.TestCase):
    def test_valid_sale(self):
        validate_price_sale(100, 80)
        validate_price_sale(100, 0)  # 0 <= sale < price is valid on write
        validate_price_sale(100, None)

    def test_invalid_sale(self):
        for sale in (100, 120, -1):
            with self.assertRaises(ValueError):
                validate_price_sale(100, sale)

    def test_effective_price(self):
        self.assertEqual(effective_price(100, 80), Decimal("80"))
        self.assertEqual(effective_price(100, None), Decimal("100"))

    def test_legacy_zero_or_high_sale_is_ignored_on_read(self):
        self.assertEqual(effective_price(100, 0), Decimal("100"))
        self.assertEqual(effective_price(100, 150), Decimal("100"))
        self.assertIsNone(normalize_price_sale(100, 0))
        self.assertEqual(normalize_price_sale(100, Decimal("99.5")), Decimal("99.5"))


class TestTotals(unittest.TestCase):
    def test_totals_keep_cents(self):
        t = compute_totals([(Decimal("10.25"), 3), (Decimal("0.99"), 1)], shipping=5, discount="2.50")
        self.assertEqual(t["subtotal"], Decimal("31.74"))
        self.assertEqual(t["total_amount"], Decimal("34.24"))

    def test_negative_rejected(self):
        with self.assertRaises(ShopError):
            compute_totals([(Decimal(1), 1)], shipping=-1)

    def test_discount_cannot_exceed_subtotal(self):
        with self.assertRaises(ShopError):
            compute_totals([(Decimal(10), 1)], discount=11)

    def test_money_rounds_half_up(self):
        self.assertEqual(money("1.005"), Decimal("1.01"))

    def test_aggregate_quantities_merges_lines(self):
        agg = aggregate_quantities([
            {"product_id": "a", "quantity": 1}, {"product_id": "b", "quantity": 2}, {"product_id": "a", "quantity": 3},
        ])
        self.assertEqual(dict(agg), {"a": 4, "b": 2})


class TestTransitions(unittest.TestCase):
    def test_every_status_has_a_map(self):
        self.assertEqual(set(STATUS_TRANSITIONS), set(ORDER_STATUSES))

    def test_allowed_and_denied(self):
        self.assertTrue(can_transition("pending", "paid"))
        self.assertTrue(can_transition("paid", "refunded"))
        self.assertFalse(can_transition("cancelled", "pending"))
        self.assertFalse(can_transition("refunded", "paid"))
        self.assertFalse(can_transition("pending", "completed"))
        self.assertFalse(can_transition("pending", "nonsense"))
        self.assertFalse(can_transition(None, "paid"))


class TestSearch(unittest.TestCase):
    def test_tokens_fold_accents(self):
        self.assertEqual(query_tokens("  Camiseta-ROJA Ñandú "), ["camiseta", "roja", "nandu"])

    def test_matches_name_and_tags(self):
        self.assertTrue(matches_query("Camiseta Roja", ["verano"], "cami roj"))
        self.assertTrue(matches_query("Gorra", ["Verano"], "verano gorra"))
        self.assertFalse(matches_query("Gorra", [], "camiseta"))
        self.assertTrue(matches_query("Gorra", [], None))


class TestSearchToken(unittest.TestCase):
    def test_roundtrip(self):
        fp = fingerprint({"q": "a"})
        tok = encode_token("acc1", 40, fp)
        self.assertEqual(decode_token(tok, "acc1", fp), 40)

    def test_bound_to_account_and_params(self):
        fp = fingerprint({"q": "a"})
        tok = encode_token("acc1", 20, fp)
        with self.assertRaises(InvalidTokenError):
            decode_token(tok, "acc2", fp)
        with self.assertRaises(InvalidTokenError):
            decode_token(tok, "acc1", fingerprint({"q": "b"}))

    def test_garbage(self):
        for bad in ("", "!!!", "e30", "bm90LWpzb24"):
            with self.assertRaises(InvalidTokenError):
                decode_token(bad, "a", "f")
