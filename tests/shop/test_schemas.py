import unittest

from pydantic import ValidationError

from api.schemas.files import ImageFileSpec
from api.schemas.orders import OrderCreate, OrderUpdate
from api.schemas.products import ProductCreate, ProductUpdate


def create(**kw):
    base = {"name": "Gorra", "category": "Hats", "price": 100}
    base.update(kw)
    return ProductCreate.model_validate(base)


class TestProductSchemas(unittest.TestCase):
    def test_available_defaults_to_quantity(self):
        self.assertEqual(create(quantity=7).available, 7)
        self.assertEqual(create(quantity=7, available=3).available, 3)

    def test_camel_case_input(self):
        p = create(priceSale=50, inventoryType="in_stock")
        self.assertEqual(p.price_sale, 50)

    def test_rejections(self):
        for bad in ({"price": -1}, {"quantity": -1}, {"available": -1}, {"taxes": -1},
                    {"priceSale": 100}, {"priceSale": 120}, {"priceSale": -5},
                    {"name": ""}, {"name": "x" * 201}, {"publish": "hidden"}):
            with self.assertRaises(ValidationError, msg=str(bad)):
                create(**bad)

    def test_name_is_stripped(self):
        self.assertEqual(create(name="  Gorra ").name, "Gorra")

    def test_update_only_sets_present_fields_and_allows_clearing(self):
        u = ProductUpdate.model_validate({"priceSale": None, "name": "Nueva"})
        dumped = u.model_dump(exclude_unset=True, by_alias=False)
        self.assertEqual(dumped, {"price_sale": None, "name": "Nueva"})

    def test_update_rejects_null_on_required_fields(self):
        for f in ("name", "price", "publish", "available", "quantity", "category"):
            with self.assertRaises(ValidationError):
                ProductUpdate.model_validate({f: None})

    def test_update_cannot_set_server_managed_fields(self):
        u = ProductUpdate.model_validate({"totalSold": 99, "totalRatings": 5, "reviews": []})
        self.assertEqual(u.model_dump(exclude_unset=True), {})

    def test_update_price_pair_checked(self):
        with self.assertRaises(ValidationError):
            ProductUpdate.model_validate({"price": 10, "priceSale": 10})

    def test_update_images_may_be_emptied(self):
        u = ProductUpdate.model_validate({"images": []})
        self.assertEqual(u.model_dump(exclude_unset=True)["images"], [])


class TestImageFileSpec(unittest.TestCase):
    def test_ok(self):
        ImageFileSpec(file_name="a.PNG", content_type="image/png", size=1000)

    def test_rejects(self):
        for kw in ({"file_name": "a.pdf", "content_type": "application/pdf"},
                   {"file_name": "a.png", "content_type": "image/jpeg"},
                   {"file_name": "a.svg", "content_type": "image/svg+xml"},
                   {"file_name": "a.png", "content_type": "image/png", "size": 6 * 1024 * 1024},
                   {"file_name": "a.png", "content_type": "image/png", "size": 0}):
            with self.assertRaises(ValidationError, msg=str(kw)):
                ImageFileSpec(**kw)


class TestOrderSchemas(unittest.TestCase):
    BODY = {
        "workspaceId": "w1",
        "items": [{"productId": "p1", "quantity": 2, "color": "red", "size": "M"}],
        "shipping": 10, "discount": 0,
        "shippingAddress": {"fullAddress": "x", "addressType": "Home", "company": ""},
        "payment": {"payment": "cash"},
        # legacy client-computed fields must be ignored, not trusted
        "subtotal": 1, "totalAmount": 1, "totalQuantity": 99,
    }

    def test_create_parses_new_shape_and_ignores_totals(self):
        o = OrderCreate.model_validate(self.BODY)
        self.assertEqual(o.items[0].product_id, "p1")
        self.assertFalse(hasattr(o, "total_amount"))

    def test_quantity_must_be_positive(self):
        for q in (0, -1):
            body = {**self.BODY, "items": [{"productId": "p1", "quantity": q}]}
            with self.assertRaises(ValidationError):
                OrderCreate.model_validate(body)

    def test_negative_shipping_discount_and_empty_items(self):
        for patch in ({"shipping": -1}, {"discount": -1}, {"items": []}):
            with self.assertRaises(ValidationError):
                OrderCreate.model_validate({**self.BODY, **patch})

    def test_update_status_enum(self):
        OrderUpdate.model_validate({"status": "paid"})
        with self.assertRaises(ValidationError):
            OrderUpdate.model_validate({"status": "shipped"})
