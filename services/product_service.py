from typing import Any
from uuid import uuid4

from api.schemas.products import PutProduct
from repositories.product_repo_ddb import ProductRepo


class ProductService:
    def __init__(self, repo: ProductRepo):
        self.repo = repo
        self._excluded_fields = {"id"}

    def list_products(
        self,
        *,
        category: str | None = None,
        inventory_type: str | None = None,
        publish: str | None = None,
    ) -> list[dict[str, Any]]:
        items = list(self.repo.list_all())

        def _matches_filters(product: dict[str, Any]) -> bool:
            if category and product.get("category") != category:
                return False
            if inventory_type and product.get("inventory_type") != inventory_type:
                return False
            if publish and product.get("publish") != publish:
                return False
            return True

        return [self._map_product(i) for i in items if _matches_filters(i)]

    def get(self, product_id: str) -> dict[str, Any] | None:
        item = self.repo.get(product_id)
        if not item:
            return None
        return self._map_product(item)

    def create(self, put_product: PutProduct) -> dict[str, Any]:
        data = put_product.model_dump(exclude_unset=True, exclude_none=True)
        product_id = data.get("id") or f"product_{uuid4().hex}"
        data["id"] = product_id
        self.repo.put(data)
        created = self.repo.get(product_id)
        if not created:
            raise ValueError(f"Product {product_id} not found after creation")
        return self._map_product(created)

    def update(self, product_id: str, put_product: PutProduct) -> dict[str, Any] | None:
        existing = self.repo.get(product_id)
        if not existing:
            return None
        updates = put_product.model_dump(exclude_unset=True, exclude_none=True)
        for field in self._excluded_fields:
            updates.pop(field, None)
        if not updates:
            return self._map_product(existing)
        self.repo.update(product_id, updates)
        refreshed = self.repo.get(product_id)
        if not refreshed:
            raise ValueError(f"Product {product_id} not found after update")
        return self._map_product(refreshed)

    def delete(self, product_id: str) -> None:
        self.repo.delete(product_id)

    def _map_product(self, item: dict[str, Any]) -> dict[str, Any]:
        product = PutProduct.model_validate(item)
        return product.model_dump(by_alias=True, exclude_none=True)
