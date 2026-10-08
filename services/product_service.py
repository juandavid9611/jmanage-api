import logging
from typing import Dict, Any, Optional
from api.schemas.products import ProductOut, ProductCreate, ProductUpdate, Review, RatingBucket, Label
from repositories.product_repo_ddb import ProductRepo
from repositories.s3_adapter import S3Adapter
from api.schemas.files import FileSpec
from services.product_rules import (
    effective_price, matches_query, normalize_price_sale, to_decimal, validate_price_sale,
)
from services.search_token import decode_token, encode_token, fingerprint

logger = logging.getLogger(__name__)

SORTS = ("featured", "newest", "priceDesc", "priceAsc")


class ProductService:
    def __init__(self, repo: ProductRepo, s3: S3Adapter):
        self.repo = repo
        self.s3 = s3

    def create_product(self, data: ProductCreate, account_id: str) -> ProductOut:
        raw = self.repo.create(data.model_dump(by_alias=False), account_id)
        return self._map_product(raw)

    def list_products(self, account_id: str, include_drafts: bool = True) -> list[ProductOut]:
        items = self.repo.list_all(account_id)
        if not include_drafts:
            items = [it for it in items if it.get("publish", "published") == "published"]
        return [self._map_product(it) for it in items]

    def get_product(
        self, product_id: str, account_id: str, get_presigned_url: bool = True, include_drafts: bool = True
    ) -> Optional[ProductOut]:
        raw = self.repo.get_by_id(product_id, account_id)
        if raw and not include_drafts and raw.get("publish", "published") != "published":
            return None
        return self._map_product(raw, get_presigned_url) if raw else None

    def search_products(
        self,
        account_id: str,
        query: str | None,
        filters: dict,
        sort_by: str | None,
        limit: int,
        next_token: str | None,
        include_drafts: bool = False,
        publish: str | None = None,
    ):
        """Filter/sort in memory over the account's products (account GSI) and page with an
        opaque offset token bound to the account and the search parameters.
        Raises ValueError (InvalidTokenError) for a bad token."""
        sort = sort_by or "featured"
        if sort not in SORTS:
            raise ValueError(f"sortBy must be one of {SORTS}")
        if not include_drafts:
            publish = "published"
        params = {"q": query, "f": filters, "s": sort, "p": publish}
        fp = fingerprint(params)
        offset = decode_token(next_token, account_id, fp) if next_token else 0

        items = [it for it in self.repo.list_all(account_id) if self._matches(it, query, filters, publish)]
        items.sort(key=self._sort_key(sort), reverse=sort in ("featured", "newest", "priceDesc"))
        page = items[offset: offset + limit]
        more = offset + limit < len(items)
        token = encode_token(account_id, offset + limit, fp) if more else None
        return {"results": [self._map_product(it) for it in page], "limit": limit, "nextToken": token}

    @staticmethod
    def _matches(item: Dict[str, Any], query, filters: dict, publish) -> bool:
        if publish and item.get("publish", "published") != publish:
            return False
        if not matches_query(item.get("name", ""), item.get("tags") or [], query):
            return False
        category = filters.get("category")
        if category and (item.get("category") or "").lower() != category.lower():
            return False
        live = effective_price(item.get("price", 0), item.get("price_sale"))
        if filters.get("min_price") is not None and live < to_decimal(filters["min_price"]):
            return False
        if filters.get("max_price") is not None and live > to_decimal(filters["max_price"]):
            return False
        genders = filters.get("genders") or []
        if genders and not set(genders) & set(item.get("genders") or item.get("gender") or []):
            return False
        colors = filters.get("colors") or []
        if colors and not set(colors) & set(item.get("colors") or []):
            return False
        min_rating = filters.get("min_rating")
        if min_rating is not None and to_decimal(item.get("total_ratings", 0)) < to_decimal(min_rating):
            return False
        return True

    @staticmethod
    def _sort_key(sort: str):
        if sort == "newest":
            return lambda it: (str(it.get("created_at", "")), it["id"])
        if sort in ("priceAsc", "priceDesc"):
            return lambda it: (effective_price(it.get("price", 0), it.get("price_sale")), it["id"])
        return lambda it: (int(it.get("total_sold", 0)), str(it.get("created_at", "")), it["id"])

    def update_product(self, product_id: str, account_id: str, data: ProductUpdate) -> Optional[ProductOut]:
        """Raises ValueError for invalid combinations (e.g. priceSale >= price)."""
        patch = data.model_dump(exclude_unset=True, by_alias=False)
        current = self.repo.get_by_id(product_id, account_id)
        if not current:
            return None

        if "price" in patch or "price_sale" in patch:
            price = patch.get("price", current.get("price"))
            sale = patch["price_sale"] if "price_sale" in patch else current.get("price_sale")
            if "price_sale" not in patch and sale is not None and to_decimal(sale) <= 0:
                sale = None  # legacy 0 meant "no discount"
            validate_price_sale(price, sale)

        removed_keys: list[str] = []
        if "images" in patch:
            existing = list(current.get("images") or [])
            desired = [self.s3.key_from_url(i) for i in (patch["images"] or [])]
            unknown = [k for k in desired if k not in existing]
            if unknown:
                raise ValueError("images can only be removed or reordered; use add_images to add new ones")
            removed_keys = [k for k in existing if k not in desired]
            patch["images"] = desired
            cover = current.get("cover_url")
            if "cover_url" not in patch and cover and self.s3.key_from_url(cover) in removed_keys:
                patch["cover_url"] = None

        raw = self.repo.update(product_id, account_id, patch)
        if raw is None:
            return None
        for key in removed_keys:
            if key.startswith("http"):
                continue
            try:
                self.s3.delete_file(key)
            except Exception:
                logger.warning("Failed to delete removed product image %s", key, exc_info=True)
        return self._map_product(raw)

    def delete_product(self, product_id: str, account_id: str) -> bool:
        return self.repo.delete(product_id, account_id)

    def generate_put_presigned_urls(self, product_id: str, account_id: str, files: list[FileSpec]) -> dict[str, dict[str, str]]:
        """Generate presigned URLs for uploading product images to S3"""
        presigned_urls = {}
        product = self.get_product(product_id, account_id)
        if not product:
            raise ValueError(f"Product {product_id} not found")

        for file in files:
            if not isinstance(file, FileSpec):
                raise TypeError("Each file must be a FileSpec instance.")

            file_name = file.file_name
            file_content_type = file.content_type
            if not file_name or not file_content_type:
                raise ValueError("File 'file_name' and 'content_type' cannot be empty.")

            result = self.s3.presign_product_image_put(
                account_id=account_id,
                product_id=product_id,
                filename=file_name,
                content_type=file_content_type,
                content_length=getattr(file, "size", None),
            )
            presigned_urls[file_name] = result["url"]
        return presigned_urls

    def add_images(self, product_id: str, account_id: str, file_names: list[str]) -> list[str]:
        """Add image keys to product after successful upload (atomic list_append)."""
        raw = self.repo.get_by_id(product_id, account_id)
        if not raw:
            raise ValueError(f"Product {product_id} not found")
        present = set(raw.get("images") or [])
        keys = []
        for file_name in file_names:
            key = self.s3._kb.product_image(account_id, product_id, file_name)
            if key not in present and key not in keys:
                keys.append(key)
        if keys and not self.repo.append_images(product_id, account_id, keys):
            raise ValueError(f"Product {product_id} not found")
        return keys

    def _map_product(self, item: Dict[str, Any], get_presigned_url: bool = True) -> ProductOut:
        """Map raw product data to ProductOut schema with optional S3 URL conversion"""
        reviews = sorted(item.get("reviews", []), key=lambda r: r.get("posted_at", ""), reverse=True)
        ratings = item.get("ratings", item.get("ratings_buckets", []))
        new_label = item.get("new_label") or {"enabled": True, "content": "NEW"}
        price_sale = normalize_price_sale(item["price"], item.get("price_sale"))
        sale_label = {"enabled": price_sale is not None, "content": "SALE"}
        
        # Convert S3 keys to public URLs if requested
        images = item.get("images", [])
        if get_presigned_url and images:
            images = [self.s3.get_s3_public_url(key=image) for image in images]
        
        # Set cover_url to first image if null
        cover_url = item.get("cover_url")
        if cover_url and get_presigned_url:
            cover_url = self.s3.get_s3_public_url(key=cover_url)
        if not cover_url and images:
            cover_url = images[0]

        return ProductOut(
            id=item["id"],
            gender=item.get("gender") or item.get("genders", []),
            images=images,
            reviews=[Review(**r) for r in reviews],
            publish=item.get("publish", "published"),
            ratings=[RatingBucket(**b) for b in ratings],
            category=item["category"],
            available=item.get("available", 0),
            price_sale=float(price_sale) if price_sale is not None else None,
            taxes=item.get("taxes"),
            quantity=item.get("quantity", 0),
            inventory_type=item.get("inventory_type"),
            tags=item.get("tags", []),
            code=item.get("code"),
            description=item.get("description_html"),
            sku=item.get("sku"),
            created_at=item["created_at"],
            name=item["name"],
            price=float(item["price"]),
            cover_url=cover_url,
            colors=item.get("colors", []),
            total_ratings=float(item.get("total_ratings", 0.0)),
            total_sold=int(item.get("total_sold", 0)),
            total_reviews=int(item.get("total_reviews", 0)),
            new_label=Label(**new_label),
            sale_label=Label(**sale_label),
            sizes=item.get("sizes", []),
            sub_description=item.get("sub_description"),
        )



