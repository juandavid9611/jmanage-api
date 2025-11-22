from typing import Dict, Any, Optional
from api.schemas.products import ProductOut, ProductCreate, ProductUpdate, Review, RatingBucket, Label
from repositories.product_repo_ddb import ProductRepo
from repositories.s3_adapter import S3Adapter
from api.schemas.files import FileSpec

class ProductService:
    def __init__(self, repo: ProductRepo, s3: S3Adapter):
        self.repo = repo
        self.s3 = s3

    def create_product(self, data: ProductCreate) -> ProductOut:
        raw = self.repo.create(data.model_dump(by_alias=False))
        return self._map_product(raw)
    
    def list_products(self) -> list[ProductOut]:
        items = self.repo.list_all()
        return [self._map_product(it) for it in items]

    def get_product(self, product_id: str, get_presigned_url: bool = True) -> Optional[ProductOut]:
        raw = self.repo.get_by_id(product_id)
        return self._map_product(raw, get_presigned_url) if raw else None

    def search_products(self, query: str | None, filters: dict, sort_by: str | None, limit: int, next_token: dict | None):
        items, token = self.repo.search(query, filters, sort_by, limit, next_token)
        mapped = [self._map_product(it) for it in items]
        return {"results": mapped, "limit": limit, "nextToken": token}

    def update_product(self, product_id: str, data: ProductUpdate) -> Optional[ProductOut]:
        update_data = data.model_dump(exclude_none=True, by_alias=False)
        # Exclude images from update - use add_images endpoint instead
        update_data.pop("images", None)
        raw = self.repo.update(product_id, update_data)
        return self._map_product(raw) if raw else None

    def delete_product(self, product_id: str) -> bool:
        return self.repo.delete(product_id)
    
    def generate_put_presigned_urls(self, product_id: str, files: list[FileSpec]) -> dict[str, dict[str, str]]:
        """Generate presigned URLs for uploading product images to S3"""
        presigned_urls = {}
        product = self.get_product(product_id)
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
                product_id=product_id,
                filename=file_name,
                content_type=file_content_type
            )
            presigned_urls[file_name] = result["url"]
        return presigned_urls
    
    def add_images(self, product_id: str, file_names: list[str]) -> list[str]:
        """Add image keys to product after successful upload"""
        # Get product without URL conversion to work with raw S3 keys
        existing = self.get_product(product_id, get_presigned_url=False)
        if not existing:
            raise ValueError(f"Product {product_id} not found")
        
        images = []
        for file_name in file_names:
            key = self.s3._kb.product_image(product_id, file_name)
            images.append(key)
        
        # Get current images and append new ones (working with raw keys)
        current_images = existing.images if existing.images else []
        updated_images = current_images + images
        
        self.repo.update(
            product_id,
            {"images": updated_images}
        )
        return images
    
    def _map_product(self, item: Dict[str, Any], get_presigned_url: bool = True) -> ProductOut:
        """Map raw product data to ProductOut schema with optional S3 URL conversion"""
        reviews = sorted(item.get("reviews", []), key=lambda r: r.get("posted_at", ""), reverse=True)
        ratings = item.get("ratings", item.get("ratings_buckets", []))
        new_label = item.get("new_label") or {"enabled": True, "content": "NEW"}
        sale_label = {"enabled": item.get("price_sale") is not None, "content": "SALE"}
        
        # Convert S3 keys to public URLs if requested
        images = item.get("images", [])
        if get_presigned_url and images:
            images = [self.s3.get_s3_public_url(key=image) for image in images]
        
        # Set cover_url to first image if null
        cover_url = item.get("cover_url")
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
            price_sale=item.get("price_sale"),
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



