from __future__ import annotations

from typing import List, Optional

from pydantic import BaseModel, ConfigDict
from pydantic.alias_generators import to_camel


class CamelCaseModel(BaseModel):
    model_config = ConfigDict(
        alias_generator=to_camel,
        populate_by_name=True,
        extra="allow",
    )


class ProductLabel(CamelCaseModel):
    enabled: Optional[bool] = None
    content: Optional[str] = None


class ProductRating(CamelCaseModel):
    name: Optional[str] = None
    star_count: Optional[int] = None
    review_count: Optional[int] = None


class ProductReview(CamelCaseModel):
    id: Optional[str] = None
    name: Optional[str] = None
    posted_at: Optional[str] = None
    comment: Optional[str] = None
    is_purchased: Optional[bool] = None
    rating: Optional[float] = None
    avatar_url: Optional[str] = None
    helpful: Optional[int] = None
    attachments: Optional[List[str]] = None


class PutProduct(CamelCaseModel):
    id: Optional[str] = None
    name: Optional[str] = None
    gender: Optional[List[str]] = None
    images: Optional[List[str]] = None
    reviews: Optional[List[ProductReview]] = None
    publish: Optional[str] = None
    ratings: Optional[List[ProductRating]] = None
    category: Optional[str] = None
    available: Optional[int] = None
    price_sale: Optional[float] = None
    taxes: Optional[float] = None
    quantity: Optional[int] = None
    inventory_type: Optional[str] = None
    tags: Optional[List[str]] = None
    code: Optional[str] = None
    description: Optional[str] = None
    sku: Optional[str] = None
    created_at: Optional[str] = None
    price: Optional[float] = None
    cover_url: Optional[str] = None
    colors: Optional[List[str]] = None
    total_ratings: Optional[float] = None
    total_sold: Optional[int] = None
    total_reviews: Optional[int] = None
    new_label: Optional[ProductLabel] = None
    sale_label: Optional[ProductLabel] = None
    sizes: Optional[List[str]] = None
    sub_description: Optional[str] = None
