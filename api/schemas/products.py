from typing import List, Optional
from pydantic import BaseModel, Field
from datetime import datetime
from core.casing import camel_alias

class CamelModel(BaseModel):
    model_config = {
        "alias_generator": camel_alias,
        "populate_by_name": True,
        "from_attributes": True,
    }

class Label(CamelModel):
    enabled: bool
    content: str

class RatingBucket(CamelModel):
    name: str
    star_count: int
    review_count: int

class Review(CamelModel):
    id: str
    name: str
    posted_at: datetime
    comment: str
    is_purchased: bool
    rating: float
    avatar_url: Optional[str] = None
    helpful: int
    attachments: List[str] = []

class ProductOut(CamelModel):
    id: str
    gender: List[str]
    images: List[str]
    reviews: List[Review]
    publish: str
    ratings: List[RatingBucket]
    category: str
    available: int
    price_sale: Optional[float] = None
    taxes: Optional[float] = None
    quantity: int
    inventory_type: Optional[str] = None
    tags: List[str]
    code: Optional[str] = None
    description: Optional[str] = None
    sku: Optional[str] = None
    created_at: datetime | str
    name: str
    price: float
    cover_url: Optional[str] = None
    colors: List[str]
    total_ratings: float
    total_sold: int
    total_reviews: int
    new_label: Label
    sale_label: Label
    sizes: List[str]
    sub_description: Optional[str] = None

class ProductCreate(CamelModel):
    # Campos mínimos + opcionales; id lo generamos en el backend (UUID)
    name: str
    category: str
    price: float
    publish: str = "published"
    available: int = 0
    quantity: int = 0
    taxes: Optional[float] = None
    price_sale: Optional[float] = None
    inventory_type: Optional[str] = None
    code: Optional[str] = None
    sku: Optional[str] = None
    description: Optional[str] = None  # HTML
    sub_description: Optional[str] = None
    cover_url: Optional[str] = None
    gender: List[str] = []
    tags: List[str] = []
    images: Optional[List] = None
    colors: List[str] = []
    sizes: List[str] = []
    ratings: List[RatingBucket] = []
    reviews: List[Review] = []

class ProductUpdate(CamelModel):
    # PUT parcial: todos opcionales; solo actualizamos lo presente
    name: Optional[str] = None
    category: Optional[str] = None
    price: Optional[float] = None
    publish: Optional[str] = None
    available: Optional[int] = None
    quantity: Optional[int] = None
    taxes: Optional[float] = None
    price_sale: Optional[float] = None
    inventory_type: Optional[str] = None
    code: Optional[str] = None
    sku: Optional[str] = None
    description: Optional[str] = None
    sub_description: Optional[str] = None
    cover_url: Optional[str] = None
    gender: Optional[List[str]] = None
    tags: Optional[List[str]] = None
    images: Optional[List[str]] = None
    colors: Optional[List[str]] = None
    sizes: Optional[List[str]] = None
    ratings: Optional[List[RatingBucket]] = None
    reviews: Optional[List[Review]] = None
    total_ratings: Optional[float] = None
    total_sold: Optional[int] = None
    total_reviews: Optional[int] = None

class ProductListOut(CamelModel):
    items: List[ProductOut]
    limit: int
    next_token: Optional[dict] = Field(default=None, alias="nextToken")
