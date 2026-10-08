from pydantic import BaseModel, Field, field_validator
from typing import List, Optional
from datetime import datetime
from core.casing import camel_alias
from services.order_rules import MAX_ORDER_LINES, ORDER_STATUSES


class CamelModel(BaseModel):
    model_config = {
        "alias_generator": camel_alias,
        "populate_by_name": True,
        "from_attributes": True,
    }


class CheckMark(CamelModel):
    checked: bool
    checked_at: datetime
    checked_by: str
    note: Optional[str] = None


class OrderCheckUpdate(CamelModel):
    checked: bool
    note: Optional[str] = None


class OrderItem(CamelModel):
    id: str
    sku: str
    quantity: int
    name: str
    cover_url: str
    price: float
    available: int
    colors: List[str]
    size: str


class OrderEvent(CamelModel):
    type: str
    title: str
    time: datetime
    meta: Optional[dict] = None


class Customer(CamelModel):
    id: str
    name: str
    email: str
    phone_number: str
    avatar_url: Optional[str] = None
    ip_address: Optional[str] = None


class Delivery(CamelModel):
    shipment_amount: float
    delivery_type: str


class ShippingAddress(CamelModel):
    full_address: str
    address_type: str
    company: str


class Payment(CamelModel):
    payment: str
    card_type: Optional[str] = None
    card_number: Optional[str] = None


class Order(CamelModel):
    id: str
    order_number: str
    created_at: datetime
    workspace_id: Optional[str] = None
    taxes: float = 0.0
    items: List[OrderItem] = []
    history: List[OrderEvent] = []
    subtotal: float
    shipping: float
    discount: float
    customer: Customer
    delivery: Delivery
    total_amount: float
    total_quantity: int
    shipping_address: ShippingAddress
    payment: Payment
    status: str
    payment_request_id: Optional[str] = None
    provider_check: Optional[CheckMark] = None
    delivery_check: Optional[CheckMark] = None


class OrderLineIn(CamelModel):
    """Order line sent by the client. Everything else (name, price, image) is read server-side."""
    product_id: str = Field(min_length=1)
    quantity: int = Field(ge=1, le=1000)
    color: Optional[str] = None
    size: Optional[str] = None


class CustomerIn(CamelModel):
    """Customer contact data. `id` is ignored: the server binds it to the token's sub."""
    name: Optional[str] = None
    email: Optional[str] = None
    phone_number: str = ""
    avatar_url: Optional[str] = None


class DeliveryIn(CamelModel):
    delivery_type: str = ""
    shipment_amount: Optional[float] = None  # ignored; shipping is taken from `shipping`


class OrderCreate(CamelModel):
    # subtotal / totalAmount / totalQuantity sent by older clients are ignored (recomputed).
    workspace_id: str = Field(min_length=1)
    items: List[OrderLineIn] = Field(min_length=1, max_length=MAX_ORDER_LINES)
    shipping: float = Field(default=0, ge=0)
    discount: float = Field(default=0, ge=0)
    customer: CustomerIn = CustomerIn()
    delivery: DeliveryIn = DeliveryIn()
    shipping_address: ShippingAddress
    payment: Payment


class OrderUpdate(CamelModel):
    status: Optional[str] = None
    delivery: Optional[Delivery] = None

    @field_validator("status")
    @classmethod
    def _valid_status(cls, v):
        if v is not None and v not in ORDER_STATUSES:
            raise ValueError(f"status must be one of {ORDER_STATUSES}")
        return v
