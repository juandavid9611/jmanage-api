from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime
from core.casing import camel_alias


class CamelModel(BaseModel):
    model_config = {
        "alias_generator": camel_alias,
        "populate_by_name": True,
        "from_attributes": True,
    }


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
    shipment_amount: int
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


class OrderCreate(CamelModel):
    workspace_id: str
    items: List[OrderItem]
    subtotal: float
    shipping: float
    discount: float
    customer: Customer
    delivery: Delivery
    total_amount: float
    total_quantity: int
    shipping_address: ShippingAddress
    payment: Payment


class OrderUpdate(CamelModel):
    status: Optional[str] = None
    delivery: Optional[Delivery] = None
