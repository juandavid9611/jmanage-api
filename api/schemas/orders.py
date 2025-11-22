from pydantic import BaseModel, Field
from typing import List, Optional
from datetime import datetime

class OrderItem(BaseModel):
    id: str
    sku: str
    quantity: int
    name: str
    coverUrl: str
    price: float
    available: int
    colors: List[str]
    size: str

class TimelineItem(BaseModel):
    title: str
    time: datetime

class OrderHistory(BaseModel):
    orderTime: datetime
    paymentTime: Optional[datetime] = None
    deliveryTime: Optional[datetime] = None
    completionTime: Optional[datetime] = None
    timeline: List[TimelineItem]

class Customer(BaseModel):
    id: str
    name: str
    email: str
    phoneNumber: str
    avatarUrl: Optional[str] = None
    ipAddress: Optional[str] = None

class Delivery(BaseModel):
    shipment_amount: int
    delivery_type: str

class ShippingAddress(BaseModel):
    fullAddress: str
    addressType: str
    company: str

class Payment(BaseModel):
    payment: str
    cardType: str
    cardNumber: str

class Order(BaseModel):
    id: str
    orderNumber: str
    createdAt: datetime
    taxes: float
    items: List[OrderItem]
    history: OrderHistory
    subtotal: float
    shipping: float
    discount: float
    customer: Customer
    delivery: Delivery
    totalAmount: float
    totalQuantity: int
    shippingAddress: ShippingAddress
    payment: Payment
    status: str

class OrderCreate(BaseModel):
    items: List[OrderItem]
    subtotal: float
    shipping: float
    discount: float
    customer: Customer
    delivery: Delivery
    totalAmount: float
    totalQuantity: int
    shippingAddress: ShippingAddress
    payment: Payment

class OrderUpdate(BaseModel):
    status: Optional[str] = None
    delivery: Optional[Delivery] = None

