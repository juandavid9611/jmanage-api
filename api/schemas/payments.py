from enum import Enum
from typing import Optional
from pydantic import BaseModel


class PaymentRequestStatus(str, Enum):
    PAID = "paid"
    PENDING = "pending"
    OVERDUE = "overdue"
    CANCELED = "canceled"
    APPROVAL_PENDING = "approval_pending"

class BulkPutPaymentRequest(BaseModel):
    id: Optional[str] = None
    status: Optional[PaymentRequestStatus] = None
    createDate: str
    dueDate: str
    concept: str
    description: str
    category: str
    group: str
    paymentRequestTo: list[dict]
    isVerified: Optional[bool] = None
    userPrice: int
    overduePrice: Optional[int] = None
