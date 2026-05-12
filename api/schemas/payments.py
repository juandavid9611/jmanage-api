from enum import Enum
from pydantic import BaseModel


class PaymentRequestStatus(str, Enum):
    PAID = "paid"
    PENDING = "pending"
    OVERDUE = "overdue"
    CANCELED = "canceled"
    APPROVAL_PENDING = "approval_pending"

class BulkPutPaymentRequest(BaseModel):
    id: str | None = None
    status: PaymentRequestStatus | None = None
    createDate: str
    dueDate: str
    concept: str
    description: str
    category: str
    group: str
    paymentRequestTo: list[dict]
    isVerified: bool | None = None
    userPrice: int
    overduePrice: int | None = None
    orderId: str | None = None
