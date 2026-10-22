from decimal import Decimal
from enum import Enum
from pydantic import BaseModel


class PaymentRequestStatus(str, Enum):
    PAID = "paid"
    PENDING = "pending"
    OVERDUE = "overdue"
    CANCELED = "canceled"
    APPROVAL_PENDING = "approval_pending"
    PARTIAL = "partial"

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
    userPrice: Decimal
    overduePrice: Decimal | None = None
    orderId: str | None = None
    reference: str | None = None
    allowInstallments: bool | None = None
    maxInstallments: int | None = None


class SubmitPaymentRequest(BaseModel):
    """Body of POST /payment_requests/{id}/submit_payment (camelCase like the rest of the payment fields)."""
    amount: Decimal | None = None  # ignored (forced to the full balance) when the request has no installments
    fileNames: list[str]
    accountId: str | None = None


class ReviewPaymentRequest(BaseModel):
    """Body of POST /payment_requests/{id}/review_payment."""
    submissionId: str
    approve: bool
    accountId: str | None = None
