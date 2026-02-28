from auth import PermissionChecker
from di import get_payment_request_service
from fastapi import APIRouter, Depends, HTTPException
from api.schemas.payments import BulkPutPaymentRequest
from services.payment_request_service import PaymentRequestService


router = APIRouter(tags=["scheduled"])

@router.post("/process_overdue_request_payments", dependencies=[Depends(PermissionChecker(required_permissions=['admin']))])
async def process_overdue_request_payments(
    svc: PaymentRequestService = Depends(get_payment_request_service),
    ):
    processed_request_payments = svc.process_overdue_payments()
    return {"processed_request_payments": processed_request_payments}
