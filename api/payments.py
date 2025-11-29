from auth import PermissionChecker, get_account_id, get_current_user, get_account_role
from api.schemas.files import FileSpec
from di import get_payment_request_service
from fastapi import APIRouter, Body, Depends, Form, HTTPException
from api.schemas.payments import BulkPutPaymentRequest
from services.payment_request_service import PaymentRequestService


router = APIRouter(prefix="/payment_requests", tags=["payment_requests"])

@router.get("", dependencies=[Depends(PermissionChecker(required_permissions=['admin', 'user']))])
async def list_payment_requests(
    user_id: str | None = None, 
    workspace_id: str | None = None,
    account_id: str = Depends(get_account_id),
    current_user: dict = Depends(get_current_user),
    role: str = Depends(get_account_role),
    svc: PaymentRequestService = Depends(get_payment_request_service)
    ):
    # Security: Non-admin users can only see their own payment requests
    if role != 'admin':
        user_id = current_user.get("sub")
    
    items = svc.list_payment_requests(account_id, user_id=user_id, group=workspace_id)
    return items

@router.post("", dependencies=[Depends(PermissionChecker(required_permissions=['admin']))])
async def create_payment_requests(
    put_payment_request: BulkPutPaymentRequest, 
    account_id: str = Depends(get_account_id),
    svc: PaymentRequestService = Depends(get_payment_request_service)
):
    items = svc.bulk_create(put_payment_request, account_id)
    return items

@router.get("/{payment_request_id}", dependencies=[Depends(PermissionChecker(required_permissions=['admin', 'user']))])
async def get_payment_request(
    payment_request_id: str, 
    account_id: str = Depends(get_account_id),
    svc: PaymentRequestService = Depends(get_payment_request_service)
):
    item = svc.get(payment_request_id, account_id)
    if not item:
        raise HTTPException(status_code=404, detail=f"Payment Request {payment_request_id} not found")
    return item

@router.put("/{payment_request_id}", dependencies=[Depends(PermissionChecker(required_permissions=['admin']))])
async def update_payment_request(
    payment_request_id: str, 
    put_payment_request: BulkPutPaymentRequest, 
    account_id: str = Depends(get_account_id),
    svc: PaymentRequestService = Depends(get_payment_request_service)
):
    existing_item = svc.get(payment_request_id, account_id)
    if not existing_item:
        raise HTTPException(status_code=404, detail=f"Payment Request {payment_request_id} not found")
    svc.update(payment_request_id, account_id, put_payment_request)
    return {"updated_payment_request_id": payment_request_id}

@router.delete("/{payment_request_id}", dependencies=[Depends(PermissionChecker(required_permissions=['admin']))])
async def delete_payment_request(
    payment_request_id: str, 
    account_id: str = Depends(get_account_id),
    svc: PaymentRequestService = Depends(get_payment_request_service)
):
    svc.delete(payment_request_id, account_id)
    return {"deleted_payment_request_id": payment_request_id}

#TODO MISSING TESTING
@router.post("/{payment_request_id}/generate-presigned-urls", dependencies=[Depends(PermissionChecker(required_permissions=['admin', 'user']))])
async def generate_payment_request_presigned_urls(
    payment_request_id: str, 
    files: list[FileSpec] = Body(..., embed=False), 
    account_id: str = Depends(get_account_id),
    svc: PaymentRequestService = Depends(get_payment_request_service)):
    payment_request = svc.get(payment_request_id, account_id)
    if not payment_request:
        raise HTTPException(status_code=404, detail=f"Payment Request {payment_request_id} not found")

    try:
        result = svc.generate_put_presigned_urls(payment_request_id=payment_request_id, account_id=account_id, files=files)
    except Exception as e:
        raise HTTPException(status_code=404, detail=f"Error generating presigned URLs: {str(e)}")

    return {"urls": result}

#TODO MISSING TESTING
@router.post("/{payment_request_id}/request_approval", dependencies=[Depends(PermissionChecker(required_permissions=['admin', 'user']))])
async def request_payment_request_approval(
    payment_request_id: str, 
    file_names: list[str] = Body(..., embed=False), 
    account_id: str = Depends(get_account_id),
    svc: PaymentRequestService = Depends(get_payment_request_service)
):
    payment_request = svc.get(payment_request_id, account_id)
    if not payment_request:
        raise HTTPException(status_code=404, detail=f"Payment Request {payment_request_id} not found")
    user = payment_request["paymentRequestTo"]
    if payment_request["status"] != "pending" and payment_request["status"] != "overdue":
        raise HTTPException(status_code=400, detail=f"Payment Request {payment_request_id} is not in pending or overdue status")
    if not file_names:
        raise HTTPException(status_code=400, detail="No files were uploaded")
    
    return {"requested_payment_request_approval_id": svc.request_payment_request_approval(payment_request_id, account_id, file_names)}
