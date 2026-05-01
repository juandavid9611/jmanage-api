from fastapi import APIRouter, Depends, HTTPException
from typing import List, Optional
from api.schemas.orders import Order, OrderCreate, OrderUpdate
from services.order_service import OrderService
from di import get_order_service
from auth import PermissionChecker, get_account_id

router = APIRouter(prefix="/orders", tags=["orders"])

@router.get("", response_model=List[Order], dependencies=[Depends(PermissionChecker(required_permissions=["admin", "user"]))])
async def list_orders(
    workspace_id: Optional[str] = None,
    account_id: str = Depends(get_account_id),
    svc: OrderService = Depends(get_order_service),
):
    return svc.list_orders(account_id, workspace_id=workspace_id)

@router.get("/{order_id}", response_model=Order, dependencies=[Depends(PermissionChecker(required_permissions=["admin", "user"]))])
async def get_order(
    order_id: str, 
    account_id: str = Depends(get_account_id),
    svc: OrderService = Depends(get_order_service)
):
    order = svc.get_order(order_id, account_id)
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    return order

@router.post("", response_model=Order, dependencies=[Depends(PermissionChecker(required_permissions=["admin", "user"]))])
async def create_order(
    payload: OrderCreate, 
    account_id: str = Depends(get_account_id),
    svc: OrderService = Depends(get_order_service)
):
    return svc.create_order(payload, account_id)

@router.put("/{order_id}", response_model=Order, dependencies=[Depends(PermissionChecker(required_permissions=["admin"]))])
async def update_order(
    order_id: str, 
    payload: OrderUpdate, 
    account_id: str = Depends(get_account_id),
    svc: OrderService = Depends(get_order_service)
):
    order = svc.update_order(order_id, account_id, payload)
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    return order

@router.delete("/{order_id}", dependencies=[Depends(PermissionChecker(required_permissions=["admin"]))])
async def delete_order(
    order_id: str, 
    account_id: str = Depends(get_account_id),
    svc: OrderService = Depends(get_order_service)
):
    success = svc.delete_order(order_id, account_id)
    if not success:
        raise HTTPException(status_code=404, detail="Order not found")
    return {"message": "Order deleted"}
