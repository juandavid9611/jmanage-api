from fastapi import APIRouter, Depends, HTTPException
from typing import List
from api.schemas.orders import Order, OrderCreate, OrderUpdate
from services.order_service import OrderService
from di import get_order_service
from auth import PermissionChecker

router = APIRouter(prefix="/orders", tags=["orders"])

@router.get("", response_model=List[Order], dependencies=[Depends(PermissionChecker(required_permissions=["admin", "user"]))])
async def list_orders(svc: OrderService = Depends(get_order_service)):
    return svc.list_orders()

@router.get("/{order_id}", response_model=Order, dependencies=[Depends(PermissionChecker(required_permissions=["admin", "user"]))])
async def get_order(order_id: str, svc: OrderService = Depends(get_order_service)):
    order = svc.get_order(order_id)
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    return order

@router.post("", response_model=Order, dependencies=[Depends(PermissionChecker(required_permissions=["admin", "user"]))])
async def create_order(payload: OrderCreate, svc: OrderService = Depends(get_order_service)):
    return svc.create_order(payload)

@router.put("/{order_id}", response_model=Order, dependencies=[Depends(PermissionChecker(required_permissions=["admin"]))])
async def update_order(order_id: str, payload: OrderUpdate, svc: OrderService = Depends(get_order_service)):
    order = svc.update_order(order_id, payload)
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    return order

@router.delete("/{order_id}", dependencies=[Depends(PermissionChecker(required_permissions=["admin"]))])
async def delete_order(order_id: str, svc: OrderService = Depends(get_order_service)):
    success = svc.delete_order(order_id)
    if not success:
        raise HTTPException(status_code=404, detail="Order not found")
    return {"message": "Order deleted"}
