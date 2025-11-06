from fastapi import APIRouter, Depends, HTTPException

from api.schemas.products import PutProduct
from auth import PermissionChecker
from di import get_product_service
from services.product_service import ProductService


router = APIRouter(prefix="/products", tags=["products"])


@router.get("", dependencies=[Depends(PermissionChecker(required_permissions=["admin", "user"]))])
async def list_products(
    category: str | None = None,
    inventory_type: str | None = None,
    publish: str | None = None,
    svc: ProductService = Depends(get_product_service),
):
    return svc.list_products(category=category, inventory_type=inventory_type, publish=publish)


@router.post("", dependencies=[Depends(PermissionChecker(required_permissions=["admin"]))])
async def create_product(put_product: PutProduct, svc: ProductService = Depends(get_product_service)):
    return svc.create(put_product)


@router.get("/{product_id}", dependencies=[Depends(PermissionChecker(required_permissions=["admin", "user"]))])
async def get_product(product_id: str, svc: ProductService = Depends(get_product_service)):
    item = svc.get(product_id)
    if not item:
        raise HTTPException(status_code=404, detail=f"Product {product_id} not found")
    return item


@router.put("/{product_id}", dependencies=[Depends(PermissionChecker(required_permissions=["admin"]))])
async def update_product(product_id: str, put_product: PutProduct, svc: ProductService = Depends(get_product_service)):
    item = svc.update(product_id, put_product)
    if not item:
        raise HTTPException(status_code=404, detail=f"Product {product_id} not found")
    return item


@router.delete("/{product_id}", dependencies=[Depends(PermissionChecker(required_permissions=["admin"]))])
async def delete_product(product_id: str, svc: ProductService = Depends(get_product_service)):
    existing = svc.get(product_id)
    if not existing:
        raise HTTPException(status_code=404, detail=f"Product {product_id} not found")
    svc.delete(product_id)
    return {"deleted_product_id": product_id}
