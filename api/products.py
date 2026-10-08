from fastapi import APIRouter, Depends, HTTPException, Query, Body

from api.schemas.products import ProductCreate, ProductOut, ProductUpdate
from api.schemas.files import ImageFileSpec
from auth import PermissionChecker, get_account_id, get_account_role
from di import get_product_service
from services.product_service import ProductService
from services.search_token import InvalidTokenError


search_router = APIRouter(prefix="/products_search", tags=["products"])
router = APIRouter(prefix="/products", tags=["products"])


def _can_see_drafts(role: str) -> bool:
    return role == "admin"


@search_router.get("", dependencies=[Depends(PermissionChecker(required_permissions=["admin", "user"]))])
async def search_products(
    q: str | None = Query(None),
    category: str | None = Query(None),
    genders: list[str] = Query(default=[]),
    colors: list[str] = Query(default=[]),
    minPrice: float | None = Query(None, ge=0),
    maxPrice: float | None = Query(None, ge=0),
    minRating: float | None = Query(None, ge=0, le=5),
    sortBy: str | None = Query(None, pattern="^(featured|newest|priceDesc|priceAsc)$"),
    publish: str | None = Query(None, pattern="^(published|draft)$"),
    limit: int = Query(20, ge=1, le=100),
    nextToken: str | None = Query(None),
    account_id: str = Depends(get_account_id),
    role: str = Depends(get_account_role),
    svc: ProductService = Depends(get_product_service),
):
    filters = {
        "category": category,
        "genders": genders,
        "colors": colors,
        "min_price": minPrice,
        "max_price": maxPrice,
        "min_rating": minRating,
    }
    try:
        return svc.search_products(
            account_id, q, filters, sortBy, limit, nextToken,
            include_drafts=_can_see_drafts(role), publish=publish,
        )
    except InvalidTokenError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("", dependencies=[Depends(PermissionChecker(required_permissions=["admin", "user"]))])
async def list_products(
    account_id: str = Depends(get_account_id),
    role: str = Depends(get_account_role),
    svc: ProductService = Depends(get_product_service)
):
    return svc.list_products(account_id, include_drafts=_can_see_drafts(role))


@router.post("", response_model=ProductOut, dependencies=[Depends(PermissionChecker(required_permissions=["admin"]))])
async def create_product(
    payload: ProductCreate,
    account_id: str = Depends(get_account_id),
    svc: ProductService = Depends(get_product_service)
):
    return svc.create_product(payload, account_id)


@router.get("/{product_id}", dependencies=[Depends(PermissionChecker(required_permissions=["admin", "user"]))])
async def get_product(
    product_id: str,
    account_id: str = Depends(get_account_id),
    role: str = Depends(get_account_role),
    svc: ProductService = Depends(get_product_service)
):
    p = svc.get_product(product_id, account_id, include_drafts=_can_see_drafts(role))
    if not p:
        raise HTTPException(status_code=404, detail="Product not found")
    return p


@router.put("/{product_id}", dependencies=[Depends(PermissionChecker(required_permissions=["admin"]))])
async def update_product(
    product_id: str,
    payload: ProductUpdate,
    account_id: str = Depends(get_account_id),
    svc: ProductService = Depends(get_product_service)
):
    try:
        item = svc.update_product(product_id, account_id, payload)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    if not item:
        raise HTTPException(status_code=404, detail="Product not found or not updated")
    return item


@router.delete("/{product_id}", dependencies=[Depends(PermissionChecker(required_permissions=["admin"]))])
async def delete_product(
    product_id: str,
    account_id: str = Depends(get_account_id),
    svc: ProductService = Depends(get_product_service)
):
    existing = svc.delete_product(product_id, account_id)
    if not existing:
        raise HTTPException(status_code=404, detail="Product not found")
    return


@router.post("/{product_id}/generate-presigned-urls", dependencies=[Depends(PermissionChecker(required_permissions=["admin"]))])
async def generate_product_presigned_urls(
    product_id: str,
    files: list[ImageFileSpec] = Body(..., embed=False),
    account_id: str = Depends(get_account_id),
    svc: ProductService = Depends(get_product_service)
):
    """Generate presigned URLs for uploading product images to S3 (images only, max 5 MB)."""
    product = svc.get_product(product_id, account_id)
    if not product:
        raise HTTPException(status_code=404, detail=f"Product {product_id} not found")

    try:
        result = svc.generate_put_presigned_urls(product_id=product_id, account_id=account_id, files=files)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error generating presigned URLs: {str(e)}")

    return {"urls": result}


@router.post("/{product_id}/add_images", dependencies=[Depends(PermissionChecker(required_permissions=["admin"]))])
async def add_product_images(
    product_id: str,
    file_names: list[str] = Body(..., embed=False),
    account_id: str = Depends(get_account_id),
    svc: ProductService = Depends(get_product_service)
):
    """Add image keys to product after successful upload to S3"""
    try:
        added_images = svc.add_images(product_id, account_id, file_names)
        return {"added_images": added_images}
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error adding images: {str(e)}")
