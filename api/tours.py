from di import get_tour_service
from auth import PermissionChecker, get_account_id
from api.schemas.files import FileSpec
from services.tour_service import TourService
from api.schemas.tours import PatchProperty, PutTour
from fastapi import APIRouter, Body, Depends, HTTPException


router = APIRouter(prefix="/tours", tags=["tours"])

@router.get("", dependencies=[Depends(PermissionChecker(required_permissions=['admin', 'user']))])
async def list_tours(
    workspace_id: str | None = None,
    tour_type: str | None = None,
    account_id: str = Depends(get_account_id),
    svc: TourService = Depends(get_tour_service)
    ):
    items = svc.list_tours(account_id, group=workspace_id, tour_type=tour_type)
    return items

@router.post("", dependencies=[Depends(PermissionChecker(required_permissions=['admin']))])
async def create_tour(
    put_tour: PutTour, 
    account_id: str = Depends(get_account_id),
    svc: TourService = Depends(get_tour_service)
):
    item = svc.create(put_tour, account_id)
    return item

@router.get("/{tour_id}", dependencies=[Depends(PermissionChecker(required_permissions=['admin', 'user']))])
async def get_tour(
    tour_id: str, 
    account_id: str = Depends(get_account_id),
    svc: TourService = Depends(get_tour_service)
):
    item = svc.get(tour_id, account_id)
    if not item:
        raise HTTPException(status_code=404, detail=f"Tour {tour_id} not found")
    return item

@router.put("/{tour_id}", dependencies=[Depends(PermissionChecker(required_permissions=['admin']))])
async def update_tour(
    tour_id: str, 
    put_tour: PutTour, 
    account_id: str = Depends(get_account_id),
    svc: TourService = Depends(get_tour_service)
):
    existing_item = svc.get(tour_id, account_id)
    if not existing_item:
        raise HTTPException(status_code=404, detail=f"Tour {tour_id} not found")
    svc.update(tour_id, account_id, put_tour)
    return {"updated_tour_id": tour_id}

@router.delete("/{tour_id}", dependencies=[Depends(PermissionChecker(required_permissions=['admin']))])
async def delete_tour(
    tour_id: str, 
    account_id: str = Depends(get_account_id),
    svc: TourService = Depends(get_tour_service)
):
    svc.delete(tour_id, account_id)
    return {"deleted_tour_id": tour_id}

@router.post("/{tour_id}/generate-presigned-urls", dependencies=[Depends(PermissionChecker(required_permissions=['admin']))])
async def generate_tour_presigned_urls(
    tour_id: str, 
    files: list[FileSpec] = Body(..., embed=False), 
    account_id: str = Depends(get_account_id),
    svc: TourService = Depends(get_tour_service)
    ):
    tour = svc.get(tour_id, account_id)
    if not tour:
        raise HTTPException(status_code=404, detail=f"Tour {tour_id} not found")

    try:
        result = svc.generate_put_presigned_urls(tour_id=tour_id, account_id=account_id, files=files)
    except Exception as e:
        raise HTTPException(status_code=404, detail=f"Error generating presigned URLs: {str(e)}")

    return {"urls": result}

@router.post("/{tour_id}/add_images", dependencies=[Depends(PermissionChecker(required_permissions=['admin']))])
async def add_tour_images(
    tour_id: str, 
    file_names: list[str] = Body(..., embed=False), 
    account_id: str = Depends(get_account_id),
    svc: TourService = Depends(get_tour_service)
):
    added_images = svc.add_images(tour_id, account_id, file_names)
    return {"added_images": added_images}

@router.patch("/{tour_id}/bookers/{booker_id}", dependencies=[Depends(PermissionChecker(required_permissions=['admin']))])
async def update_booker_property(
        tour_id: str, 
        booker_id: str, 
        patch_property: PatchProperty, 
        account_id: str = Depends(get_account_id),
        svc: TourService = Depends(get_tour_service)
        ):
    existing_item = svc.get(tour_id, account_id)
    if not existing_item:
        raise HTTPException(status_code=404, detail=f"Tour {tour_id} not found")

    updated_bookers = svc.update_booker_property(tour_id, account_id, booker_id, patch_property)
    return {"updated_booker_properties": updated_bookers}
