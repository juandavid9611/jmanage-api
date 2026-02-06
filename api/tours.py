from di import get_tour_service
from auth import PermissionChecker, WorkspacePermissionChecker, get_account_id
from api.schemas.files import FileSpec
from services.tour_service import TourService
from api.schemas.tours import PatchProperty, PutTour
from fastapi import APIRouter, Body, Depends, HTTPException, Query




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

@router.post(
    "",
    dependencies=[Depends(WorkspacePermissionChecker(required_permissions=['admin']))]
)
async def create_tour(
    put_tour: PutTour,
    workspace_id: str = Query(..., description="Workspace ID for this tour"),
    account_id: str = Depends(get_account_id),
    svc: TourService = Depends(get_tour_service)
):
    """Create tour (requires workspace admin permission)"""
    # Set workspace in tour data if not already set
    if not put_tour.group:
        put_tour.group = workspace_id
    elif put_tour.group != workspace_id:
        raise HTTPException(
            status_code=400,
            detail="Tour group must match workspace_id"
        )
    
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

@router.put(
    "/{tour_id}",
    dependencies=[Depends(WorkspacePermissionChecker(required_permissions=['admin']))]
)
async def update_tour(
    tour_id: str,
    put_tour: PutTour,
    workspace_id: str = Query(..., description="Workspace ID that owns this tour"),
    account_id: str = Depends(get_account_id),
    svc: TourService = Depends(get_tour_service)
):
    """Update a tour (requires workspace admin permission)"""
    # Fetch and verify tour exists
    existing_item = svc.get(tour_id, account_id)
    if not existing_item:
        raise HTTPException(status_code=404, detail=f"Tour {tour_id} not found")
    
    # Verify tour belongs to workspace
    if existing_item.get("group") != workspace_id:
        raise HTTPException(
            status_code=400,
            detail=f"Tour does not belong to workspace {workspace_id}"
        )
    
    svc.update(tour_id, account_id, put_tour)
    return {"updated_tour_id": tour_id}


@router.delete(
    "/{tour_id}",
    dependencies=[Depends(WorkspacePermissionChecker(required_permissions=['admin']))]
)
async def delete_tour(
    tour_id: str,
    workspace_id: str = Query(..., description="Workspace ID that owns this tour"),
    account_id: str = Depends(get_account_id),
    svc: TourService = Depends(get_tour_service)
):
    """
    Delete a tour (requires workspace admin permission)
    
    Permissions are validated by WorkspacePermissionChecker dependency.
    Account admins automatically bypass workspace checks.
    """
    # Fetch tour and verify it exists
    tour = svc.get(tour_id, account_id)
    if not tour:
        raise HTTPException(status_code=404, detail=f"Tour {tour_id} not found")
    
    # Verify tour belongs to the specified workspace
    tour_workspace = tour.get("group")
    if tour_workspace != workspace_id:
        raise HTTPException(
            status_code=400,
            detail=f"Tour does not belong to workspace {workspace_id}"
        )
    
    svc.delete(tour_id, account_id)
    return {"deleted_tour_id": tour_id}


@router.post(
    "/{tour_id}/generate-presigned-urls",
    dependencies=[Depends(WorkspacePermissionChecker(required_permissions=['admin']))]
)
async def generate_tour_presigned_urls(
    tour_id: str,
    files: list[FileSpec] = Body(..., embed=False),
    workspace_id: str = Query(..., description="Workspace ID that owns this tour"),
    account_id: str = Depends(get_account_id),
    svc: TourService = Depends(get_tour_service)
):
    """Generate presigned URLs for tour (requires workspace admin permission)"""
    # Fetch and verify tour exists
    tour = svc.get(tour_id, account_id)
    if not tour:
        raise HTTPException(status_code=404, detail=f"Tour {tour_id} not found")
    
    # Verify tour belongs to workspace
    if tour.get("group") != workspace_id:
        raise HTTPException(
            status_code=400,
            detail=f"Tour does not belong to workspace {workspace_id}"
        )

    try:
        result = svc.generate_put_presigned_urls(tour_id=tour_id, account_id=account_id, files=files)
    except Exception as e:
        raise HTTPException(status_code=404, detail=f"Error generating presigned URLs: {str(e)}")

    return {"urls": result}

@router.post(
    "/{tour_id}/add_images",
    dependencies=[Depends(WorkspacePermissionChecker(required_permissions=['admin']))]
)
async def add_tour_images(
    tour_id: str,
    file_names: list[str] = Body(..., embed=False),
    workspace_id: str = Query(..., description="Workspace ID that owns this tour"),
    account_id: str = Depends(get_account_id),
    svc: TourService = Depends(get_tour_service)
):
    """Add images to tour (requires workspace admin permission)"""
    # Fetch and verify tour exists
    tour = svc.get(tour_id, account_id)
    if not tour:
        raise HTTPException(status_code=404, detail=f"Tour {tour_id} not found")
    
    # Verify tour belongs to workspace
    if tour.get("group") != workspace_id:
        raise HTTPException(
            status_code=400,
            detail=f"Tour does not belong to workspace {workspace_id}"
        )
    
    added_images = svc.add_images(tour_id, account_id, file_names)
    return {"added_images": added_images}

@router.patch(
    "/{tour_id}/bookers/{booker_id}",
    dependencies=[Depends(WorkspacePermissionChecker(required_permissions=['admin']))]
)
async def update_booker_property(
    tour_id: str,
    booker_id: str,
    patch_property: PatchProperty,
    workspace_id: str = Query(..., description="Workspace ID that owns this tour"),
    account_id: str = Depends(get_account_id),
    svc: TourService = Depends(get_tour_service)
):
    """Update tour booker property (requires workspace admin permission)"""
    # Fetch and verify tour exists
    existing_item = svc.get(tour_id, account_id)
    if not existing_item:
        raise HTTPException(status_code=404, detail=f"Tour {tour_id} not found")
    
    # Verify tour belongs to workspace
    if existing_item.get("group") != workspace_id:
        raise HTTPException(
            status_code=400,
            detail=f"Tour does not belong to workspace {workspace_id}"
        )

    updated_bookers = svc.update_booker_property(tour_id, account_id, booker_id, patch_property)
    return {"updated_booker_properties": updated_bookers}
