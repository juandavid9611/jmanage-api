from api.schemas.workspaces import PutWorkspace, CreateWorkspace
from di import get_workspace_service
from auth import PermissionChecker, get_current_user, get_account_id, get_account_role
from fastapi import APIRouter, Depends, HTTPException, Query

from services.workspace_service import WorkspaceService

router = APIRouter(prefix="/workspaces", tags=["workspaces"])


@router.get("", dependencies=[Depends(PermissionChecker(required_permissions=['admin', 'user']))])
async def list_workspaces(
    user: dict = Depends(get_current_user),
    account_id: str = Depends(get_account_id),
    svc: WorkspaceService = Depends(get_workspace_service)
    ):
    return svc.get_related(user, account_id)

@router.post("", dependencies=[Depends(PermissionChecker(required_permissions=['admin']))])
async def create_workspace(
    create_workspace: CreateWorkspace, 
    account_id: str = Depends(get_account_id),
    svc: WorkspaceService = Depends(get_workspace_service)
):
    item = svc.create(create_workspace, account_id)
    return item

@router.get("/{workspace_id}", dependencies=[Depends(PermissionChecker(required_permissions=['admin', 'user']))])
async def get_workspace(
    workspace_id: str, 
    account_id: str = Depends(get_account_id),
    svc: WorkspaceService = Depends(get_workspace_service)
):
    item = svc.get(workspace_id, account_id)
    if not item:
        raise HTTPException(status_code=404, detail=f"Workspace {workspace_id} not found")
    return item

@router.put("/{workspace_id}", dependencies=[Depends(PermissionChecker(required_permissions=['admin']))])
async def update_workspace(
    workspace_id: str, 
    put_workspace: PutWorkspace, 
    account_id: str = Depends(get_account_id),
    svc: WorkspaceService = Depends(get_workspace_service)
):
    existing_item = svc.get(workspace_id, account_id)
    if not existing_item:
        raise HTTPException(status_code=404, detail=f"Workspace {workspace_id} not found")
    svc.update(workspace_id, account_id, put_workspace)
    return {"updated_workspace_id": workspace_id}

@router.delete("/{workspace_id}", dependencies=[Depends(PermissionChecker(required_permissions=['admin']))])
async def delete_workspace(
    workspace_id: str, 
    account_id: str = Depends(get_account_id),
    svc: WorkspaceService = Depends(get_workspace_service)
):
    svc.delete(workspace_id, account_id)
    return {"deleted_workspace_id": workspace_id}
