from api.schemas.workspaces import PutWorkspace, CreateWorkspace
from di import get_workspace_service
from auth import ClubAccountChecker, PermissionChecker, get_current_user, get_account_id, get_account_role
from fastapi import APIRouter, Depends, HTTPException, Query

from services.workspace_service import WorkspaceCreationError, WorkspaceNameConflict, WorkspaceService

router = APIRouter(prefix="/workspaces", tags=["workspaces"])


@router.get("", dependencies=[Depends(PermissionChecker(required_permissions=['admin', 'user', 'team_owner']))])
async def list_workspaces(
    user: dict = Depends(get_current_user),
    account_id: str = Depends(get_account_id),
    svc: WorkspaceService = Depends(get_workspace_service)
    ):
    return svc.get_related(user, account_id)

@router.get("/all", dependencies=[Depends(PermissionChecker(required_permissions=['admin', 'user', 'team_owner']))])
async def list_all_workspaces(
    account_id: str = Depends(get_account_id),
    svc: WorkspaceService = Depends(get_workspace_service)
    ):
    """List ALL workspaces in the account (not filtered by membership)"""
    return svc.list_workspaces(account_id)

@router.post(
    "",
    status_code=201,
    dependencies=[Depends(PermissionChecker(required_permissions=['admin'])), Depends(ClubAccountChecker())],
)
async def create_workspace(
    create_workspace: CreateWorkspace,
    user: dict = Depends(get_current_user),
    account_id: str = Depends(get_account_id),
    svc: WorkspaceService = Depends(get_workspace_service)
):
    """Create a category in the current club account; the creator becomes its admin.

    409 on duplicate name (case-insensitive), 500 if the membership write fails (rolled back).
    """
    try:
        return svc.create_with_admin(create_workspace.name, create_workspace.logo, account_id, user["sub"])
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except WorkspaceNameConflict as e:
        raise HTTPException(status_code=409, detail=str(e))
    except WorkspaceCreationError as e:
        raise HTTPException(status_code=500, detail=str(e))

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
