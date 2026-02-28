from fastapi import APIRouter, Depends, HTTPException, Query
from di import get_membership_service
from services.membership_service import MembershipService
from auth import get_current_user, PermissionChecker, get_account_id

router = APIRouter(prefix="/memberships", tags=["memberships"])

@router.get("/my-memberships")
async def get_my_memberships(
    user: dict = Depends(get_current_user),
    svc: MembershipService = Depends(get_membership_service)
):
    """Get active memberships for the current user"""
    user_id = user.get("sub")
    return svc.get_user_memberships(user_id)

@router.put("/my-workspace")
async def update_my_workspace(
    workspace_id: str = Query(..., description="Workspace ID to set as active"),
    user: dict = Depends(get_current_user),
    account_id: str = Depends(get_account_id),
    svc: MembershipService = Depends(get_membership_service)
):
    """Update the current user's active workspace selection.
    
    Validates the user has an active membership for the target workspace.
    """
    user_id = user.get("sub")
    result = svc.update_user_active_workspace(user_id, account_id, workspace_id)
    if not result:
        raise HTTPException(
            status_code=404,
            detail="No active membership found for user in this account"
        )
    return result

@router.post("/{user_id}", dependencies=[Depends(PermissionChecker(required_permissions=['admin']))])
async def create_membership(
    user_id: str,
    workspace_id: str = Query(..., description="Workspace ID for the membership"),
    role: str = Query("user", description="Role for the membership"),
    account_id: str = Depends(get_account_id),
    svc: MembershipService = Depends(get_membership_service)
):
    """Create a membership for a user in a specific workspace (admin only)"""
    try:
        svc.create_membership(user_id, account_id, workspace_id, role, "active")
        return {
            "message": f"Membership created for user {user_id} in workspace {workspace_id}",
            "user_id": user_id,
            "workspace_id": workspace_id,
            "role": role
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/{user_id}", dependencies=[Depends(PermissionChecker(required_permissions=['admin']))])
async def delete_membership(
    user_id: str,
    workspace_id: str = Query(..., description="Workspace ID of the membership to delete"),
    account_id: str = Depends(get_account_id),
    svc: MembershipService = Depends(get_membership_service)
):
    """Delete a user's membership from a specific workspace (admin only)"""
    try:
        svc.delete_membership(user_id, account_id, workspace_id)
        return {
            "message": f"Membership deleted for user {user_id} from workspace {workspace_id}",
            "user_id": user_id,
            "workspace_id": workspace_id
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/{user_id}/enable", dependencies=[Depends(PermissionChecker(required_permissions=['admin']))])
async def enable_membership(
    user_id: str,
    workspace_id: str = Query(..., description="Workspace ID of the membership to enable"),
    account_id: str = Depends(get_account_id),
    svc: MembershipService = Depends(get_membership_service)
):
    """Enable a user's membership in a specific workspace (admin only)"""
    try:
        svc.enable_membership(user_id, account_id, workspace_id)
        return {
            "message": f"Membership enabled for user {user_id} in workspace {workspace_id}",
            "user_id": user_id,
            "workspace_id": workspace_id
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/{user_id}/disable", dependencies=[Depends(PermissionChecker(required_permissions=['admin']))])
async def disable_membership(
    user_id: str,
    workspace_id: str = Query(..., description="Workspace ID of the membership to disable"),
    account_id: str = Depends(get_account_id),
    svc: MembershipService = Depends(get_membership_service)
):
    """Disable a user's membership in a specific workspace (admin only)"""
    try:
        svc.disable_membership(user_id, account_id, workspace_id)
        return {
            "message": f"Membership disabled for user {user_id} in workspace {workspace_id}",
            "user_id": user_id,
            "workspace_id": workspace_id
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
