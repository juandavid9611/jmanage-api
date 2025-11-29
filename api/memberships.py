from fastapi import APIRouter, Depends, HTTPException
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

@router.post("/{user_id}", dependencies=[Depends(PermissionChecker(required_permissions=['admin']))])
async def create_membership(
    user_id: str,
    role: str = "user",
    account_id: str = Depends(get_account_id),
    svc: MembershipService = Depends(get_membership_service)
):
    """Create a membership for a user in the current account (admin only)"""
    try:
        svc.create_membership(user_id, account_id, role, "active")
        return {"message": f"Membership created for user {user_id} in account {account_id}"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/{user_id}", dependencies=[Depends(PermissionChecker(required_permissions=['admin']))])
async def delete_membership(
    user_id: str,
    account_id: str = Depends(get_account_id),
    svc: MembershipService = Depends(get_membership_service)
):
    """Delete a user's membership from the current account (admin only)"""
    try:
        svc.delete_membership(user_id, account_id)
        return {"message": f"Membership deleted for user {user_id} from account {account_id}"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/{user_id}/enable", dependencies=[Depends(PermissionChecker(required_permissions=['admin']))])
async def enable_membership(
    user_id: str,
    account_id: str = Depends(get_account_id),
    svc: MembershipService = Depends(get_membership_service)
):
    """Enable a user's membership in the current account (admin only)"""
    try:
        svc.enable_membership(user_id, account_id)
        return {"message": f"Membership enabled for user {user_id} in account {account_id}"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/{user_id}/disable", dependencies=[Depends(PermissionChecker(required_permissions=['admin']))])
async def disable_membership(
    user_id: str,
    account_id: str = Depends(get_account_id),
    svc: MembershipService = Depends(get_membership_service)
):
    """Disable a user's membership in the current account (admin only)"""
    try:
        svc.disable_membership(user_id, account_id)
        return {"message": f"Membership disabled for user {user_id} in account {account_id}"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
