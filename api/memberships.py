from fastapi import APIRouter, Depends
from di import get_membership_service
from services.membership_service import MembershipService
from auth import get_current_user

router = APIRouter(prefix="/memberships", tags=["memberships"])

@router.get("/my-memberships")
async def get_my_memberships(
    user: dict = Depends(get_current_user),
    service: MembershipService = Depends(get_membership_service)
):
    """Get active memberships for the current user"""
    user_id = user.get("sub")
    return service.get_user_memberships(user_id)
