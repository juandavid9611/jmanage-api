from di import get_user_service
from auth import PermissionChecker, get_current_user, get_account_id
from fastapi import APIRouter, Depends, HTTPException, Query
from services.user_service import UserService


router = APIRouter(tags=["friendly_scripts"])

@router.post("/send_christmas_greetings", dependencies=[Depends(PermissionChecker(required_permissions=['admin']))])
async def send_christmas_greetings(
    account_id: str = Depends(get_account_id),
    svc: UserService = Depends(get_user_service)
):
    svc.send_christmas_greetings(account_id)
    return {"message": "Christmas greetings sent successfully."}

@router.get("/health_check")
async def health_check():
    return {"status": "healthy"}

@router.get("/assists_stats", dependencies=[Depends(PermissionChecker(required_permissions=['admin', 'user']))])
async def get_assists_stats(
    user = Depends(get_current_user), 
    workspace_id: str = Query(..., description="Workspace ID for this calendar event"),
    account_id: str = Depends(get_account_id),
    svc: UserService = Depends(get_user_service)
):
    return svc.get_assists_stats(user["sub"], account_id, workspace_id)

@router.get("/top_goals_and_assists", dependencies=[Depends(PermissionChecker(required_permissions=['admin', 'user']))])
async def get_top_goals_and_assists(
    account_id: str = Depends(get_account_id),
    workspace_id: str = Query(None), 
    svc: UserService = Depends(get_user_service)
):
    return svc.get_top_goals_and_assists(account_id, workspace_id)

@router.get("/workspace_assists_stats", dependencies=[Depends(PermissionChecker(required_permissions=['admin', 'user']))])
async def get_workspace_assists_stats(
    workspace_id: str,
    account_id: str = Depends(get_account_id),
    svc: UserService = Depends(get_user_service)
):
    return svc.get_workspace_assists_stats(account_id, workspace_id)

@router.get("/raise_error", dependencies=[Depends(PermissionChecker(required_permissions=['admin']))])
async def raise_error():
    raise HTTPException(status_code=500, detail="This is a test error for monitoring purposes. We got CI/CD working! Congrats!")


@router.get("/wins_draws_loses", dependencies=[Depends(PermissionChecker(required_permissions=['admin', 'user']))])
async def get_wins_draws_loses(
    account_id: str = Depends(get_account_id),
    workspace_id: str = Query(None), 
    svc: UserService = Depends(get_user_service)
):
    return svc.get_wins_draws_loses(account_id, workspace_id)