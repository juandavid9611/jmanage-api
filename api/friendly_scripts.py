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
async def get_assists_stats_from_token_user(
    user = Depends(get_current_user), 
    account_id: str = Depends(get_account_id),
    svc: UserService = Depends(get_user_service)
):
    return svc.get_assists_stats(user["sub"], account_id)

@router.get("/user_assists_stats", dependencies=[Depends(PermissionChecker(required_permissions=['admin']))])
async def get_assists_stats(
    user_id: str, 
    account_id: str = Depends(get_account_id),
    svc: UserService = Depends(get_user_service)
):
    return svc.get_assists_stats(user_id, account_id)

@router.get("/top_goals_and_assists", dependencies=[Depends(PermissionChecker(required_permissions=['admin', 'user']))])
async def get_top_goals_and_assists(
    account_id: str = Depends(get_account_id),
    workspace_id: str = Query(None), 
    svc: UserService = Depends(get_user_service)
):
    return svc.get_top_goals_and_assists(account_id, workspace_id)

@router.get("/raise_error", dependencies=[Depends(PermissionChecker(required_permissions=['admin']))])
async def raise_error():
    raise HTTPException(status_code=500, detail="This is a test error for monitoring purposes.")