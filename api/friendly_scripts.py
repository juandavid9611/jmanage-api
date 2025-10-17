from di import get_user_service
from auth import PermissionChecker, get_current_user
from fastapi import APIRouter, Depends, Query
from services.user_service import UserService


router = APIRouter(tags=["friendly_scripts"])

@router.post("/send_christmas_greetings", dependencies=[Depends(PermissionChecker(required_permissions=['admin']))])
async def send_christmas_greetings(svc: UserService = Depends(get_user_service)):
    svc.send_christmas_greetings()
    return {"message": "Christmas greetings sent successfully."}

@router.get("/health_check")
async def health_check():
    return {"status": "healthy"}

@router.get("/assists_stats", dependencies=[Depends(PermissionChecker(required_permissions=['admin', 'user']))])
async def get_assists_stats_from_token_user(user = Depends(get_current_user), svc: UserService = Depends(get_user_service)):
    return svc.get_assists_stats(user["sub"])

@router.get("/user_assists_stats", dependencies=[Depends(PermissionChecker(required_permissions=['admin']))])
async def get_assists_stats(user_id: str, svc: UserService = Depends(get_user_service)):
    return svc.get_assists_stats(user_id)

@router.get("/top_goals_and_assists", dependencies=[Depends(PermissionChecker(required_permissions=['admin', 'user']))])
async def get_top_goals_and_assists(workspace_id: str = Query(None), svc: UserService = Depends(get_user_service)):
    return svc.get_top_goals_and_assists(workspace_id)
