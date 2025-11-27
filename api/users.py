from api.schemas.files import FileSpec
from di import get_user_service
from auth import PermissionChecker, get_account_id
from services.user_service import UserService
from fastapi import APIRouter, Body, Depends, HTTPException, Query
from api.schemas.users import PutUser, CreateUser, PutUserAvatar, PutUserMetrics

router = APIRouter(prefix="/users", tags=["users"])


@router.get("", dependencies=[Depends(PermissionChecker(required_permissions=['admin', 'user']))])
async def list_users(
    workspace_id: str = Query(None), 
    include_disabled: bool = False, 
    account_id: str = Depends(get_account_id),
    svc: UserService = Depends(get_user_service)
    ):
    items = svc.list_users(account_id, group=workspace_id, include_disabled=include_disabled)
    return {"users": items}

@router.post("") 
async def create_user(
    create_user: CreateUser, 
    account_id: str = Depends(get_account_id),
    svc: UserService = Depends(get_user_service)
):
    item = svc.create(create_user, account_id)
    return item

@router.get("/{user_id}", dependencies=[Depends(PermissionChecker(required_permissions=['admin', 'user']))])
async def get_user(
    user_id: str, 
    account_id: str = Depends(get_account_id),
    svc: UserService = Depends(get_user_service)
):
    item = svc.get(user_id, account_id)
    if not item:
        raise HTTPException(status_code=404, detail=f"User {user_id} not found")
    return item

@router.put("/{user_id}", dependencies=[Depends(PermissionChecker(required_permissions=['admin', 'user']))])
async def update_user(
    user_id: str, 
    put_user: PutUser, 
    account_id: str = Depends(get_account_id),
    svc: UserService = Depends(get_user_service)
):
    existing_item = svc.get(user_id, account_id)
    if not existing_item:
        raise HTTPException(status_code=404, detail=f"User {user_id} not found")
    svc.update(user_id, account_id, put_user)
    return {"updated_user_id": user_id}

@router.delete("/{user_id}", dependencies=[Depends(PermissionChecker(required_permissions=['admin']))])
async def delete_user(
    user_id: str, 
    account_id: str = Depends(get_account_id),
    svc: UserService = Depends(get_user_service)
):
    svc.delete(user_id, account_id)
    return {"deleted_user_id": user_id}

@router.put("/{user_id}/enable", dependencies=[Depends(PermissionChecker(required_permissions=['admin']))])
async def enable_user(
    user_id: str, 
    account_id: str = Depends(get_account_id),
    svc: UserService = Depends(get_user_service)
):
    svc.enable(user_id, account_id)
    return {"enabled_user_id": user_id}

@router.put("/{user_id}/disable", dependencies=[Depends(PermissionChecker(required_permissions=['admin']))])
async def disable_user(
    user_id: str, 
    account_id: str = Depends(get_account_id),
    svc: UserService = Depends(get_user_service)
):
    svc.disable(user_id, account_id)
    return {"disabled_user_id": user_id}

@router.put("/{user_id}/avatar", dependencies=[Depends(PermissionChecker(required_permissions=['admin', 'user']))])
async def update_user_avatar_url(
    user_id: str, 
    user_avatar: PutUserAvatar, 
    account_id: str = Depends(get_account_id),
    svc: UserService = Depends(get_user_service)
):
    svc.update_user_avatar_url(user_id, account_id, user_avatar)
    return {"updated_avatar_url": user_avatar.avatar_url}

@router.post("/{user_id}/generate-presigned-url", dependencies=[Depends(PermissionChecker(required_permissions=['user', 'admin']))])
async def generate_user_presigned_url(
    user_id: str, 
    files: list[FileSpec] = Body(..., embed=False), 
    account_id: str = Depends(get_account_id),
    svc: UserService = Depends(get_user_service)
):
    try:
        result = svc.generate_presigned_urls(user_id=user_id, account_id=account_id, files=files)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error generating presigned URLs: {str(e)}")
    return {"urls": result}

@router.get("/{user_id}/metrics", dependencies=[Depends(PermissionChecker(required_permissions=['admin', 'user']))])
async def get_user_metrics(
    user_id: str, 
    account_id: str = Depends(get_account_id),
    svc: UserService = Depends(get_user_service)
):
    item = svc.get_user_metrics(user_id, account_id)
    if not item:
        raise HTTPException(status_code=404, detail=f"User {user_id} not found")
    return item

@router.put("/{user_id}/metrics", dependencies=[Depends(PermissionChecker(required_permissions=['admin']))])
async def update_user_metrics(
    user_id: str, 
    put_user_metrics: PutUserMetrics, 
    account_id: str = Depends(get_account_id),
    svc: UserService = Depends(get_user_service)
):
    existing_item = svc.get(user_id, account_id)
    if not existing_item:
        raise HTTPException(status_code=404, detail=f"User {user_id} not found")
    dict_metrics = svc.update_metrics(user_id, account_id, put_user_metrics)
    return {"updated_metrics": dict_metrics}

# TODO: This should be improved to use group/workspace context
@router.get("/{user_id}/late_arrives", dependencies=[Depends(PermissionChecker(required_permissions=['admin', 'user']))])
async def get_late_arrives(
    user_id: str, 
    account_id: str = Depends(get_account_id),
    svc: UserService = Depends(get_user_service)
):
    items = svc.get_late_arrives(user_id, account_id)
    return items
