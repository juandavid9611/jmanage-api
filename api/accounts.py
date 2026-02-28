from fastapi import APIRouter, Depends, HTTPException
from auth import PermissionChecker, get_account_id, get_current_user
from di import get_account_service
from services.account_service import AccountService
from api.schemas.accounts import CreateAccount, UpdateAccount

router = APIRouter(prefix="/accounts", tags=["accounts"])


@router.get("/me", dependencies=[Depends(PermissionChecker(required_permissions=['admin', 'user']))])
async def get_current_account(
    account_id: str = Depends(get_account_id),
    svc: AccountService = Depends(get_account_service)
):
    """Get current account details"""
    account = svc.get(account_id)
    if not account:
        raise HTTPException(status_code=404, detail="Account not found")
    return account


@router.put("/me", dependencies=[Depends(PermissionChecker(required_permissions=['admin']))])
async def update_current_account(
    update_account: UpdateAccount,
    account_id: str = Depends(get_account_id),
    svc: AccountService = Depends(get_account_service)
):
    """Update current account (admin only)"""
    account = svc.update(account_id, update_account)
    if not account:
        raise HTTPException(status_code=404, detail="Account not found")
    return account


@router.post("", dependencies=[Depends(PermissionChecker(required_permissions=['admin']))])
async def create_account(
    create_account: CreateAccount,
    user: dict = Depends(get_current_user),
    svc: AccountService = Depends(get_account_service)
):
    """Create new account (admin only)"""
    user_id = user["sub"]
    account = svc.create(create_account, owner_user_id=user_id)
    return account


@router.get("/my-accounts", dependencies=[Depends(PermissionChecker(required_permissions=['admin', 'user']))])
async def get_my_accounts(
    user: dict = Depends(get_current_user),
    svc: AccountService = Depends(get_account_service)
):
    """Get all accounts the current user belongs to"""
    user_id = user["sub"]
    accounts = svc.get_user_accounts(user_id)
    return accounts
