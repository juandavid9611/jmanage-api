"""Bank accounts + adjustments. Admin of the workspace only (decided in jmanage-web#191)."""
from fastapi import APIRouter, Depends, Query

from api.schemas.bank_accounts import CreateAdjustment, CreateBankAccount, UpdateBankAccount
from auth import ClubAccountChecker, WorkspacePermissionChecker, get_account_id, get_current_user
from di import get_bank_account_service
from services.bank_account_service import BankAccountService

router = APIRouter(
    prefix="/bank-accounts",
    tags=["finance"],
    dependencies=[Depends(ClubAccountChecker()), Depends(WorkspacePermissionChecker(["admin"]))],
)


@router.get("")
async def list_bank_accounts(
    workspace_id: str = Query(...),
    account_id: str = Depends(get_account_id),
    svc: BankAccountService = Depends(get_bank_account_service),
):
    return svc.list_accounts(workspace_id, account_id)


@router.post("")
async def create_bank_account(
    body: CreateBankAccount,
    workspace_id: str = Query(...),
    account_id: str = Depends(get_account_id),
    user: dict = Depends(get_current_user),
    svc: BankAccountService = Depends(get_bank_account_service),
):
    return svc.create(body, workspace_id, account_id, user["sub"])


@router.get("/{bank_account_id}")
async def get_bank_account(
    bank_account_id: str,
    workspace_id: str = Query(...),
    account_id: str = Depends(get_account_id),
    svc: BankAccountService = Depends(get_bank_account_service),
):
    return svc.get(bank_account_id, workspace_id, account_id)


@router.put("/{bank_account_id}")
async def update_bank_account(
    bank_account_id: str,
    body: UpdateBankAccount,
    workspace_id: str = Query(...),
    account_id: str = Depends(get_account_id),
    user: dict = Depends(get_current_user),
    svc: BankAccountService = Depends(get_bank_account_service),
):
    return svc.update(bank_account_id, body, workspace_id, account_id, user["sub"])


@router.delete("/{bank_account_id}")
async def delete_bank_account(
    bank_account_id: str,
    workspace_id: str = Query(...),
    account_id: str = Depends(get_account_id),
    user: dict = Depends(get_current_user),
    svc: BankAccountService = Depends(get_bank_account_service),
):
    """Soft delete (is_active=false). Never hard-deletes; already inactive is a 200 no-op."""
    return svc.deactivate(bank_account_id, workspace_id, account_id, user["sub"])


@router.get("/{bank_account_id}/adjustments")
async def list_adjustments(
    bank_account_id: str,
    workspace_id: str = Query(...),
    account_id: str = Depends(get_account_id),
    svc: BankAccountService = Depends(get_bank_account_service),
):
    return svc.list_adjustments(bank_account_id, workspace_id, account_id)


@router.post("/{bank_account_id}/adjustments")
async def create_adjustment(
    bank_account_id: str,
    body: CreateAdjustment,
    workspace_id: str = Query(...),
    account_id: str = Depends(get_account_id),
    user: dict = Depends(get_current_user),
    svc: BankAccountService = Depends(get_bank_account_service),
):
    return svc.create_adjustment(bank_account_id, body, workspace_id, account_id, user["sub"])
