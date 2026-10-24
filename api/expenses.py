"""Expenses (egresos) with an approval flow and a payments ledger. Admin of the workspace only."""
from fastapi import APIRouter, Body, Depends, Query

from api.schemas.expenses import CreateExpense, ExpenseFileSpec, MarkPaid, RejectExpense, UpdateExpense
from auth import ClubAccountChecker, WorkspacePermissionChecker, get_account_id, get_current_user
from di import get_expense_service
from services.expense_service import ExpenseService

router = APIRouter(
    prefix="/expenses",
    tags=["finance"],
    dependencies=[Depends(ClubAccountChecker()), Depends(WorkspacePermissionChecker(["admin"]))],
)


@router.get("")
async def list_expenses(
    workspace_id: str = Query(...),
    status: str | None = Query(None),
    account_id_filter: str | None = Query(None, alias="account_id", description="Bank account of the latest payment"),
    bank_account_id: str | None = Query(None, description="Same as account_id; use it if the header X-Account-Id is not sent"),
    team: str | None = Query(None),
    category: str | None = Query(None),
    date_from: str | None = Query(None, description="YYYY-MM-DD, inclusive"),
    date_to: str | None = Query(None, description="YYYY-MM-DD, inclusive"),
    account_id: str = Depends(get_account_id),
    svc: ExpenseService = Depends(get_expense_service),
):
    return svc.list_expenses(
        workspace_id, account_id, status=status, bank_account_id=account_id_filter or bank_account_id, team=team,
        category=category, date_from=date_from, date_to=date_to,
    )


@router.post("")
async def create_expense(
    body: CreateExpense,
    workspace_id: str = Query(...),
    account_id: str = Depends(get_account_id),
    user: dict = Depends(get_current_user),
    svc: ExpenseService = Depends(get_expense_service),
):
    return svc.create(body, workspace_id, account_id, user["sub"])


@router.get("/{expense_id}")
async def get_expense(
    expense_id: str,
    workspace_id: str = Query(...),
    account_id: str = Depends(get_account_id),
    svc: ExpenseService = Depends(get_expense_service),
):
    return svc.get(expense_id, workspace_id, account_id)


@router.put("/{expense_id}")
async def update_expense(
    expense_id: str,
    body: UpdateExpense,
    workspace_id: str = Query(...),
    account_id: str = Depends(get_account_id),
    user: dict = Depends(get_current_user),
    svc: ExpenseService = Depends(get_expense_service),
):
    return svc.update(expense_id, body, workspace_id, account_id, user["sub"])


@router.delete("/{expense_id}")
async def delete_expense(
    expense_id: str,
    workspace_id: str = Query(...),
    account_id: str = Depends(get_account_id),
    svc: ExpenseService = Depends(get_expense_service),
):
    return svc.delete(expense_id, workspace_id, account_id)


@router.post("/{expense_id}/approve")
async def approve_expense(
    expense_id: str,
    workspace_id: str = Query(...),
    account_id: str = Depends(get_account_id),
    user: dict = Depends(get_current_user),
    svc: ExpenseService = Depends(get_expense_service),
):
    return svc.approve(expense_id, workspace_id, account_id, user["sub"])


@router.post("/{expense_id}/reject")
async def reject_expense(
    expense_id: str,
    body: RejectExpense,
    workspace_id: str = Query(...),
    account_id: str = Depends(get_account_id),
    user: dict = Depends(get_current_user),
    svc: ExpenseService = Depends(get_expense_service),
):
    return svc.reject(expense_id, body.reason, workspace_id, account_id, user["sub"])


@router.post("/{expense_id}/mark-paid")
async def mark_expense_paid(
    expense_id: str,
    body: MarkPaid,
    workspace_id: str = Query(...),
    account_id: str = Depends(get_account_id),
    user: dict = Depends(get_current_user),
    svc: ExpenseService = Depends(get_expense_service),
):
    """Atomic and idempotent. Send `idempotency_key` (8-64 chars) so a retry or double click cannot pay twice;
    it is optional only until the frontend sends it."""
    return svc.mark_paid(expense_id, body, workspace_id, account_id, user["sub"])


@router.post("/{expense_id}/generate-presigned-urls")
async def generate_expense_presigned_urls(
    expense_id: str,
    files: list[ExpenseFileSpec] = Body(..., embed=False),
    workspace_id: str = Query(...),
    idempotency_key: str | None = Query(None, description="Same key later sent to mark-paid"),
    account_id: str = Depends(get_account_id),
    svc: ExpenseService = Depends(get_expense_service),
):
    return svc.presign_uploads(expense_id, files, idempotency_key, workspace_id, account_id)
