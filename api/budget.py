"""Budget catalog API: items (soft delete) and the per-month quantities. Club accounts, workspace admins."""
from fastapi import APIRouter, Depends, HTTPException, Query

from api.schemas.budget import BudgetItemCreate, BudgetItemUpdate, BudgetQuantityIn
from auth import ClubAccountChecker, WorkspacePermissionChecker, get_account_id
from di import get_budget_service
from services.budget_service import BudgetConflict, BudgetNotFound, BudgetService, BudgetValidationError

CLUB_ONLY = ClubAccountChecker()
ADMIN = WorkspacePermissionChecker(required_permissions=["admin"])

MONTH_QUERY = Query(..., pattern=r"^\d{4}-(0[1-9]|1[0-2])$", description="YYYY-MM")

router = APIRouter(tags=["budget"], dependencies=[Depends(CLUB_ONLY), Depends(ADMIN)])


def _call(fn, *args, **kwargs):
    try:
        return fn(*args, **kwargs)
    except BudgetNotFound as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except BudgetConflict as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    except BudgetValidationError as exc:
        raise HTTPException(status_code=422, detail=str(exc))


@router.get("/budget-items")
async def list_budget_items(
    workspace_id: str = Query(...),
    account_id: str = Depends(get_account_id),
    svc: BudgetService = Depends(get_budget_service),
):
    return svc.list_items(account_id, workspace_id)


@router.post("/budget-items")
async def create_budget_item(
    body: BudgetItemCreate,
    workspace_id: str = Query(...),
    account_id: str = Depends(get_account_id),
    svc: BudgetService = Depends(get_budget_service),
):
    return _call(svc.create_item, account_id, workspace_id, body.category, body.name, body.unit_cost)


@router.put("/budget-items/{item_id}")
async def update_budget_item(
    item_id: str,
    body: BudgetItemUpdate,
    workspace_id: str = Query(...),
    account_id: str = Depends(get_account_id),
    svc: BudgetService = Depends(get_budget_service),
):
    return _call(svc.update_item, account_id, workspace_id, item_id, body.category, body.name,
                 body.unit_cost, body.is_active)


@router.delete("/budget-items/{item_id}")
async def delete_budget_item(
    item_id: str,
    workspace_id: str = Query(...),
    account_id: str = Depends(get_account_id),
    svc: BudgetService = Depends(get_budget_service),
):
    return _call(svc.delete_item, account_id, workspace_id, item_id)


@router.get("/budget-quantities")
async def list_budget_quantities(
    workspace_id: str = Query(...),
    month: str = MONTH_QUERY,
    account_id: str = Depends(get_account_id),
    svc: BudgetService = Depends(get_budget_service),
):
    return _call(svc.list_quantities, account_id, workspace_id, month)


@router.put("/budget-quantities")
async def set_budget_quantities(
    body: list[BudgetQuantityIn],
    workspace_id: str = Query(...),
    month: str = MONTH_QUERY,
    account_id: str = Depends(get_account_id),
    svc: BudgetService = Depends(get_budget_service),
):
    return _call(svc.set_quantities, account_id, workspace_id, month, [e.model_dump() for e in body])
