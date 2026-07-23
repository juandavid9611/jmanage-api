"""Votations API — player-of-the-month voting."""

from fastapi import APIRouter, Depends, HTTPException, Query

from auth import (
    PermissionChecker,
    WorkspacePermissionChecker,
    get_account_id,
    get_current_user,
)
from di import get_votation_service
from services.votation_service import VotationService
from api.schemas.votations import CastVote, CreateVotation


ADMIN = WorkspacePermissionChecker(required_permissions=["admin"])
ALL_ROLES = WorkspacePermissionChecker(required_permissions=["admin", "user"])

router = APIRouter(prefix="/votations", tags=["votations"])


@router.get("/preview", dependencies=[Depends(ADMIN)])
async def preview_candidates(
    workspace_id: str = Query(...),
    period_type: str = Query("month"),
    month: str | None = Query(None, description="YYYY-MM"),
    start_date: str | None = Query(None, description="YYYY-MM-DD"),
    end_date: str | None = Query(None, description="YYYY-MM-DD"),
    min_pct: int = Query(70, ge=0, le=100),
    account_id: str = Depends(get_account_id),
    svc: VotationService = Depends(get_votation_service),
):
    if period_type == "semester":
        if not start_date or not end_date:
            raise HTTPException(status_code=400, detail="start_date and end_date are required for semester preview")
        return svc.preview_candidates(workspace_id, min_pct, account_id, start_date=start_date, end_date=end_date)
    if not month:
        raise HTTPException(status_code=400, detail="month is required for month preview")
    return svc.preview_candidates(workspace_id, min_pct, account_id, month=month)


@router.get("", dependencies=[Depends(ALL_ROLES)])
async def list_votations(
    workspace_id: str = Query(...),
    account_id: str = Depends(get_account_id),
    svc: VotationService = Depends(get_votation_service),
):
    return svc.list_votations(workspace_id, account_id)


@router.post("", dependencies=[Depends(ADMIN)])
async def create_votation(
    body: CreateVotation,
    account_id: str = Depends(get_account_id),
    user: dict = Depends(get_current_user),
    svc: VotationService = Depends(get_votation_service),
):
    return svc.create_votation(
        workspace_id=body.workspace_id,
        min_pct=body.min_pct,
        candidates=[c.model_dump() for c in body.candidates],
        created_by=user.get("sub", ""),
        account_id=account_id,
        period_type=body.period_type,
        month=body.month,
        start_date=body.start_date,
        end_date=body.end_date,
    )


@router.get("/{votation_id}", dependencies=[Depends(ALL_ROLES)])
async def get_votation(
    votation_id: str,
    account_id: str = Depends(get_account_id),
    svc: VotationService = Depends(get_votation_service),
):
    item = svc.get_votation(votation_id, account_id)
    if not item:
        raise HTTPException(status_code=404, detail="Votation not found")
    return item


@router.post("/{votation_id}/vote", dependencies=[Depends(ALL_ROLES)])
async def cast_vote(
    votation_id: str,
    body: CastVote,
    account_id: str = Depends(get_account_id),
    user: dict = Depends(get_current_user),
    svc: VotationService = Depends(get_votation_service),
):
    try:
        return svc.cast_vote(
            votation_id=votation_id,
            voter_id=user["sub"],
            candidate_id=body.candidate_id,
            account_id=account_id,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.delete("/{votation_id}", dependencies=[Depends(ADMIN)])
async def delete_votation(
    votation_id: str,
    workspace_id: str = Query(...),
    account_id: str = Depends(get_account_id),
    svc: VotationService = Depends(get_votation_service),
):
    item = svc.get_votation(votation_id, account_id)
    if not item:
        raise HTTPException(status_code=404, detail="Votation not found")
    svc.delete_votation(votation_id, workspace_id, account_id)
    return {"deleted_votation_id": votation_id}


@router.post("/{votation_id}/tiebreaker", dependencies=[Depends(ADMIN)])
async def create_tiebreaker(
    votation_id: str,
    workspace_id: str = Query(...),
    account_id: str = Depends(get_account_id),
    user: dict = Depends(get_current_user),
    svc: VotationService = Depends(get_votation_service),
):
    result = svc.create_tiebreaker(votation_id, workspace_id, account_id, user.get("sub", ""))
    if not result:
        raise HTTPException(status_code=400, detail="Cannot create tiebreaker: votation not in tied state or tiebreaker already exists")
    return result


@router.patch("/{votation_id}/close", dependencies=[Depends(ADMIN)])
async def close_votation(
    votation_id: str,
    account_id: str = Depends(get_account_id),
    svc: VotationService = Depends(get_votation_service),
):
    item = svc.close_votation(votation_id, account_id)
    if not item:
        raise HTTPException(status_code=404, detail="Votation not found or not open")
    return item
