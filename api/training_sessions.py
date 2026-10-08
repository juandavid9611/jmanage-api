"""Training sessions API — coach drafts, admin reviews. Club accounts only."""

from fastapi import APIRouter, Depends, HTTPException, Query

from auth import (
    ClubAccountChecker,
    WorkspacePermissionChecker,
    get_account_id,
    get_current_user,
)
from api.schemas.training_sessions import ReviewIn, SessionIn
from di import get_training_session_service
from repositories.training_session_repo_ddb import StatusConflict
from services.training_session_service import InvalidTransition, TrainingSessionService


# team_owner is outside the role hierarchy, so it is named explicitly; admin passes via the hierarchy.
ALL_ROLES = WorkspacePermissionChecker(required_permissions=["user", "team_owner"])
AUTHORS = WorkspacePermissionChecker(required_permissions=["coach", "team_owner"])
REVIEWERS = WorkspacePermissionChecker(required_permissions=["admin", "team_owner"])

router = APIRouter(
    prefix="/training-sessions",
    tags=["training-sessions"],
    dependencies=[Depends(ClubAccountChecker())],
)


def _run(fn):
    """Map service errors to HTTP: invalid input 400, wrong state 409, missing 404."""
    try:
        result = fn()
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except (InvalidTransition, StatusConflict) as e:
        raise HTTPException(status_code=409, detail=str(e) or "Session status changed, reload and retry")
    if result is None:
        raise HTTPException(status_code=404, detail="Training session not found")
    return result


@router.get("", dependencies=[Depends(ALL_ROLES)])
async def list_sessions(
    workspace_id: str = Query(...),
    account_id: str = Depends(get_account_id),
    svc: TrainingSessionService = Depends(get_training_session_service),
):
    return svc.list_sessions(workspace_id, account_id)


@router.post("", dependencies=[Depends(AUTHORS)])
async def create_session(
    body: SessionIn,
    workspace_id: str = Query(...),
    account_id: str = Depends(get_account_id),
    user: dict = Depends(get_current_user),
    svc: TrainingSessionService = Depends(get_training_session_service),
):
    return _run(lambda: svc.create_session(workspace_id, body, user.get("sub", ""), account_id))


@router.get("/{session_id}", dependencies=[Depends(ALL_ROLES)])
async def get_session(
    session_id: str,
    workspace_id: str = Query(...),
    account_id: str = Depends(get_account_id),
    svc: TrainingSessionService = Depends(get_training_session_service),
):
    return _run(lambda: svc.get_session(session_id, workspace_id, account_id))


@router.put("/{session_id}", dependencies=[Depends(AUTHORS)])
async def update_session(
    session_id: str,
    body: SessionIn,
    workspace_id: str = Query(...),
    account_id: str = Depends(get_account_id),
    svc: TrainingSessionService = Depends(get_training_session_service),
):
    return _run(lambda: svc.update_session(session_id, workspace_id, account_id, body))


@router.delete("/{session_id}", dependencies=[Depends(AUTHORS)])
async def delete_session(
    session_id: str,
    workspace_id: str = Query(...),
    account_id: str = Depends(get_account_id),
    svc: TrainingSessionService = Depends(get_training_session_service),
):
    _run(lambda: svc.delete_session(session_id, workspace_id, account_id) or None)
    return {"deleted_session_id": session_id}


@router.post("/{session_id}/send", dependencies=[Depends(AUTHORS)])
async def send_session(
    session_id: str,
    workspace_id: str = Query(...),
    account_id: str = Depends(get_account_id),
    svc: TrainingSessionService = Depends(get_training_session_service),
):
    return _run(lambda: svc.send_session(session_id, workspace_id, account_id))


@router.post("/{session_id}/review", dependencies=[Depends(REVIEWERS)])
async def review_session(
    session_id: str,
    body: ReviewIn,
    workspace_id: str = Query(...),
    account_id: str = Depends(get_account_id),
    user: dict = Depends(get_current_user),
    svc: TrainingSessionService = Depends(get_training_session_service),
):
    return _run(lambda: svc.review_session(session_id, workspace_id, account_id, body, user.get("sub", "")))
