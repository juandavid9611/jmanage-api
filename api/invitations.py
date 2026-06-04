from fastapi import APIRouter, Depends, HTTPException

from auth import PermissionChecker, get_account_id
from di import get_tournament_invitation_service
from services.tournament_invitation_service import TournamentInvitationService


router = APIRouter(prefix="/tournaments", tags=["invitations"])
ADMIN = PermissionChecker(required_permissions=["admin"])


@router.get("/{tournament_id}/invitations", dependencies=[Depends(ADMIN)])
def list_invitations(
    tournament_id: str,
    account_id: str = Depends(get_account_id),
    svc: TournamentInvitationService = Depends(get_tournament_invitation_service),
):
    return svc.list_for_tournament(account_id=account_id, tournament_id=tournament_id)


@router.post("/{tournament_id}/teams/{team_id}/invitations/resend", dependencies=[Depends(ADMIN)])
def resend_invitation(
    tournament_id: str,
    team_id: str,
    account_id: str = Depends(get_account_id),
    svc: TournamentInvitationService = Depends(get_tournament_invitation_service),
):
    try:
        return svc.resend(account_id=account_id, tournament_team_id=team_id)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.delete("/{tournament_id}/teams/{team_id}/invitations", dependencies=[Depends(ADMIN)])
def revoke_invitation(
    tournament_id: str,
    team_id: str,
    account_id: str = Depends(get_account_id),
    svc: TournamentInvitationService = Depends(get_tournament_invitation_service),
):
    try:
        svc.revoke(account_id=account_id, tournament_team_id=team_id)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return {"status": "ok"}
