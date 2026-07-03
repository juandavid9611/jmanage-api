from fastapi import APIRouter, Depends

from auth import PermissionChecker, get_account_id
from di import get_tournament_invitation_service
from services.tournament_invitation_service import TournamentInvitationService
from api.schemas.invitations import CreateAdminInvitationRequest


router = APIRouter(prefix="/admin-invitations", tags=["admin-invitations"])
ADMIN = PermissionChecker(required_permissions=["admin"])


@router.post("", dependencies=[Depends(ADMIN)])
def create_admin_invitation(
    body: CreateAdminInvitationRequest,
    account_id: str = Depends(get_account_id),
    svc: TournamentInvitationService = Depends(get_tournament_invitation_service),
):
    return svc.create_admin_invitation(account_id=account_id, email=body.email)
