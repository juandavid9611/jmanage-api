from datetime import datetime, timedelta

from pydantic import BaseModel

from auth import PermissionChecker, WorkspacePermissionChecker, get_account_id, get_current_user, get_account_role
from api.schemas.files import FileSpec
from di import (
    get_account_service,
    get_match_event_service,
    get_match_service,
    get_payment_request_service,
    get_tournament_service,
    get_tournament_team_service,
    get_user_service,
)
from fastapi import APIRouter, Body, Depends, Form, HTTPException, Query
from api.schemas.payments import BulkPutPaymentRequest
from services.account_service import AccountService
from services.payment_request_service import PaymentRequestService
from services.tournament_match_event_service import TournamentMatchEventService
from services.tournament_match_service import TournamentMatchService
from services.tournament_service import TournamentService
from services.tournament_team_service import TournamentTeamService
from services.user_service import UserService


router = APIRouter(prefix="/payment_requests", tags=["payment_requests"])

@router.get("", dependencies=[Depends(PermissionChecker(required_permissions=['admin', 'user']))])
async def list_payment_requests(
    user_id: str | None = None, 
    workspace_id: str | None = None,
    account_id: str = Depends(get_account_id),
    current_user: dict = Depends(get_current_user),
    role: str = Depends(get_account_role),
    svc: PaymentRequestService = Depends(get_payment_request_service)
    ):
    # Security: Non-admin users can only see their own payment requests
    if role != 'admin':
        user_id = current_user.get("sub")
    
    items = svc.list_payment_requests(account_id, user_id=user_id, group=workspace_id)
    return items

@router.post(
    "",
    dependencies=[Depends(WorkspacePermissionChecker(required_permissions=['admin']))]
)
async def create_payment_requests(
    put_payment_request: BulkPutPaymentRequest,
    workspace_id: str = Query(..., description="Workspace ID for payment requests"),
    account_id: str = Depends(get_account_id),
    svc: PaymentRequestService = Depends(get_payment_request_service)
):
    """Create payment requests (requires workspace admin permission)"""
    # Set workspace in request data if not already set
    if not put_payment_request.group:
        put_payment_request.group = workspace_id
    elif put_payment_request.group != workspace_id:
        raise HTTPException(
            status_code=400,
            detail="Payment request group must match workspace_id"
        )
    
    items = svc.bulk_create(put_payment_request, account_id)
    return items

@router.get(
    "/{payment_request_id}",
    dependencies=[Depends(PermissionChecker(required_permissions=['user', 'admin']))]
)
async def get_payment_request(
    payment_request_id: str,
    account_id: str = Depends(get_account_id),
    svc: PaymentRequestService = Depends(get_payment_request_service)
):
    item = svc.get(payment_request_id, account_id)
    if not item:
        raise HTTPException(status_code=404, detail=f"Payment Request {payment_request_id} not found")
    return item


@router.put(
    "/{payment_request_id}",
    dependencies=[Depends(WorkspacePermissionChecker(required_permissions=['admin']))]
)
async def update_payment_request(
    payment_request_id: str,
    put_payment_request: BulkPutPaymentRequest,
    workspace_id: str = Query(..., description="Workspace ID that owns this payment request"),
    account_id: str = Depends(get_account_id),
    svc: PaymentRequestService = Depends(get_payment_request_service)
):
    """Update payment request (requires workspace admin permission)"""
    # Fetch and verify payment request exists
    existing_item = svc.get(payment_request_id, account_id)
    if not existing_item:
        raise HTTPException(status_code=404, detail=f"Payment Request {payment_request_id} not found")
    
    # Verify payment request belongs to workspace
    if existing_item.get("group") != workspace_id:
        raise HTTPException(
            status_code=400,
            detail=f"Payment request does not belong to workspace {workspace_id}"
        )
    
    svc.update(payment_request_id, account_id, put_payment_request)
    return {"updated_payment_request_id": payment_request_id}

@router.delete(
    "/{payment_request_id}",
    dependencies=[Depends(WorkspacePermissionChecker(required_permissions=['admin']))]
)
async def delete_payment_request(
    payment_request_id: str,
    workspace_id: str = Query(..., description="Workspace ID that owns this payment request"),
    account_id: str = Depends(get_account_id),
    svc: PaymentRequestService = Depends(get_payment_request_service)
):
    """Delete payment request (requires workspace admin permission)"""
    # Fetch and verify payment request exists
    payment_request = svc.get(payment_request_id, account_id)
    if not payment_request:
        raise HTTPException(status_code=404, detail=f"Payment Request {payment_request_id} not found")
    
    # Verify payment request belongs to workspace
    if payment_request.get("group") != workspace_id:
        raise HTTPException(
            status_code=400,
            detail=f"Payment request does not belong to workspace {workspace_id}"
        )
    
    svc.delete(payment_request_id, account_id)
    return {"deleted_payment_request_id": payment_request_id}

@router.post(
    "/{payment_request_id}/generate-presigned-urls",
    dependencies=[Depends(WorkspacePermissionChecker(required_permissions=['user', 'admin']))]
)
async def generate_payment_request_presigned_urls(
    payment_request_id: str,
    files: list[FileSpec] = Body(..., embed=False),
    workspace_id: str = Query(..., description="Workspace ID that owns this payment request"),
    account_id: str = Depends(get_account_id),
    svc: PaymentRequestService = Depends(get_payment_request_service)
):
    """Generate presigned URLs for payment request (requires workspace membership)"""
    # Fetch and verify payment request exists
    payment_request = svc.get(payment_request_id, account_id)
    if not payment_request:
        raise HTTPException(status_code=404, detail=f"Payment Request {payment_request_id} not found")
    
    # Verify payment request belongs to workspace
    if payment_request.get("group") != workspace_id:
        raise HTTPException(
            status_code=400,
            detail=f"Payment request does not belong to workspace {workspace_id}"
        )

    try:
        result = svc.generate_put_presigned_urls(payment_request_id=payment_request_id, account_id=account_id, files=files)
    except Exception as e:
        raise HTTPException(status_code=404, detail=f"Error generating presigned URLs: {str(e)}")

    return {"urls": result}

@router.post(
    "/{payment_request_id}/request_approval",
    dependencies=[Depends(WorkspacePermissionChecker(required_permissions=['user', 'admin']))]
)
async def request_payment_request_approval(
    payment_request_id: str,
    file_names: list[str] = Body(..., embed=False),
    workspace_id: str = Query(..., description="Workspace ID that owns this payment request"),
    account_id: str = Depends(get_account_id),
    svc: PaymentRequestService = Depends(get_payment_request_service)
):
    """Request payment approval (requires workspace membership)"""
    # Fetch and verify payment request exists
    payment_request = svc.get(payment_request_id, account_id)
    if not payment_request:
        raise HTTPException(status_code=404, detail=f"Payment Request {payment_request_id} not found")
    
    # Verify payment request belongs to workspace
    if payment_request.get("group") != workspace_id:
        raise HTTPException(
            status_code=400,
            detail=f"Payment request does not belong to workspace {workspace_id}"
        )
    
    user = payment_request["paymentRequestTo"]
    if payment_request["status"] != "pending" and payment_request["status"] != "overdue":
        raise HTTPException(status_code=400, detail=f"Payment Request {payment_request_id} is not in pending or overdue status")
    if not file_names:
        raise HTTPException(status_code=400, detail="No files were uploaded")
    
    return {"requested_payment_request_approval_id": svc.request_payment_request_approval(payment_request_id, account_id, file_names)}


_CARD_YELLOW_TYPES = {"yellow_card"}
_CARD_RED_TYPES = {"red_card", "second_yellow"}


class TournamentMatchChargesRequest(BaseModel):
    tournamentId: str
    matchId: str


@router.post(
    "/tournament-match-charges",
    dependencies=[Depends(PermissionChecker(required_permissions=['admin']))]
)
async def create_tournament_match_charges(
    body: TournamentMatchChargesRequest,
    account_id: str = Depends(get_account_id),
    t_svc: TournamentService = Depends(get_tournament_service),
    m_svc: TournamentMatchService = Depends(get_match_service),
    ev_svc: TournamentMatchEventService = Depends(get_match_event_service),
    team_svc: TournamentTeamService = Depends(get_tournament_team_service),
    user_svc: UserService = Depends(get_user_service),
    pr_svc: PaymentRequestService = Depends(get_payment_request_service),
    account_svc: AccountService = Depends(get_account_service),
):
    tournament = t_svc.get_tournament(body.tournamentId, account_id)
    if not tournament:
        raise HTTPException(status_code=404, detail="Tournament not found")
    if not tournament.get("payments_enabled"):
        raise HTTPException(status_code=400, detail="Payments not enabled for this tournament")

    match = m_svc.get_match(body.matchId)
    if not match or match.get("tournament_id") != body.tournamentId:
        raise HTTPException(status_code=404, detail="Match not found")
    if match.get("status") != "finished":
        raise HTTPException(status_code=400, detail="Match is not finished")

    # Charges must land in the account's real workspace (same convention as
    # tournament_invitation_service._accept: "group" everywhere else means
    # workspace_id, and tournaments have no workspace_id of their own), so the
    # normal Pagos/Pagos Totales pages pick them up without a tournament-specific view.
    account = account_svc.get(account_id)
    workspace_id = (account.get("settings") or {}).get("default_workspace") if account else None
    if not workspace_id:
        raise HTTPException(status_code=400, detail="Account has no default workspace configured")

    rules = tournament.get("rules") or {}
    yellow_fee = int(rules.get("yellow_card_fee") or 0)
    red_fee = int(rules.get("red_card_fee") or 0)

    events = ev_svc.list_events(body.matchId)
    card_events = [ev for ev in events if ev.get("type") in (_CARD_YELLOW_TYPES | _CARD_RED_TYPES)]

    existing_prs = pr_svc.list_payment_requests(account_id, group=workspace_id)
    charged_event_ids = {pr.get("reference") for pr in existing_prs if pr.get("reference")}

    today = datetime.utcnow().date().isoformat()
    due = (datetime.utcnow() + timedelta(days=30)).date().isoformat()
    tournament_name = tournament.get("name", "Torneo")

    team_cache: dict[str, dict] = {}
    for team_id in {ev.get("team_id") for ev in card_events if ev.get("team_id")}:
        team = team_svc.get_team(team_id)
        if team:
            team_cache[team_id] = team

    # Build recipient info per team.
    # Priority: owner_user_id (invitation-accepted) → contact_email user lookup → contact_email direct.
    RecipientInfo = tuple  # (id, email, name)
    recipient_by_team: dict[str, RecipientInfo] = {}
    for tid, team in team_cache.items():
        owner_id = team.get("owner_user_id")
        if owner_id:
            user = user_svc.repo.get(owner_id, account_id)
            if user:
                recipient_by_team[tid] = (user["id"], user.get("email", ""), user.get("name") or team.get("name", "Equipo"))
                continue
        contact_email = (team.get("contact_email") or "").strip()
        if contact_email:
            user = user_svc.repo.get_by_email(contact_email)
            if user:
                recipient_by_team[tid] = (user["id"], user.get("email", contact_email), user.get("name") or team.get("name", "Equipo"))
            else:
                recipient_by_team[tid] = (tid, contact_email, team.get("name", "Equipo"))

    created = 0
    skipped_already_charged = 0
    skipped_no_team = 0
    skipped_no_manager = 0
    skipped_fee_zero = 0

    for ev in card_events:
        if ev.get("id") in charged_event_ids:
            skipped_already_charged += 1
            continue

        team_id = ev.get("team_id")
        team = team_cache.get(team_id)
        if not team:
            skipped_no_team += 1
            continue

        recipient = recipient_by_team.get(team_id)
        if not recipient:
            skipped_no_manager += 1
            continue

        ev_type = ev.get("type")
        is_red = ev_type in _CARD_RED_TYPES
        fee = red_fee if is_red else yellow_fee
        if fee == 0:
            skipped_fee_zero += 1
            continue

        card_label = "Tarjeta Roja" if is_red else "Tarjeta Amarilla"
        recipient_id, recipient_email, recipient_name = recipient
        home_team = team_cache.get(match.get("home_team_id", ""))
        away_team = team_cache.get(match.get("away_team_id", ""))
        home_name = (home_team or {}).get("name") or match.get("home_team_id", "")
        away_name = (away_team or {}).get("name") or match.get("away_team_id", "")

        bulk_item = BulkPutPaymentRequest(
            createDate=today,
            dueDate=due,
            concept=f"{card_label} - {tournament_name}",
            description=f"Partido {match.get('date', '')[:10]}: {home_name} vs {away_name}",
            category="tournament_fine",
            group=workspace_id,
            paymentRequestTo=[{"id": recipient_id, "name": recipient_name, "email": recipient_email}],
            userPrice=fee,
            reference=ev["id"],
        )
        try:
            pr_svc.bulk_create(bulk_item, account_id)
            created += 1
        except Exception as exc:
            print(f"[payments] failed to create charge for team {team_id} event {ev.get('id')}: {exc}")

    return {
        "created": created,
        "card_events_found": len(card_events),
        "skipped_already_charged": skipped_already_charged,
        "skipped_no_team": skipped_no_team,
        "skipped_no_manager": skipped_no_manager,
        "skipped_fee_zero": skipped_fee_zero,
    }
