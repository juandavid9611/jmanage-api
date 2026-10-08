"""Club tournaments API — the club's participation in external tournaments: roster, matches, lineups.

Club accounts only. Reads are open to every workspace role; writes need admin or coach.
"""

from fastapi import APIRouter, Depends, HTTPException, Query

from auth import (
    ClubAccountChecker,
    WorkspacePermissionChecker,
    get_account_id,
    get_current_user,
)
from api.schemas.club_tournaments import (
    LineupIn,
    MatchIn,
    RosterBulkIn,
    RosterEntryIn,
    RosterEntryUpdate,
    TournamentIn,
)
from di import get_club_tournament_service
from services.club_tournament_service import ClubTournamentService


# team_owner is outside the role hierarchy, so it is named explicitly for reads.
ALL_ROLES = WorkspacePermissionChecker(required_permissions=["user", "team_owner"])
MANAGERS = WorkspacePermissionChecker(required_permissions=["admin", "coach"])

router = APIRouter(
    prefix="/club-tournaments",
    tags=["club-tournaments"],
    dependencies=[Depends(ClubAccountChecker())],
)


def _run(fn, what: str = "Tournament"):
    """Map service errors to HTTP: invalid input 400, missing 404."""
    try:
        result = fn()
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    if result is None or result is False:
        raise HTTPException(status_code=404, detail=f"{what} not found")
    return result


@router.get("", dependencies=[Depends(ALL_ROLES)])
async def list_tournaments(
    workspace_id: str = Query(...),
    account_id: str = Depends(get_account_id),
    svc: ClubTournamentService = Depends(get_club_tournament_service),
):
    return svc.list_tournaments(workspace_id, account_id)


@router.post("", dependencies=[Depends(MANAGERS)])
async def create_tournament(
    body: TournamentIn,
    workspace_id: str = Query(...),
    account_id: str = Depends(get_account_id),
    svc: ClubTournamentService = Depends(get_club_tournament_service),
):
    return svc.create_tournament(workspace_id, body, account_id)


# Declared before "/{tournament_id}" so "mine" is not read as an id.
@router.get("/mine", dependencies=[Depends(ALL_ROLES)])
async def my_tournaments(
    workspace_id: str = Query(...),
    account_id: str = Depends(get_account_id),
    user: dict = Depends(get_current_user),
    svc: ClubTournamentService = Depends(get_club_tournament_service),
):
    return svc.my_tournaments(user.get("sub", ""), workspace_id, account_id)


@router.get("/{tournament_id}", dependencies=[Depends(ALL_ROLES)])
async def get_tournament(
    tournament_id: str,
    workspace_id: str = Query(...),
    account_id: str = Depends(get_account_id),
    svc: ClubTournamentService = Depends(get_club_tournament_service),
):
    return _run(lambda: svc.get_tournament(tournament_id, workspace_id, account_id))


@router.put("/{tournament_id}", dependencies=[Depends(MANAGERS)])
async def update_tournament(
    tournament_id: str,
    body: TournamentIn,
    workspace_id: str = Query(...),
    account_id: str = Depends(get_account_id),
    svc: ClubTournamentService = Depends(get_club_tournament_service),
):
    return _run(lambda: svc.update_tournament(tournament_id, workspace_id, account_id, body))


@router.delete("/{tournament_id}", dependencies=[Depends(MANAGERS)])
async def delete_tournament(
    tournament_id: str,
    workspace_id: str = Query(...),
    account_id: str = Depends(get_account_id),
    svc: ClubTournamentService = Depends(get_club_tournament_service),
):
    _run(lambda: svc.delete_tournament(tournament_id, workspace_id, account_id))
    return {"deleted_tournament_id": tournament_id}


# ── Roster ───────────────────────────────────────────────────────────

@router.get("/{tournament_id}/roster", dependencies=[Depends(ALL_ROLES)])
async def list_roster(
    tournament_id: str,
    workspace_id: str = Query(...),
    account_id: str = Depends(get_account_id),
    svc: ClubTournamentService = Depends(get_club_tournament_service),
):
    return _run(lambda: svc.list_roster(tournament_id, workspace_id, account_id))


@router.post("/{tournament_id}/roster", dependencies=[Depends(MANAGERS)])
async def add_roster_entry(
    tournament_id: str,
    body: RosterEntryIn,
    workspace_id: str = Query(...),
    account_id: str = Depends(get_account_id),
    svc: ClubTournamentService = Depends(get_club_tournament_service),
):
    return _run(lambda: svc.add_roster_entry(tournament_id, workspace_id, account_id, body))


@router.post("/{tournament_id}/roster/bulk", dependencies=[Depends(MANAGERS)])
async def bulk_add_roster(
    tournament_id: str,
    body: RosterBulkIn,
    workspace_id: str = Query(...),
    account_id: str = Depends(get_account_id),
    svc: ClubTournamentService = Depends(get_club_tournament_service),
):
    """Always 200 with a per-row result so partial failures are visible."""
    return _run(lambda: svc.bulk_add_roster(tournament_id, workspace_id, account_id, body))


@router.put("/{tournament_id}/roster/{entry_id}", dependencies=[Depends(MANAGERS)])
async def update_roster_entry(
    tournament_id: str,
    entry_id: str,
    body: RosterEntryUpdate,
    workspace_id: str = Query(...),
    account_id: str = Depends(get_account_id),
    svc: ClubTournamentService = Depends(get_club_tournament_service),
):
    return _run(
        lambda: svc.update_roster_entry(tournament_id, entry_id, workspace_id, account_id, body),
        "Roster entry",
    )


@router.delete("/{tournament_id}/roster/{entry_id}", dependencies=[Depends(MANAGERS)])
async def remove_roster_entry(
    tournament_id: str,
    entry_id: str,
    workspace_id: str = Query(...),
    account_id: str = Depends(get_account_id),
    svc: ClubTournamentService = Depends(get_club_tournament_service),
):
    _run(lambda: svc.remove_roster_entry(tournament_id, entry_id, workspace_id, account_id), "Roster entry")
    return {"deleted_roster_entry_id": entry_id}


# ── Matches and lineups ──────────────────────────────────────────────

@router.get("/{tournament_id}/matches", dependencies=[Depends(ALL_ROLES)])
async def list_matches(
    tournament_id: str,
    workspace_id: str = Query(...),
    account_id: str = Depends(get_account_id),
    svc: ClubTournamentService = Depends(get_club_tournament_service),
):
    return _run(lambda: svc.list_matches(tournament_id, workspace_id, account_id))


@router.post("/{tournament_id}/matches", dependencies=[Depends(MANAGERS)])
async def create_match(
    tournament_id: str,
    body: MatchIn,
    workspace_id: str = Query(...),
    account_id: str = Depends(get_account_id),
    svc: ClubTournamentService = Depends(get_club_tournament_service),
):
    return _run(lambda: svc.create_match(tournament_id, workspace_id, account_id, body))


@router.put("/{tournament_id}/matches/{match_id}", dependencies=[Depends(MANAGERS)])
async def update_match(
    tournament_id: str,
    match_id: str,
    body: MatchIn,
    workspace_id: str = Query(...),
    account_id: str = Depends(get_account_id),
    svc: ClubTournamentService = Depends(get_club_tournament_service),
):
    return _run(lambda: svc.update_match(tournament_id, match_id, workspace_id, account_id, body), "Match")


@router.delete("/{tournament_id}/matches/{match_id}", dependencies=[Depends(MANAGERS)])
async def delete_match(
    tournament_id: str,
    match_id: str,
    workspace_id: str = Query(...),
    account_id: str = Depends(get_account_id),
    svc: ClubTournamentService = Depends(get_club_tournament_service),
):
    _run(lambda: svc.delete_match(tournament_id, match_id, workspace_id, account_id), "Match")
    return {"deleted_match_id": match_id}


@router.put("/{tournament_id}/matches/{match_id}/lineup", dependencies=[Depends(MANAGERS)])
async def save_lineup(
    tournament_id: str,
    match_id: str,
    body: LineupIn,
    workspace_id: str = Query(...),
    account_id: str = Depends(get_account_id),
    svc: ClubTournamentService = Depends(get_club_tournament_service),
):
    return _run(lambda: svc.save_lineup(tournament_id, match_id, workspace_id, account_id, body), "Match")
