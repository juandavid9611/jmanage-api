"""Tournaments API — all 40 endpoints for the tournament domain.

Endpoint groups:
  1. Tournaments CRUD (5)
  2. Groups CRUD + team assignment (7)
  3. Teams CRUD (5)
  4. Players CRUD (5)
  5. Matches CRUD (5)
  6. Fixture generation (3)
  7. Match events CRUD (3)
  8. Standings (2)
  9. Bracket (4)
 10. Stats (1)
"""

from fastapi import APIRouter, Depends, HTTPException, Query, Body

from auth import PermissionChecker, get_account_id, get_current_user
from di import (
    get_tournament_service,
    get_tournament_team_service,
    get_tournament_player_service,
    get_match_service,
    get_match_event_service,
    get_standings_service,
    get_tournament_stats_service,
)
from services.tournament_service import TournamentService
from services.tournament_team_service import TournamentTeamService
from services.tournament_player_service import TournamentPlayerService
from services.tournament_match_service import TournamentMatchService
from services.tournament_match_event_service import TournamentMatchEventService
from services.standings_service import StandingsService
from services.tournament_stats_service import TournamentStatsService

from api.schemas.tournaments import (
    CreateTournament,
    PatchTournament,
    CreateGroup,
    PatchGroup,
    AssignTeamToGroup,
    CreateTeam,
    PatchTeam,
    CreatePlayer,
    PatchPlayer,
    CreateMatch,
    PatchMatch,
    CreateMatchEvent,
    PatchMatchEvent,
    GenerateScheduleRequest,
    GenerateBracketRequest,
    BulkMatchesRequest,
    BracketOverride,
)


ADMIN = PermissionChecker(required_permissions=["admin"])
ALL_ROLES = PermissionChecker(required_permissions=["admin", "user"])


router = APIRouter(prefix="/tournaments", tags=["tournaments"])


# ╔══════════════════════════════════════════════════════════════════════╗
# ║  1. TOURNAMENTS CRUD                                                ║
# ╚══════════════════════════════════════════════════════════════════════╝

@router.get("", dependencies=[Depends(ALL_ROLES)])
async def list_tournaments(
    status: str | None = None,
    account_id: str = Depends(get_account_id),
    svc: TournamentService = Depends(get_tournament_service),
):
    return svc.list_tournaments(account_id, status=status)


@router.post("", dependencies=[Depends(ADMIN)])
async def create_tournament(
    body: CreateTournament,
    account_id: str = Depends(get_account_id),
    svc: TournamentService = Depends(get_tournament_service),
):
    return svc.create_tournament(body, account_id)


@router.get("/{tournament_id}", dependencies=[Depends(ALL_ROLES)])
async def get_tournament(
    tournament_id: str,
    account_id: str = Depends(get_account_id),
    svc: TournamentService = Depends(get_tournament_service),
):
    item = svc.get_tournament(tournament_id, account_id)
    if not item:
        raise HTTPException(status_code=404, detail="Tournament not found")
    return item


@router.patch("/{tournament_id}", dependencies=[Depends(ADMIN)])
async def update_tournament(
    tournament_id: str,
    body: PatchTournament,
    account_id: str = Depends(get_account_id),
    svc: TournamentService = Depends(get_tournament_service),
):
    result = svc.update_tournament(tournament_id, account_id, body)
    if not result:
        raise HTTPException(status_code=404, detail="Tournament not found")
    return result


@router.delete("/{tournament_id}", dependencies=[Depends(ADMIN)])
async def delete_tournament(
    tournament_id: str,
    account_id: str = Depends(get_account_id),
    svc: TournamentService = Depends(get_tournament_service),
):
    if not svc.delete_tournament(tournament_id, account_id):
        raise HTTPException(status_code=404, detail="Tournament not found")
    return {"deleted": tournament_id}


# ╔══════════════════════════════════════════════════════════════════════╗
# ║  2. GROUPS                                                           ║
# ╚══════════════════════════════════════════════════════════════════════╝

@router.get("/{tournament_id}/groups", dependencies=[Depends(ALL_ROLES)])
async def list_groups(
    tournament_id: str,
    account_id: str = Depends(get_account_id),
    svc: TournamentService = Depends(get_tournament_service),
):
    groups = svc.list_groups(tournament_id, account_id)
    if groups is None:
        raise HTTPException(status_code=404, detail="Tournament not found")
    return groups


@router.post("/{tournament_id}/groups", dependencies=[Depends(ADMIN)])
async def create_group(
    tournament_id: str,
    body: CreateGroup,
    account_id: str = Depends(get_account_id),
    svc: TournamentService = Depends(get_tournament_service),
):
    group = svc.create_group(tournament_id, account_id, body)
    if group is None:
        raise HTTPException(status_code=404, detail="Tournament not found")
    return group


@router.patch("/{tournament_id}/groups/{group_id}", dependencies=[Depends(ADMIN)])
async def update_group(
    tournament_id: str,
    group_id: str,
    body: PatchGroup,
    account_id: str = Depends(get_account_id),
    svc: TournamentService = Depends(get_tournament_service),
):
    result = svc.update_group(tournament_id, account_id, group_id, body)
    if result is None:
        raise HTTPException(status_code=404, detail="Group not found")
    return result


@router.delete("/{tournament_id}/groups/{group_id}", dependencies=[Depends(ADMIN)])
async def delete_group(
    tournament_id: str,
    group_id: str,
    account_id: str = Depends(get_account_id),
    svc: TournamentService = Depends(get_tournament_service),
):
    if not svc.delete_group(tournament_id, account_id, group_id):
        raise HTTPException(status_code=404, detail="Group not found")
    return {"deleted": group_id}


@router.post("/{tournament_id}/groups/{group_id}/teams", dependencies=[Depends(ADMIN)])
async def assign_team_to_group(
    tournament_id: str,
    group_id: str,
    body: AssignTeamToGroup,
    account_id: str = Depends(get_account_id),
    svc: TournamentService = Depends(get_tournament_service),
):
    result = svc.assign_team_to_group(tournament_id, account_id, group_id, body.team_id, body.seed)
    if result is None:
        raise HTTPException(status_code=404, detail="Tournament or group not found")
    return result


@router.delete("/{tournament_id}/groups/{group_id}/teams/{team_id}", dependencies=[Depends(ADMIN)])
async def remove_team_from_group(
    tournament_id: str,
    group_id: str,
    team_id: str,
    account_id: str = Depends(get_account_id),
    svc: TournamentService = Depends(get_tournament_service),
):
    if not svc.remove_team_from_group(tournament_id, account_id, group_id, team_id):
        raise HTTPException(status_code=404, detail="Team not found in group")
    return {"removed": team_id}


@router.get("/{tournament_id}/groups/{group_id}/standings", dependencies=[Depends(ALL_ROLES)])
async def group_standings(
    tournament_id: str,
    group_id: str,
    account_id: str = Depends(get_account_id),
    t_svc: TournamentService = Depends(get_tournament_service),
    s_svc: StandingsService = Depends(get_standings_service),
):
    t = t_svc.get_tournament(tournament_id, account_id)
    if not t:
        raise HTTPException(status_code=404, detail="Tournament not found")
    # Get team IDs from embedded group data for fallback filtering
    group_team_ids = []
    for g in t.get("groups", []):
        if g.get("id") == group_id:
            group_team_ids = [gt["team_id"] for gt in g.get("teams", [])]
            break
    return s_svc.get_standings(
        tournament_id, t.get("rules", {}),
        group_id=group_id, group_team_ids=group_team_ids
    )


# ╔══════════════════════════════════════════════════════════════════════╗
# ║  3. TEAMS                                                           ║
# ╚══════════════════════════════════════════════════════════════════════╝

@router.get("/{tournament_id}/teams", dependencies=[Depends(ALL_ROLES)])
async def list_teams(
    tournament_id: str,
    group_id: str | None = None,
    account_id: str = Depends(get_account_id),
    t_svc: TournamentService = Depends(get_tournament_service),
    svc: TournamentTeamService = Depends(get_tournament_team_service),
):
    _require_tournament(t_svc, tournament_id, account_id)
    return svc.list_teams(tournament_id, group_id=group_id)


@router.post("/{tournament_id}/teams", dependencies=[Depends(ADMIN)])
async def create_team(
    tournament_id: str,
    body: CreateTeam,
    account_id: str = Depends(get_account_id),
    t_svc: TournamentService = Depends(get_tournament_service),
    svc: TournamentTeamService = Depends(get_tournament_team_service),
):
    _require_tournament(t_svc, tournament_id, account_id)
    return svc.create_team(tournament_id, body)


@router.get("/{tournament_id}/teams/{team_id}", dependencies=[Depends(ALL_ROLES)])
async def get_team(
    tournament_id: str,
    team_id: str,
    account_id: str = Depends(get_account_id),
    t_svc: TournamentService = Depends(get_tournament_service),
    svc: TournamentTeamService = Depends(get_tournament_team_service),
):
    _require_tournament(t_svc, tournament_id, account_id)
    item = svc.get_team(team_id)
    if not item or item.get("tournament_id") != tournament_id:
        raise HTTPException(status_code=404, detail="Team not found")
    return item


@router.patch("/{tournament_id}/teams/{team_id}", dependencies=[Depends(ADMIN)])
async def update_team(
    tournament_id: str,
    team_id: str,
    body: PatchTeam,
    account_id: str = Depends(get_account_id),
    t_svc: TournamentService = Depends(get_tournament_service),
    svc: TournamentTeamService = Depends(get_tournament_team_service),
):
    _require_tournament(t_svc, tournament_id, account_id)
    existing = svc.get_team(team_id)
    if not existing or existing.get("tournament_id") != tournament_id:
        raise HTTPException(status_code=404, detail="Team not found")
    return svc.update_team(team_id, body)


@router.delete("/{tournament_id}/teams/{team_id}", dependencies=[Depends(ADMIN)])
async def delete_team(
    tournament_id: str,
    team_id: str,
    account_id: str = Depends(get_account_id),
    t_svc: TournamentService = Depends(get_tournament_service),
    svc: TournamentTeamService = Depends(get_tournament_team_service),
):
    _require_tournament(t_svc, tournament_id, account_id)
    existing = svc.get_team(team_id)
    if not existing or existing.get("tournament_id") != tournament_id:
        raise HTTPException(status_code=404, detail="Team not found")
    svc.delete_team(team_id)
    return {"deleted": team_id}


# ╔══════════════════════════════════════════════════════════════════════╗
# ║  4. PLAYERS                                                         ║
# ╚══════════════════════════════════════════════════════════════════════╝

@router.get("/{tournament_id}/players", dependencies=[Depends(ALL_ROLES)])
async def list_players(
    tournament_id: str,
    team_id: str | None = None,
    sort: str | None = None,
    account_id: str = Depends(get_account_id),
    t_svc: TournamentService = Depends(get_tournament_service),
    svc: TournamentPlayerService = Depends(get_tournament_player_service),
):
    _require_tournament(t_svc, tournament_id, account_id)
    return svc.list_players(tournament_id, team_id=team_id, sort_by=sort)


@router.post("/{tournament_id}/teams/{team_id}/players", dependencies=[Depends(ADMIN)])
async def create_player(
    tournament_id: str,
    team_id: str,
    body: CreatePlayer,
    account_id: str = Depends(get_account_id),
    t_svc: TournamentService = Depends(get_tournament_service),
    team_svc: TournamentTeamService = Depends(get_tournament_team_service),
    svc: TournamentPlayerService = Depends(get_tournament_player_service),
):
    _require_tournament(t_svc, tournament_id, account_id)
    if not team_svc.belongs_to_tournament(team_id, tournament_id):
        raise HTTPException(status_code=404, detail="Team not found in tournament")
    return svc.create_player(tournament_id, team_id, body)


@router.get("/{tournament_id}/players/{player_id}", dependencies=[Depends(ALL_ROLES)])
async def get_player(
    tournament_id: str,
    player_id: str,
    account_id: str = Depends(get_account_id),
    t_svc: TournamentService = Depends(get_tournament_service),
    svc: TournamentPlayerService = Depends(get_tournament_player_service),
):
    _require_tournament(t_svc, tournament_id, account_id)
    item = svc.get_player(player_id)
    if not item or item.get("tournament_id") != tournament_id:
        raise HTTPException(status_code=404, detail="Player not found")
    return item


@router.patch("/{tournament_id}/players/{player_id}", dependencies=[Depends(ADMIN)])
async def update_player(
    tournament_id: str,
    player_id: str,
    body: PatchPlayer,
    account_id: str = Depends(get_account_id),
    t_svc: TournamentService = Depends(get_tournament_service),
    svc: TournamentPlayerService = Depends(get_tournament_player_service),
):
    _require_tournament(t_svc, tournament_id, account_id)
    existing = svc.get_player(player_id)
    if not existing or existing.get("tournament_id") != tournament_id:
        raise HTTPException(status_code=404, detail="Player not found")
    return svc.update_player(player_id, body)


@router.delete("/{tournament_id}/players/{player_id}", dependencies=[Depends(ADMIN)])
async def delete_player(
    tournament_id: str,
    player_id: str,
    account_id: str = Depends(get_account_id),
    t_svc: TournamentService = Depends(get_tournament_service),
    svc: TournamentPlayerService = Depends(get_tournament_player_service),
):
    _require_tournament(t_svc, tournament_id, account_id)
    existing = svc.get_player(player_id)
    if not existing or existing.get("tournament_id") != tournament_id:
        raise HTTPException(status_code=404, detail="Player not found")
    svc.delete_player(player_id)
    return {"deleted": player_id}


# ╔══════════════════════════════════════════════════════════════════════╗
# ║  5. MATCHES CRUD                                                     ║
# ╚══════════════════════════════════════════════════════════════════════╝

@router.get("/{tournament_id}/matches", dependencies=[Depends(ALL_ROLES)])
async def list_matches(
    tournament_id: str,
    matchweek: int | None = None,
    status: str | None = None,
    team_id: str | None = None,
    round: str | None = None,
    group_id: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
    account_id: str = Depends(get_account_id),
    t_svc: TournamentService = Depends(get_tournament_service),
    svc: TournamentMatchService = Depends(get_match_service),
):
    _require_tournament(t_svc, tournament_id, account_id)
    return svc.list_matches(
        tournament_id,
        matchweek=matchweek,
        status=status,
        team_id=team_id,
        round_name=round,
        group_id=group_id,
        date_from=date_from,
        date_to=date_to,
    )


@router.post("/{tournament_id}/matches", dependencies=[Depends(ADMIN)])
async def create_match(
    tournament_id: str,
    body: CreateMatch,
    account_id: str = Depends(get_account_id),
    t_svc: TournamentService = Depends(get_tournament_service),
    team_svc: TournamentTeamService = Depends(get_tournament_team_service),
    svc: TournamentMatchService = Depends(get_match_service),
):
    t = _require_tournament(t_svc, tournament_id, account_id)
    # Validate both teams belong to tournament
    if not team_svc.belongs_to_tournament(body.home_team_id, tournament_id):
        raise HTTPException(status_code=400, detail="Home team not in tournament")
    if not team_svc.belongs_to_tournament(body.away_team_id, tournament_id):
        raise HTTPException(status_code=400, detail="Away team not in tournament")
    try:
        return svc.create_match(tournament_id, body, t.get("type", "league"))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/{tournament_id}/matches/{match_id}", dependencies=[Depends(ALL_ROLES)])
async def get_match(
    tournament_id: str,
    match_id: str,
    account_id: str = Depends(get_account_id),
    t_svc: TournamentService = Depends(get_tournament_service),
    svc: TournamentMatchService = Depends(get_match_service),
    ev_svc: TournamentMatchEventService = Depends(get_match_event_service),
):
    _require_tournament(t_svc, tournament_id, account_id)
    item = svc.get_match(match_id)
    if not item or item.get("tournament_id") != tournament_id:
        raise HTTPException(status_code=404, detail="Match not found")
    item["events"] = ev_svc.list_events(match_id)
    return item


@router.patch("/{tournament_id}/matches/{match_id}", dependencies=[Depends(ADMIN)])
async def update_match(
    tournament_id: str,
    match_id: str,
    body: PatchMatch,
    account_id: str = Depends(get_account_id),
    t_svc: TournamentService = Depends(get_tournament_service),
    svc: TournamentMatchService = Depends(get_match_service),
):
    _require_tournament(t_svc, tournament_id, account_id)
    existing = svc.get_match(match_id)
    if not existing or existing.get("tournament_id") != tournament_id:
        raise HTTPException(status_code=404, detail="Match not found")
    try:
        return svc.update_match(match_id, body)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.delete("/{tournament_id}/matches/{match_id}", dependencies=[Depends(ADMIN)])
async def delete_match(
    tournament_id: str,
    match_id: str,
    account_id: str = Depends(get_account_id),
    t_svc: TournamentService = Depends(get_tournament_service),
    svc: TournamentMatchService = Depends(get_match_service),
):
    _require_tournament(t_svc, tournament_id, account_id)
    existing = svc.get_match(match_id)
    if not existing or existing.get("tournament_id") != tournament_id:
        raise HTTPException(status_code=404, detail="Match not found")
    svc.delete_match(match_id)
    return {"deleted": match_id}


# ╔══════════════════════════════════════════════════════════════════════╗
# ║  6. FIXTURE GENERATION                                              ║
# ╚══════════════════════════════════════════════════════════════════════╝

@router.post("/{tournament_id}/schedule:generate", dependencies=[Depends(ADMIN)])
async def generate_schedule(
    tournament_id: str,
    body: GenerateScheduleRequest,
    account_id: str = Depends(get_account_id),
    t_svc: TournamentService = Depends(get_tournament_service),
    team_svc: TournamentTeamService = Depends(get_tournament_team_service),
    svc: TournamentMatchService = Depends(get_match_service),
):
    t = _require_tournament(t_svc, tournament_id, account_id)
    teams = team_svc.list_teams(tournament_id, group_id=body.group_id)
    team_ids = [tm["id"] for tm in teams]
    legs = t.get("rules", {}).get("legs", 2)
    try:
        return svc.generate_schedule(tournament_id, team_ids, body, legs=legs)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/{tournament_id}/bracket:generate", dependencies=[Depends(ADMIN)])
async def generate_bracket(
    tournament_id: str,
    body: GenerateBracketRequest,
    account_id: str = Depends(get_account_id),
    svc: TournamentService = Depends(get_tournament_service),
):
    result = svc.generate_bracket(tournament_id, account_id, body)
    if result is None:
        raise HTTPException(status_code=404, detail="Tournament not found")
    return result


@router.post("/{tournament_id}/matches:bulk", dependencies=[Depends(ADMIN)])
async def bulk_create_matches(
    tournament_id: str,
    body: BulkMatchesRequest,
    account_id: str = Depends(get_account_id),
    t_svc: TournamentService = Depends(get_tournament_service),
    svc: TournamentMatchService = Depends(get_match_service),
):
    _require_tournament(t_svc, tournament_id, account_id)
    return svc.bulk_create(tournament_id, body)


# ╔══════════════════════════════════════════════════════════════════════╗
# ║  7. MATCH EVENTS                                                    ║
# ╚══════════════════════════════════════════════════════════════════════╝

@router.post("/matches/{match_id}/events", dependencies=[Depends(ADMIN)])
async def create_event(
    match_id: str,
    body: CreateMatchEvent,
    svc: TournamentMatchEventService = Depends(get_match_event_service),
):
    return svc.create_event(match_id, body)


@router.patch("/matches/{match_id}/events/{event_id}", dependencies=[Depends(ADMIN)])
async def update_event(
    match_id: str,
    event_id: str,
    body: PatchMatchEvent,
    svc: TournamentMatchEventService = Depends(get_match_event_service),
):
    result = svc.update_event(event_id, body)
    if not result:
        raise HTTPException(status_code=404, detail="Event not found")
    return result


@router.delete("/matches/{match_id}/events/{event_id}", dependencies=[Depends(ADMIN)])
async def delete_event(
    match_id: str,
    event_id: str,
    svc: TournamentMatchEventService = Depends(get_match_event_service),
):
    if not svc.delete_event(event_id):
        raise HTTPException(status_code=404, detail="Event not found")
    return {"deleted": event_id}


# ╔══════════════════════════════════════════════════════════════════════╗
# ║  8. STANDINGS                                                       ║
# ╚══════════════════════════════════════════════════════════════════════╝

@router.get("/{tournament_id}/standings", dependencies=[Depends(ALL_ROLES)])
async def tournament_standings(
    tournament_id: str,
    account_id: str = Depends(get_account_id),
    t_svc: TournamentService = Depends(get_tournament_service),
    s_svc: StandingsService = Depends(get_standings_service),
):
    t = _require_tournament(t_svc, tournament_id, account_id)
    return s_svc.get_standings(tournament_id, t.get("rules", {}))


# NOTE: group_standings is defined above in the Groups section (§2)


# ╔══════════════════════════════════════════════════════════════════════╗
# ║  9. BRACKET                                                         ║
# ╚══════════════════════════════════════════════════════════════════════╝

@router.get("/{tournament_id}/bracket", dependencies=[Depends(ALL_ROLES)])
async def get_bracket(
    tournament_id: str,
    account_id: str = Depends(get_account_id),
    svc: TournamentService = Depends(get_tournament_service),
):
    bracket = svc.get_bracket(tournament_id, account_id)
    if bracket is None:
        raise HTTPException(status_code=404, detail="Tournament not found")
    return bracket


# NOTE: bracket:generate is defined above in Fixture Generation (§6)


@router.patch("/{tournament_id}/bracket", dependencies=[Depends(ADMIN)])
async def override_bracket(
    tournament_id: str,
    body: BracketOverride,
    account_id: str = Depends(get_account_id),
    svc: TournamentService = Depends(get_tournament_service),
):
    result = svc.update_bracket(tournament_id, account_id, body)
    if result is None:
        raise HTTPException(status_code=404, detail="Bracket round/slot not found")
    return result


@router.post("/{tournament_id}/matches/{match_id}:advance", dependencies=[Depends(ADMIN)])
async def advance_winner(
    tournament_id: str,
    match_id: str,
    winner_team_id: str = Body(..., embed=True),
    account_id: str = Depends(get_account_id),
    svc: TournamentService = Depends(get_tournament_service),
):
    result = svc.advance_winner(tournament_id, account_id, match_id, winner_team_id)
    if result is None:
        raise HTTPException(status_code=404, detail="Match not found in bracket")
    return result


# ╔══════════════════════════════════════════════════════════════════════╗
# ║  10. STATS                                                          ║
# ╚══════════════════════════════════════════════════════════════════════╝

@router.get("/{tournament_id}/stats", dependencies=[Depends(ALL_ROLES)])
async def tournament_stats(
    tournament_id: str,
    account_id: str = Depends(get_account_id),
    t_svc: TournamentService = Depends(get_tournament_service),
    stats_svc: TournamentStatsService = Depends(get_tournament_stats_service),
):
    t = _require_tournament(t_svc, tournament_id, account_id)
    return stats_svc.get_stats(
        tournament_id,
        current_matchweek=t.get("current_matchweek", 0),
        total_matchweeks=t.get("rules", {}).get("total_matchweeks"),
    )


@router.get("/{tournament_id}/top-scorers", dependencies=[Depends(ALL_ROLES)])
async def top_scorers(
    tournament_id: str,
    limit: int = Query(50, ge=1, le=100),
    account_id: str = Depends(get_account_id),
    t_svc: TournamentService = Depends(get_tournament_service),
    stats_svc: TournamentStatsService = Depends(get_tournament_stats_service),
):
    _require_tournament(t_svc, tournament_id, account_id)
    return stats_svc.get_top_scorers(tournament_id, limit=limit)


# ── Helpers ──────────────────────────────────────────────────────────

def _require_tournament(t_svc: TournamentService, tournament_id: str, account_id: str) -> dict:
    """Fetch tournament and raise 404 if not found or wrong account."""
    t = t_svc.get_tournament(tournament_id, account_id)
    if not t:
        raise HTTPException(status_code=404, detail="Tournament not found")
    return t
