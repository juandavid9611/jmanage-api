"""Public Tournaments API — read-only endpoints without authentication.

Tournaments are accessible publicly only when `is_public=True`.
No account_id or auth token required.
"""

from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query

from di import (
    get_tournament_service,
    get_tournament_team_service,
    get_standings_service,
    get_tournament_stats_service,
    get_match_service,
    get_match_event_service,
    get_tournament_player_service,
)
from services.standings_service import StandingsService
from services.tournament_service import TournamentService
from services.tournament_match_service import TournamentMatchService
from services.tournament_match_event_service import TournamentMatchEventService
from services.tournament_player_service import TournamentPlayerService
from services.tournament_stats_service import TournamentStatsService
from services.tournament_team_service import TournamentTeamService

router = APIRouter(prefix="/public/tournaments", tags=["public"])


def _get_public_or_404(tournament_id: str, svc: TournamentService) -> dict:
    t = svc.get_public_tournament(tournament_id)
    if not t:
        raise HTTPException(status_code=404, detail="Tournament not found")
    return t


# ── Tournaments ────────────────────────────────────────────────────────

@router.get("")
async def list_public_tournaments(
    status: Optional[str] = Query(None),
    svc: TournamentService = Depends(get_tournament_service),
):
    return svc.list_public_tournaments(status=status)


@router.get("/{tournament_id}")
async def get_public_tournament(
    tournament_id: str,
    svc: TournamentService = Depends(get_tournament_service),
):
    return _get_public_or_404(tournament_id, svc)


# ── Groups ─────────────────────────────────────────────────────────────

@router.get("/{tournament_id}/groups")
async def get_public_groups(
    tournament_id: str,
    svc: TournamentService = Depends(get_tournament_service),
):
    t = _get_public_or_404(tournament_id, svc)
    return t.get("groups", [])


# ── Teams ──────────────────────────────────────────────────────────────

@router.get("/{tournament_id}/teams")
async def get_public_teams(
    tournament_id: str,
    svc: TournamentService = Depends(get_tournament_service),
    team_svc: TournamentTeamService = Depends(get_tournament_team_service),
):
    _get_public_or_404(tournament_id, svc)
    return team_svc.list_teams(tournament_id)


# ── Matches ────────────────────────────────────────────────────────────

@router.get("/{tournament_id}/matches")
async def get_public_matches(
    tournament_id: str,
    matchweek: Optional[int] = Query(None),
    status: Optional[str] = Query(None),
    svc: TournamentService = Depends(get_tournament_service),
    match_svc: TournamentMatchService = Depends(get_match_service),
):
    _get_public_or_404(tournament_id, svc)
    return match_svc.list_matches(tournament_id, matchweek=matchweek, status=status)


# ── Standings ──────────────────────────────────────────────────────────

@router.get("/{tournament_id}/standings")
async def get_public_standings(
    tournament_id: str,
    svc: TournamentService = Depends(get_tournament_service),
    team_svc: TournamentTeamService = Depends(get_tournament_team_service),
    s_svc: StandingsService = Depends(get_standings_service),
):
    t = _get_public_or_404(tournament_id, svc)
    groups = t.get("groups", [])
    if groups:
        teams = team_svc.list_teams(tournament_id)
        return s_svc.get_all_standings(tournament_id, t.get("rules", {}), groups, teams)
    return s_svc.get_standings(tournament_id, t.get("rules", {}))


# ── Stats ──────────────────────────────────────────────────────────────

@router.get("/{tournament_id}/stats")
async def get_public_stats(
    tournament_id: str,
    svc: TournamentService = Depends(get_tournament_service),
    stats_svc: TournamentStatsService = Depends(get_tournament_stats_service),
):
    t = _get_public_or_404(tournament_id, svc)
    return stats_svc.get_stats(
        tournament_id,
        current_matchweek=t.get("current_matchweek", 0),
        total_matchweeks=t.get("rules", {}).get("total_matchweeks"),
        tournament=t,
    )


# ── Top Scorers ────────────────────────────────────────────────────────

@router.get("/{tournament_id}/top-scorers")
async def get_public_top_scorers(
    tournament_id: str,
    svc: TournamentService = Depends(get_tournament_service),
    stats_svc: TournamentStatsService = Depends(get_tournament_stats_service),
):
    _get_public_or_404(tournament_id, svc)
    return stats_svc.get_top_scorers(tournament_id)


# ── Bracket ────────────────────────────────────────────────────────────

@router.get("/{tournament_id}/bracket")
async def get_public_bracket(
    tournament_id: str,
    svc: TournamentService = Depends(get_tournament_service),
):
    t = _get_public_or_404(tournament_id, svc)
    return t.get("bracket") or {}


# ── Players ────────────────────────────────────────────────────────────

@router.get("/{tournament_id}/players")
async def get_public_players(
    tournament_id: str,
    team_id: Optional[str] = Query(None),
    svc: TournamentService = Depends(get_tournament_service),
    player_svc: TournamentPlayerService = Depends(get_tournament_player_service),
):
    _get_public_or_404(tournament_id, svc)
    return player_svc.list_players(tournament_id, team_id=team_id)


# ── Match detail (single match with events) ────────────────────────────

@router.get("/{tournament_id}/matches/{match_id}")
async def get_public_match(
    tournament_id: str,
    match_id: str,
    svc: TournamentService = Depends(get_tournament_service),
    match_svc: TournamentMatchService = Depends(get_match_service),
    ev_svc: TournamentMatchEventService = Depends(get_match_event_service),
):
    _get_public_or_404(tournament_id, svc)
    item = match_svc.get_match(match_id)
    if not item or item.get("tournament_id") != tournament_id:
        raise HTTPException(status_code=404, detail="Match not found")
    item["events"] = ev_svc.list_events(match_id)
    return item
