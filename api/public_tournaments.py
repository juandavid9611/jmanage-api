"""Public Tournaments API — read-only endpoints without authentication.

All endpoints require `account_id` as a query parameter.
"""

from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query

from di import (
    get_tournament_service,
    get_tournament_team_service,
    get_standings_service,
    get_tournament_stats_service,
    get_match_service,
)
from services.standings_service import StandingsService
from services.tournament_service import TournamentService
from services.tournament_match_service import TournamentMatchService
from services.tournament_stats_service import TournamentStatsService
from services.tournament_team_service import TournamentTeamService

router = APIRouter(prefix="/public/tournaments", tags=["public"])


# ── Tournaments ────────────────────────────────────────────────────────

@router.get("")
async def list_public_tournaments(
    account_id: str = Query(...),
    status: Optional[str] = Query(None),
    svc: TournamentService = Depends(get_tournament_service),
):
    return svc.list_tournaments(account_id, status=status)


@router.get("/{tournament_id}")
async def get_public_tournament(
    tournament_id: str,
    account_id: str = Query(...),
    svc: TournamentService = Depends(get_tournament_service),
):
    item = svc.get_tournament(tournament_id, account_id)
    if not item:
        raise HTTPException(status_code=404, detail="Tournament not found")
    return item


# ── Groups ─────────────────────────────────────────────────────────────

@router.get("/{tournament_id}/groups")
async def get_public_groups(
    tournament_id: str,
    account_id: str = Query(...),
    svc: TournamentService = Depends(get_tournament_service),
):
    groups = svc.list_groups(tournament_id, account_id)
    if groups is None:
        raise HTTPException(status_code=404, detail="Tournament not found")
    return groups


# ── Teams ──────────────────────────────────────────────────────────────

@router.get("/{tournament_id}/teams")
async def get_public_teams(
    tournament_id: str,
    account_id: str = Query(...),
    svc: TournamentTeamService = Depends(get_tournament_team_service),
    tournament_svc: TournamentService = Depends(get_tournament_service),
):
    item = tournament_svc.get_tournament(tournament_id, account_id)
    if not item:
        raise HTTPException(status_code=404, detail="Tournament not found")
    return svc.list_teams(tournament_id)


# ── Matches ────────────────────────────────────────────────────────────

@router.get("/{tournament_id}/matches")
async def get_public_matches(
    tournament_id: str,
    account_id: str = Query(...),
    matchweek: Optional[int] = Query(None),
    status: Optional[str] = Query(None),
    svc: TournamentMatchService = Depends(get_match_service),
    tournament_svc: TournamentService = Depends(get_tournament_service),
):
    item = tournament_svc.get_tournament(tournament_id, account_id)
    if not item:
        raise HTTPException(status_code=404, detail="Tournament not found")
    return svc.list_matches(tournament_id, matchweek=matchweek, status=status)


# ── Standings ──────────────────────────────────────────────────────────

@router.get("/{tournament_id}/standings")
async def get_public_standings(
    tournament_id: str,
    account_id: str = Query(...),
    tournament_svc: TournamentService = Depends(get_tournament_service),
    team_svc: TournamentTeamService = Depends(get_tournament_team_service),
    s_svc: StandingsService = Depends(get_standings_service),
):
    t = tournament_svc.get_tournament(tournament_id, account_id)
    if not t:
        raise HTTPException(status_code=404, detail="Tournament not found")
    groups = t.get("groups", [])
    if groups:
        teams = team_svc.list_teams(tournament_id)
        return s_svc.get_all_standings(tournament_id, t.get("rules", {}), groups, teams)
    return s_svc.get_standings(tournament_id, t.get("rules", {}))


# ── Stats ──────────────────────────────────────────────────────────────

@router.get("/{tournament_id}/stats")
async def get_public_stats(
    tournament_id: str,
    account_id: str = Query(...),
    tournament_svc: TournamentService = Depends(get_tournament_service),
    stats_svc: TournamentStatsService = Depends(get_tournament_stats_service),
):
    t = tournament_svc.get_tournament(tournament_id, account_id)
    if not t:
        raise HTTPException(status_code=404, detail="Tournament not found")
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
    account_id: str = Query(...),
    tournament_svc: TournamentService = Depends(get_tournament_service),
    stats_svc: TournamentStatsService = Depends(get_tournament_stats_service),
):
    t = tournament_svc.get_tournament(tournament_id, account_id)
    if not t:
        raise HTTPException(status_code=404, detail="Tournament not found")
    return stats_svc.get_top_scorers(tournament_id)
