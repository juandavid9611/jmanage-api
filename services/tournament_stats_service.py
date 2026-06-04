"""Tournament read aggregator — reads materialized `stats` fields on
Tournament / TournamentTeam / TournamentPlayer items rather than scanning
all matches and events on every render.

The materialized stats are kept in sync by:
- TournamentMatchEventService (event lifecycle: create/update/delete)
- TournamentMatchService (match lifecycle: status transitions, delete)

For local data that pre-dates this refactor, run
`POST /tournaments/{id}:recompute-stats` to rebuild the stats fields from
the raw events.
"""

from datetime import datetime
from typing import Any

from repositories.tournament_match_event_repo_ddb import TournamentMatchEventRepo
from repositories.tournament_match_repo_ddb import TournamentMatchRepo
from repositories.tournament_player_repo_ddb import TournamentPlayerRepo
from repositories.tournament_team_repo_ddb import TournamentTeamRepo
from services.tournament_aggregator import (
    default_player_stats,
    default_team_stats,
    default_tournament_stats,
    derive_average_goals_per_match,
)


class TournamentStatsService:
    def __init__(
        self,
        match_repo: TournamentMatchRepo,
        event_repo: TournamentMatchEventRepo,
        team_repo: TournamentTeamRepo,
        player_repo: TournamentPlayerRepo,
    ):
        self.match_repo = match_repo
        self.event_repo = event_repo
        self.team_repo = team_repo
        self.player_repo = player_repo

    # ── Stats overview ────────────────────────────────────────────────

    def get_stats(
        self,
        tournament_id: str,
        current_matchweek: int = 0,
        total_matchweeks: int | None = None,
        tournament: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Read tournament-level aggregates from the materialized `stats`
        field on the Tournament item. Augments with current matchweek,
        total matchweeks, total teams (read once), and champion (from
        bracket). Cost: 2 queries (tournament if not passed in, teams).
        """
        stats = (tournament or {}).get("stats") or default_tournament_stats()
        teams = self.team_repo.list_by_tournament(tournament_id)

        total_matches = stats.get("total_matches") or 0
        matches_played = stats.get("matches_played") or 0
        matches_remaining = max(total_matches - matches_played, 0)

        champion = None
        if tournament:
            final = (tournament.get("bracket") or {}).get("final") or []
            winner_id = final[0].get("winner_team_id") if final else None
            if winner_id:
                teams_map = {t["id"]: t for t in teams}
                w = teams_map.get(winner_id) or {}
                if w:
                    champion = {
                        "team_id": winner_id,
                        "name": w.get("name", ""),
                        "short_name": w.get("short_name", ""),
                    }

        return {
            "as_of": datetime.utcnow().isoformat(),
            "total_goals": stats.get("total_goals", 0),
            "total_matches": total_matches,
            "matches_played": matches_played,
            "matches_remaining": matches_remaining,
            "average_goals_per_match": derive_average_goals_per_match(stats),
            "total_yellow_cards": stats.get("total_yellow_cards", 0),
            "total_red_cards": stats.get("total_red_cards", 0),
            "current_matchweek": current_matchweek,
            "total_matchweeks": total_matchweeks,
            "total_teams": len(teams),
            "champion": champion,
        }

    # ── Team discipline (summary only) ────────────────────────────────

    def get_team_discipline(self, tournament_id: str) -> list[dict[str, Any]]:
        """Per-team yellow/red counts with per-player counts (no per-card
        drill-down — see `get_team_cards` for that).

        Cost: 2 queries (teams + players). No event scan.
        """
        teams = self.team_repo.list_by_tournament(tournament_id)
        players = self.player_repo.list_by_tournament(tournament_id)

        # Group players by team
        players_by_team: dict[str, list[dict[str, Any]]] = {}
        for p in players:
            players_by_team.setdefault(p.get("team_id"), []).append(p)

        result: list[dict[str, Any]] = []
        for team in teams:
            tstats = team.get("stats") or default_team_stats()
            player_rows = []
            for p in players_by_team.get(team["id"], []):
                pstats = p.get("stats") or default_player_stats()
                if (pstats.get("yellow_cards") or 0) == 0 and (pstats.get("red_cards") or 0) == 0:
                    continue
                player_rows.append({
                    "player_id": p["id"],
                    "name": p.get("name", ""),
                    "number": p.get("number"),
                    "yellow_cards": pstats.get("yellow_cards", 0),
                    "red_cards": pstats.get("red_cards", 0),
                })
            player_rows.sort(key=lambda r: (-r["red_cards"], -r["yellow_cards"], r["name"]))
            yellow = tstats.get("yellow_cards", 0)
            red = tstats.get("red_cards", 0)
            result.append({
                "team_id": team["id"],
                "name": team.get("name", ""),
                "short_name": team.get("short_name"),
                "yellow_cards": yellow,
                "red_cards": red,
                "total_cards": yellow + red,
                "players": player_rows,
            })

        result.sort(
            key=lambda r: (-r["total_cards"], -r["red_cards"], -r["yellow_cards"], r["name"])
        )
        return result

    # ── Per-team card drill-down (lazy) ────────────────────────────────

    def get_team_cards(self, tournament_id: str, team_id: str) -> list[dict[str, Any]]:
        """Return the per-card drill-down for one team: each card event
        with the match it happened in.

        Cost: 1 query (matches involving this team) + N queries (events per
        match the team appears in). Used by the Sanciones drawer only when
        a team row is expanded.
        """
        all_matches = self.match_repo.list_by_tournament(tournament_id)
        team_matches = [
            m for m in all_matches
            if m.get("status") in ("finished", "live")
            and (m.get("home_team_id") == team_id or m.get("away_team_id") == team_id)
        ]
        match_index = {m["id"]: m for m in team_matches}

        teams = self.team_repo.list_by_tournament(tournament_id)
        team_lookup = {t["id"]: t for t in teams}
        players = {p["id"]: p for p in self.player_repo.list_by_tournament(tournament_id)}

        events_by_match = self.event_repo.batch_list_by_matches([m["id"] for m in team_matches])

        # player_id -> {name, number, cards: [...]}
        by_player: dict[str, dict] = {}
        for mid, evs in events_by_match.items():
            match = match_index.get(mid, {})
            home_id = match.get("home_team_id")
            away_id = match.get("away_team_id")
            opp_id = away_id if home_id == team_id else home_id
            opp = team_lookup.get(opp_id) or {}
            home = team_lookup.get(home_id) or {}
            away = team_lookup.get(away_id) or {}
            home_name = home.get("short_name") or home.get("name") or "—"
            away_name = away.get("short_name") or away.get("name") or "—"
            sh = match.get("score_home")
            sa = match.get("score_away")
            match_label = (
                f"{home_name} {sh}·{sa} {away_name}"
                if sh is not None and sa is not None
                else f"{home_name} vs {away_name}"
            )
            for ev in evs:
                if ev.get("team_id") != team_id:
                    continue
                etype = ev.get("type", "")
                if etype not in ("yellow_card", "red_card", "second_yellow"):
                    continue
                pid = ev.get("player_id")
                if not pid:
                    continue
                bucket = by_player.setdefault(pid, {
                    "player_id": pid,
                    "name": (players.get(pid) or {}).get("name", ""),
                    "number": (players.get(pid) or {}).get("number"),
                    "cards": [],
                })
                bucket["cards"].append({
                    "event_id": ev.get("id"),
                    "type": etype,
                    "minute": ev.get("minute", 0),
                    "match_id": mid,
                    "matchweek": match.get("matchweek"),
                    "match_label": match_label,
                    "match_date": match.get("date"),
                    "opponent_team_id": opp_id,
                    "opponent_name": opp.get("short_name") or opp.get("name"),
                })

        # Sort each player's cards: most recent first
        for bucket in by_player.values():
            bucket["cards"].sort(
                key=lambda c: (-(c.get("matchweek") or 0), -(c.get("minute") or 0))
            )

        rows = list(by_player.values())
        rows.sort(key=lambda r: (-len(r["cards"]), r["name"]))
        return rows

    # ── Top scorers ───────────────────────────────────────────────────

    def get_top_scorers(self, tournament_id: str, limit: int = 50) -> list[dict[str, Any]]:
        """Read top scorers from the materialized `stats.goals` on each
        player item. Cost: 2 queries (players + teams)."""
        players = self.player_repo.list_by_tournament(tournament_id)
        teams = {t["id"]: t for t in self.team_repo.list_by_tournament(tournament_id)}

        scorers: list[dict[str, Any]] = []
        for p in players:
            stats = p.get("stats") or default_player_stats()
            goals = stats.get("goals") or 0
            penalties = stats.get("penalties") or 0
            own_goals = stats.get("own_goals") or 0
            if goals == 0 and penalties == 0 and own_goals == 0:
                continue
            team = teams.get(p.get("team_id") or "") or {}
            scorers.append({
                "player_id": p["id"],
                "team_id": p.get("team_id", ""),
                "goals": goals,
                "penalties": penalties,
                "own_goals": own_goals,
                "player_name": p.get("name", ""),
                "player_number": p.get("number", ""),
                "team_name": team.get("name", ""),
                "team_short_name": team.get("short_name", ""),
            })

        scorers.sort(key=lambda s: -s["goals"])
        scorers = scorers[:limit]
        for i, entry in enumerate(scorers, 1):
            entry["rank"] = i
        return scorers
