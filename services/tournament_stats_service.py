"""Dashboard aggregator — computed tournament statistics.

All values are computed live from matches and events.
"""

from datetime import datetime
from typing import Any

from repositories.tournament_match_repo_ddb import TournamentMatchRepo
from repositories.tournament_match_event_repo_ddb import TournamentMatchEventRepo
from repositories.tournament_team_repo_ddb import TournamentTeamRepo
from repositories.tournament_player_repo_ddb import TournamentPlayerRepo


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

    def get_stats(self, tournament_id: str, current_matchweek: int = 0, total_matchweeks: int | None = None) -> dict[str, Any]:
        all_matches = self.match_repo.list_by_tournament(tournament_id)
        teams = self.team_repo.list_by_tournament(tournament_id)

        total_matches = len(all_matches)
        finished = [m for m in all_matches if m.get("status") == "finished"]
        matches_played = len(finished)
        matches_remaining = total_matches - matches_played

        # Compute goals and cards from events
        total_goals = 0
        total_yellow_cards = 0
        total_red_cards = 0

        goal_types = {"goal", "own_goal", "penalty_scored"}

        for match in finished:
            events = self.event_repo.list_by_match(match["id"])
            for ev in events:
                etype = ev.get("type", "")
                if etype in goal_types:
                    total_goals += 1
                elif etype == "yellow_card":
                    total_yellow_cards += 1
                elif etype in ("red_card", "second_yellow"):
                    total_red_cards += 1

        avg_goals = round(total_goals / matches_played, 2) if matches_played > 0 else 0.0

        return {
            "as_of": datetime.utcnow().isoformat(),
            "total_goals": total_goals,
            "total_matches": total_matches,
            "matches_played": matches_played,
            "matches_remaining": matches_remaining,
            "average_goals_per_match": avg_goals,
            "total_yellow_cards": total_yellow_cards,
            "total_red_cards": total_red_cards,
            "current_matchweek": current_matchweek,
            "total_matchweeks": total_matchweeks,
            "total_teams": len(teams),
        }

    def get_top_scorers(self, tournament_id: str, limit: int = 50) -> list[dict[str, Any]]:
        """Return top scorers ranked by goals scored."""
        all_matches = self.match_repo.list_by_tournament(tournament_id)
        finished = [m for m in all_matches if m.get("status") == "finished"]

        goal_types = {"goal", "penalty_scored", "penalty"}
        # Accumulate: player_id -> {goals, penalties, own_goals}
        scorers: dict[str, dict] = {}

        for match in finished:
            events = self.event_repo.list_by_match(match["id"])
            for ev in events:
                etype = ev.get("type", "")
                pid = ev.get("player_id")
                if not pid:
                    continue
                if etype in goal_types or etype == "own_goal":
                    if pid not in scorers:
                        scorers[pid] = {
                            "player_id": pid,
                            "team_id": ev.get("team_id", ""),
                            "goals": 0,
                            "penalties": 0,
                            "own_goals": 0,
                        }
                    if etype in goal_types:
                        scorers[pid]["goals"] += 1
                    if etype in ("penalty_scored", "penalty"):
                        scorers[pid]["penalties"] += 1
                    if etype == "own_goal":
                        scorers[pid]["own_goals"] += 1

        # Sort by goals desc
        result = sorted(scorers.values(), key=lambda s: -s["goals"])[:limit]

        # Enrich with player and team names
        players = {p["id"]: p for p in self.player_repo.list_by_tournament(tournament_id)}
        teams = {t["id"]: t for t in self.team_repo.list_by_tournament(tournament_id)}
        for i, entry in enumerate(result, 1):
            entry["rank"] = i
            player = players.get(entry["player_id"], {})
            entry["player_name"] = player.get("name", "")
            entry["player_number"] = player.get("number", "")
            entry["team_name"] = teams.get(entry["team_id"], {}).get("name", "")
            entry["team_short_name"] = teams.get(entry["team_id"], {}).get("short_name", "")

        return result

