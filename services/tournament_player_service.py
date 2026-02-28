"""Business logic for TournamentPlayer management.

Player stats (goals, assists, cards, appearances) are computed from
match events — never stored directly.
"""

from uuid import uuid4
from datetime import datetime
from typing import Any

from api.schemas.tournaments import CreatePlayer, PatchPlayer
from repositories.tournament_player_repo_ddb import TournamentPlayerRepo
from repositories.tournament_match_event_repo_ddb import TournamentMatchEventRepo
from repositories.tournament_match_repo_ddb import TournamentMatchRepo


class TournamentPlayerService:
    def __init__(
        self,
        repo: TournamentPlayerRepo,
        match_repo: TournamentMatchRepo,
        event_repo: TournamentMatchEventRepo,
    ):
        self.repo = repo
        self.match_repo = match_repo
        self.event_repo = event_repo

    def create_player(self, tournament_id: str, team_id: str, body: CreatePlayer) -> dict[str, Any]:
        item = {
            "id": f"tpl_{uuid4().hex}",
            "tournament_id": tournament_id,
            "team_id": team_id,
            "name": body.name,
            "position": body.position.value,
            "number": body.number,
            "avatar_url": body.avatar_url or "",
            "created_at": datetime.utcnow().isoformat(),
        }
        self.repo.put(item)
        return self._with_stats(item)

    def get_player(self, player_id: str) -> dict[str, Any] | None:
        item = self.repo.get(player_id)
        if not item:
            return None
        return self._with_stats(item)

    def list_players(
        self,
        tournament_id: str,
        team_id: str | None = None,
        sort_by: str | None = None,
    ) -> list[dict[str, Any]]:
        if team_id:
            items = self.repo.list_by_team(team_id)
        else:
            items = self.repo.list_by_tournament(tournament_id)

        result = [self._with_stats(i) for i in items]

        if sort_by and sort_by in ("goals", "assists", "appearances"):
            result.sort(key=lambda p: p.get("stats", {}).get(sort_by, 0), reverse=True)
        return result

    def update_player(self, player_id: str, body: PatchPlayer) -> dict[str, Any] | None:
        existing = self.repo.get(player_id)
        if not existing:
            return None
        updates = body.dict(exclude_unset=True, exclude_none=True)
        if "position" in updates:
            updates["position"] = updates["position"]  # already str via Pydantic
        if not updates:
            return self._with_stats(existing)
        updated = self.repo.update(player_id, updates)
        return self._with_stats(updated) if updated else None

    def delete_player(self, player_id: str) -> bool:
        existing = self.repo.get(player_id)
        if not existing:
            return False
        self.repo.delete(player_id)
        return True

    # ── Computed stats ───────────────────────────────────────────────

    def _with_stats(self, player: dict[str, Any]) -> dict[str, Any]:
        """Attach computed stats from match events."""
        player_id = player["id"]
        tournament_id = player.get("tournament_id")
        if not tournament_id:
            player["stats"] = self._empty_stats()
            return player

        # Get all matches in tournament to check appearances
        all_matches = self.match_repo.list_by_tournament(tournament_id, status="finished")
        team_id = player.get("team_id")

        goals = 0
        assists = 0
        yellow_cards = 0
        red_cards = 0
        appearances = 0

        for match in all_matches:
            # Check appearances (player's team must be in the match)
            if match.get("home_team_id") != team_id and match.get("away_team_id") != team_id:
                continue

            events = self.event_repo.list_by_match(match["id"])
            player_in_match = False
            for ev in events:
                if ev.get("player_id") == player_id:
                    player_in_match = True
                    etype = ev.get("type")
                    if etype in ("goal", "penalty_scored"):
                        goals += 1
                    elif etype == "yellow_card":
                        yellow_cards += 1
                    elif etype in ("red_card", "second_yellow"):
                        red_cards += 1
                if ev.get("assist_player_id") == player_id:
                    player_in_match = True
                    assists += 1

            if player_in_match:
                appearances += 1

        player["stats"] = {
            "goals": goals,
            "assists": assists,
            "yellow_cards": yellow_cards,
            "red_cards": red_cards,
            "appearances": appearances,
        }
        return player

    @staticmethod
    def _empty_stats() -> dict:
        return {
            "goals": 0,
            "assists": 0,
            "yellow_cards": 0,
            "red_cards": 0,
            "appearances": 0,
        }
