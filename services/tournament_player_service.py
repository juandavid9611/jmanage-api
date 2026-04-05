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

        result = self._with_batch_stats(tournament_id, items)

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

    def _with_batch_stats(self, tournament_id: str, players: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Compute stats for all players in a single batch pass (1 + N_matches queries)."""
        if not players:
            return players

        finished = self.match_repo.list_by_tournament(tournament_id, status="finished")
        if not finished:
            for p in players:
                p["stats"] = self._empty_stats()
            return players

        events_by_match = self.event_repo.batch_list_by_matches([m["id"] for m in finished])

        for player in players:
            player_id = player["id"]
            team_id = player.get("team_id")
            goals = assists = yellow_cards = red_cards = appearances = 0

            for match in finished:
                if match.get("home_team_id") != team_id and match.get("away_team_id") != team_id:
                    continue
                player_in_match = False
                for ev in events_by_match.get(match["id"], []):
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
        return players

    def _with_stats(self, player: dict[str, Any]) -> dict[str, Any]:
        """Attach computed stats for a single player (used by get/create/update)."""
        result = self._with_batch_stats(player.get("tournament_id", ""), [player])
        return result[0]

    @staticmethod
    def _empty_stats() -> dict:
        return {
            "goals": 0,
            "assists": 0,
            "yellow_cards": 0,
            "red_cards": 0,
            "appearances": 0,
        }
