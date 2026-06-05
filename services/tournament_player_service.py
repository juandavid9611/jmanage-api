"""Business logic for TournamentPlayer management.

Player stats (goals, assists, cards, appearances) are computed from
match events — never stored directly.
"""

from uuid import uuid4
from datetime import datetime
from typing import Any

from api.schemas.tournaments import CreatePlayer, PatchPlayer
from repositories.s3_adapter import S3Adapter
from repositories.tournament_player_repo_ddb import TournamentPlayerRepo
from repositories.tournament_match_event_repo_ddb import TournamentMatchEventRepo
from repositories.tournament_match_repo_ddb import TournamentMatchRepo
from repositories.tournament_team_repo_ddb import TournamentTeamRepo


class TournamentPlayerService:
    def __init__(
        self,
        repo: TournamentPlayerRepo,
        match_repo: TournamentMatchRepo,
        event_repo: TournamentMatchEventRepo,
        s3: S3Adapter | None = None,
        team_repo: TournamentTeamRepo | None = None,
    ):
        self.repo = repo
        self.match_repo = match_repo
        self.event_repo = event_repo
        self.s3 = s3
        self.team_repo = team_repo

    # ── Ownership guard ──────────────────────────────────────────────────

    def _assert_team_ownership(self, team_id: str, acting_user_id: str) -> None:
        """Raise PermissionError if acting_user_id is not the owner of team_id."""
        if not self.team_repo:
            raise PermissionError("Team ownership check unavailable")
        team = self.team_repo.get(team_id)
        if not team or team.get("owner_user_id") != acting_user_id:
            raise PermissionError("Team owner can only manage players on their own team")

    def create_player(
        self,
        tournament_id: str,
        team_id: str,
        body: CreatePlayer,
        *,
        acting_user_id: str | None = None,
        acting_role: str | None = None,
    ) -> dict[str, Any]:
        if acting_role == "team_owner":
            self._assert_team_ownership(team_id, acting_user_id)
        item = {
            "id": f"tpl_{uuid4().hex}",
            "tournament_id": tournament_id,
            "team_id": team_id,
            "name": body.name,
            "position": body.position.value,
            "number": body.number,
            "id_number": body.id_number or "",
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

    def update_player(
        self,
        player_id: str,
        body: PatchPlayer,
        *,
        acting_user_id: str | None = None,
        acting_role: str | None = None,
    ) -> dict[str, Any] | None:
        existing = self.repo.get(player_id)
        if not existing:
            return None
        if acting_role == "team_owner":
            self._assert_team_ownership(existing["team_id"], acting_user_id)
        updates = body.dict(exclude_unset=True, exclude_none=True)
        if "position" in updates:
            updates["position"] = updates["position"]  # already str via Pydantic
        if not updates:
            return self._with_stats(existing)
        updated = self.repo.update(player_id, updates)
        return self._with_stats(updated) if updated else None

    def delete_player(
        self,
        player_id: str,
        *,
        acting_user_id: str | None = None,
        acting_role: str | None = None,
    ) -> bool:
        existing = self.repo.get(player_id)
        if not existing:
            return False
        if acting_role == "team_owner":
            self._assert_team_ownership(existing["team_id"], acting_user_id)
        self.repo.delete(player_id)
        return True

    def generate_avatar_upload_url(
        self,
        player_id: str,
        account_id: str,
        filename: str,
        content_type: str,
        *,
        acting_user_id: str | None = None,
        acting_role: str | None = None,
    ) -> dict[str, str]:
        if acting_role == "team_owner":
            existing = self.repo.get(player_id)
            if existing:
                self._assert_team_ownership(existing["team_id"], acting_user_id)
        if not self.s3:
            raise ValueError("S3 adapter not configured")
        return self.s3.presign_player_avatar_put(
            account_id=account_id,
            player_id=player_id,
            filename=filename,
            content_type=content_type,
        )

    # ── Stats (materialized) ─────────────────────────────────────────

    def _with_stats(self, player: dict[str, Any]) -> dict[str, Any]:
        """Attach the materialized stats and presign avatar URL."""
        self._resolve_avatar(player)
        if not player.get("stats"):
            player["stats"] = self._empty_stats()
        return player

    def _with_batch_stats(self, _tournament_id: str, players: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Compatibility shim — stats are now stored on the player item,
        so this is a no-op that just presigns avatars."""
        for p in players:
            self._with_stats(p)
        return players

    def _resolve_avatar(self, player: dict[str, Any]) -> None:
        """Convert stored S3 key to a presigned GET URL in-place."""
        key = player.get("avatar_url")
        if key and self.s3 and not key.startswith("http"):
            player["avatar_url"] = self.s3.presign_get_from_explicit_key(
                key=key, content_type="image/jpeg"
            )

    @staticmethod
    def _empty_stats() -> dict:
        return {
            "appearances": 0,
            "goals": 0,
            "penalties": 0,
            "own_goals": 0,
            "assists": 0,
            "yellow_cards": 0,
            "red_cards": 0,
        }
