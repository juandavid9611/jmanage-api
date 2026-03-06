"""Business logic for TournamentTeam management."""

from uuid import uuid4
from datetime import datetime
from typing import Any

from api.schemas.tournaments import CreateTeam, PatchTeam
from repositories.tournament_team_repo_ddb import TournamentTeamRepo


class TournamentTeamService:
    def __init__(self, repo: TournamentTeamRepo):
        self.repo = repo

    def create_team(self, tournament_id: str, body: CreateTeam) -> dict[str, Any]:
        item = {
            "id": f"ttm_{uuid4().hex}",
            "tournament_id": tournament_id,
            "name": body.name,
            "short_name": body.short_name,
            "logo_url": body.logo_url or "",
            "seed": body.seed,
            "manager_user_ids": [],
            "created_at": datetime.utcnow().isoformat(),
        }
        # DynamoDB GSI keys cannot be empty strings — only set when present
        if body.group_id:
            item["group_id"] = body.group_id
        self.repo.put(item)
        return item

    def get_team(self, team_id: str) -> dict[str, Any] | None:
        return self.repo.get(team_id)

    def count_teams(self, tournament_id: str) -> int:
        return self.repo.count_by_tournament(tournament_id)

    def list_teams(self, tournament_id: str, group_id: str | None = None) -> list[dict[str, Any]]:
        if group_id:
            return self.repo.list_by_group(group_id)
        return self.repo.list_by_tournament(tournament_id)

    def update_team(self, team_id: str, body: PatchTeam) -> dict[str, Any] | None:
        existing = self.repo.get(team_id)
        if not existing:
            return None
        updates = body.dict(exclude_unset=True, exclude_none=True)
        if not updates:
            return existing
        return self.repo.update(team_id, updates)

    def delete_team(self, team_id: str) -> bool:
        existing = self.repo.get(team_id)
        if not existing:
            return False
        self.repo.delete(team_id)
        return True

    def is_team_manager(self, team_id: str, user_id: str) -> bool:
        """Check if a user is a manager of this team."""
        team = self.repo.get(team_id)
        if not team:
            return False
        return user_id in team.get("manager_user_ids", [])

    def belongs_to_tournament(self, team_id: str, tournament_id: str) -> bool:
        """Check if a team belongs to a given tournament."""
        team = self.repo.get(team_id)
        if not team:
            return False
        return team.get("tournament_id") == tournament_id
