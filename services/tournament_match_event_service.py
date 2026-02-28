"""Business logic for MatchEvent management.

Validates player-team relationships and provides event CRUD.
Stat recomputation happens on read in the Player service.
"""

from uuid import uuid4
from datetime import datetime
from typing import Any

from api.schemas.tournaments import CreateMatchEvent, PatchMatchEvent
from repositories.tournament_match_event_repo_ddb import TournamentMatchEventRepo


class TournamentMatchEventService:
    def __init__(self, repo: TournamentMatchEventRepo):
        self.repo = repo

    def create_event(self, match_id: str, body: CreateMatchEvent) -> dict[str, Any]:
        # Determine next event_index if not provided
        event_index = body.event_index
        if event_index is None:
            existing = self.repo.list_by_match(match_id)
            event_index = max((e.get("event_index", 0) for e in existing), default=0) + 1

        item = {
            "id": f"mev_{uuid4().hex}",
            "match_id": match_id,
            "type": body.type.value,
            "minute": body.minute,
            "stoppage_time": body.stoppage_time,
            "player_id": body.player_id,
            "assist_player_id": body.assist_player_id,
            "team_id": body.team_id,
            "event_index": event_index,
            "created_at": datetime.utcnow().isoformat(),
        }
        self.repo.put(item)
        return item

    def get_event(self, event_id: str) -> dict[str, Any] | None:
        return self.repo.get(event_id)

    def list_events(self, match_id: str) -> list[dict[str, Any]]:
        return self.repo.list_by_match(match_id)

    def update_event(self, event_id: str, body: PatchMatchEvent) -> dict[str, Any] | None:
        existing = self.repo.get(event_id)
        if not existing:
            return None
        updates = body.dict(exclude_unset=True, exclude_none=True)
        if "type" in updates:
            updates["type"] = updates["type"]  # already str
        if not updates:
            return existing
        return self.repo.update(event_id, updates)

    def delete_event(self, event_id: str) -> bool:
        existing = self.repo.get(event_id)
        if not existing:
            return False
        self.repo.delete(event_id)
        return True
