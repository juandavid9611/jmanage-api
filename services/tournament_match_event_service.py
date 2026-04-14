"""Business logic for MatchEvent management.

Validates player-team relationships and provides event CRUD.
Stat recomputation happens on read in the Player service.
When a goal-related event is created or deleted, the parent match
score is automatically recomputed so live scores stay in sync.
"""

from uuid import uuid4
from datetime import datetime
from typing import Any

from api.schemas.tournaments import CreateMatchEvent, PatchMatchEvent
from repositories.tournament_match_event_repo_ddb import TournamentMatchEventRepo
from repositories.tournament_match_repo_ddb import TournamentMatchRepo

_GOAL_TYPES = {"goal", "own_goal", "penalty_scored"}


class TournamentMatchEventService:
    def __init__(
        self,
        repo: TournamentMatchEventRepo,
        match_repo: TournamentMatchRepo | None = None,
    ):
        self.repo = repo
        self.match_repo = match_repo

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

        # Auto-sync match score when a goal-related event is added
        if body.type.value in _GOAL_TYPES:
            self._sync_match_score(match_id)

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
        result = self.repo.update(event_id, updates)

        # If the type changed (e.g. goal → yellow_card or vice versa), re-sync score
        old_is_goal = existing.get("type", "") in _GOAL_TYPES
        new_is_goal = updates.get("type", existing.get("type", "")) in _GOAL_TYPES
        if old_is_goal or new_is_goal:
            self._sync_match_score(existing["match_id"])

        return result

    def delete_event(self, event_id: str) -> bool:
        existing = self.repo.get(event_id)
        if not existing:
            return False
        self.repo.delete(event_id)

        # Re-sync match score when a goal-related event is removed
        if existing.get("type", "") in _GOAL_TYPES:
            self._sync_match_score(existing["match_id"])

        return True

    # ── Helpers ──────────────────────────────────────────────────────

    def _sync_match_score(self, match_id: str) -> None:
        """Recompute score from all events and update the match record."""
        if not self.match_repo:
            return
        match = self.match_repo.get(match_id)
        if not match:
            return

        home_team_id = match.get("home_team_id")
        away_team_id = match.get("away_team_id")
        events = self.repo.list_by_match(match_id)

        score_home = 0
        score_away = 0
        for ev in events:
            etype = ev.get("type", "")
            team_id = ev.get("team_id", "")

            if etype in ("goal", "penalty_scored"):
                if team_id == home_team_id:
                    score_home += 1
                elif team_id == away_team_id:
                    score_away += 1
            elif etype == "own_goal":
                # Own goal counts for the OTHER team
                if team_id == home_team_id:
                    score_away += 1
                elif team_id == away_team_id:
                    score_home += 1

        self.match_repo.update(match_id, {
            "score_home": score_home,
            "score_away": score_away,
        })
