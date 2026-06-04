"""Business logic for MatchEvent management.

Each event mutation (create/update/delete) applies a delta to the
materialized stats on TournamentPlayer, TournamentTeam, and Tournament
items via the aggregator. The parent match score is also auto-synced
for goal-type events so the score stays correct.
"""

from uuid import uuid4
from datetime import datetime
from typing import Any

from api.schemas.tournaments import CreateMatchEvent, PatchMatchEvent
from repositories.tournament_match_event_repo_ddb import TournamentMatchEventRepo
from repositories.tournament_match_repo_ddb import TournamentMatchRepo
from repositories.tournament_player_repo_ddb import TournamentPlayerRepo
from repositories.tournament_repo_ddb import TournamentRepo
from repositories.tournament_team_repo_ddb import TournamentTeamRepo
from services.tournament_aggregator import (
    apply_delta,
    default_player_stats,
    default_team_stats,
    default_tournament_stats,
    event_delta,
    update_average_goals_per_match,
)

_GOAL_TYPES = {"goal", "own_goal", "penalty_scored"}


class TournamentMatchEventService:
    def __init__(
        self,
        repo: TournamentMatchEventRepo,
        match_repo: TournamentMatchRepo | None = None,
        team_repo: TournamentTeamRepo | None = None,
        player_repo: TournamentPlayerRepo | None = None,
        tournament_repo: TournamentRepo | None = None,
    ):
        self.repo = repo
        self.match_repo = match_repo
        self.team_repo = team_repo
        self.player_repo = player_repo
        self.tournament_repo = tournament_repo

    def create_event(self, match_id: str, body: CreateMatchEvent) -> dict[str, Any]:
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

        self._apply_event(item, sign=+1)

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
        if not updates:
            return existing

        # Reverse the old event's contribution before persisting the change,
        # then apply the new one.
        self._apply_event(existing, sign=-1)
        result = self.repo.update(event_id, updates)
        if result:
            self._apply_event(result, sign=+1)

        old_is_goal = existing.get("type", "") in _GOAL_TYPES
        new_is_goal = (result or existing).get("type", "") in _GOAL_TYPES
        if old_is_goal or new_is_goal:
            self._sync_match_score(existing["match_id"])

        return result

    def delete_event(self, event_id: str) -> bool:
        existing = self.repo.get(event_id)
        if not existing:
            return False

        self._apply_event(existing, sign=-1)
        self.repo.delete(event_id)

        if existing.get("type", "") in _GOAL_TYPES:
            self._sync_match_score(existing["match_id"])

        return True

    # ── Aggregator hook ──────────────────────────────────────────────

    def _apply_event(self, event: dict[str, Any], sign: int) -> None:
        """Apply (or reverse) this event's contribution to the materialized
        stats on Player / Team / Tournament items. No-ops if the relevant
        repos weren't injected (e.g., older test wiring)."""
        d = event_delta(event, sign=sign)

        match = None
        tournament_id = None
        if self.match_repo and event.get("match_id"):
            match = self.match_repo.get(event["match_id"])
            tournament_id = (match or {}).get("tournament_id")

        if self.player_repo and d["player_id"]:
            current = (self.player_repo.get(d["player_id"]) or {}).get(
                "stats"
            ) or default_player_stats()
            self.player_repo.update_stats(
                d["player_id"], apply_delta(current, d["player_delta"])
            )

        if self.player_repo and d["assist_player_id"]:
            current = (self.player_repo.get(d["assist_player_id"]) or {}).get(
                "stats"
            ) or default_player_stats()
            self.player_repo.update_stats(
                d["assist_player_id"],
                apply_delta(current, d["assist_player_delta"]),
            )

        if self.team_repo and d["team_id"]:
            current = (self.team_repo.get(d["team_id"]) or {}).get(
                "stats"
            ) or default_team_stats()
            self.team_repo.update_stats(
                d["team_id"], apply_delta(current, d["team_delta"])
            )

        if self.tournament_repo and tournament_id:
            current = (self.tournament_repo.get(tournament_id) or {}).get(
                "stats"
            ) or default_tournament_stats()
            merged = apply_delta(current, d["tournament_delta"])
            self.tournament_repo.update_stats(
                tournament_id, update_average_goals_per_match(merged)
            )

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
                if team_id == home_team_id:
                    score_away += 1
                elif team_id == away_team_id:
                    score_home += 1

        self.match_repo.update(match_id, {
            "score_home": score_home,
            "score_away": score_away,
        })
