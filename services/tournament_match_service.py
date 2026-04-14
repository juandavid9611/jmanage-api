"""Business logic for Match management including fixture generation."""

from uuid import uuid4
from datetime import datetime, timedelta
from typing import Any
from itertools import combinations

from api.schemas.tournaments import CreateMatch, PatchMatch, GenerateScheduleRequest, BulkMatchesRequest
from repositories.tournament_match_repo_ddb import TournamentMatchRepo
from repositories.tournament_match_event_repo_ddb import TournamentMatchEventRepo


# Valid status transitions
_STATUS_TRANSITIONS = {
    "scheduled": {"live", "postponed"},
    "live": {"finished"},
    "postponed": {"scheduled"},
    # "finished" is terminal unless admin force-override
}


class TournamentMatchService:
    def __init__(self, repo: TournamentMatchRepo, event_repo: TournamentMatchEventRepo | None = None):
        self.repo = repo
        self.event_repo = event_repo

    # ── CRUD ─────────────────────────────────────────────────────────

    def create_match(self, tournament_id: str, body: CreateMatch, tournament_type: str) -> dict[str, Any]:
        # Validation
        if body.home_team_id == body.away_team_id:
            raise ValueError("home_team_id and away_team_id cannot be the same")
        if tournament_type == "league" and body.matchweek is None:
            raise ValueError("matchweek is required for league tournaments")
        if tournament_type == "knockout" and not body.round:
            raise ValueError("round is required for knockout tournaments")

        item = {
            "id": f"mtc_{uuid4().hex}",
            "tournament_id": tournament_id,
            "home_team_id": body.home_team_id,
            "away_team_id": body.away_team_id,
            "date": body.date,
            "venue": body.venue or "",
            "matchweek": body.matchweek or 0,
            "round": body.round or "",
            "group_id": body.group_id or "",
            "status": "scheduled",
            "score_home": 0,
            "score_away": 0,
            "created_at": datetime.utcnow().isoformat(),
        }
        self.repo.put(item)
        return item

    def get_match(self, match_id: str) -> dict[str, Any] | None:
        return self.repo.get(match_id)

    def list_matches(
        self,
        tournament_id: str,
        *,
        matchweek: int | None = None,
        status: str | None = None,
        team_id: str | None = None,
        round_name: str | None = None,
        group_id: str | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
    ) -> list[dict[str, Any]]:
        return self.repo.list_by_tournament(
            tournament_id,
            matchweek=matchweek,
            status=status,
            team_id=team_id,
            round_name=round_name,
            group_id=group_id,
            date_from=date_from,
            date_to=date_to,
        )

    def update_match(self, match_id: str, body: PatchMatch) -> dict[str, Any] | None:
        existing = self.repo.get(match_id)
        if not existing:
            return None
        updates = body.dict(exclude_unset=True, exclude_none=True)

        # Validate status transitions
        if "status" in updates:
            current_status = existing.get("status", "scheduled")
            new_status = updates["status"]
            allowed = _STATUS_TRANSITIONS.get(current_status, set())
            if new_status not in allowed:
                raise ValueError(
                    f"Cannot transition from '{current_status}' to '{new_status}'. "
                    f"Allowed: {allowed}"
                )

            # Auto-compute score from events when transitioning to live or finishing
            if new_status in ("live", "finished") and self.event_repo:
                score_home, score_away = self._compute_score(
                    match_id,
                    existing.get("home_team_id"),
                    existing.get("away_team_id"),
                )
                # Only auto-set if not explicitly provided in the request
                if "score_home" not in updates:
                    updates["score_home"] = score_home
                if "score_away" not in updates:
                    updates["score_away"] = score_away

        if not updates:
            return existing
        return self.repo.update(match_id, updates)

    def _compute_score(self, match_id: str, home_team_id: str, away_team_id: str) -> tuple[int, int]:
        """Compute score from match events.

        Goals and penalties count for the scoring team.
        Own goals count for the opposing team.
        """
        events = self.event_repo.list_by_match(match_id)
        score_home = 0
        score_away = 0
        goal_types = {"goal", "penalty_scored"}

        for ev in events:
            etype = ev.get("type", "")
            team_id = ev.get("team_id", "")

            if etype in goal_types:
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

        return score_home, score_away

    def delete_match(self, match_id: str) -> bool:
        existing = self.repo.get(match_id)
        if not existing:
            return False
        self.repo.delete(match_id)
        return True

    # ── Fixture Generation ───────────────────────────────────────────

    def generate_schedule(
        self,
        tournament_id: str,
        team_ids: list[str],
        body: GenerateScheduleRequest,
        legs: int = 2,
    ) -> dict[str, Any]:
        """Generate round-robin schedule for a league tournament.

        Uses a classic round-robin algorithm. If legs=2, each pair plays
        twice (home/away swapped in the second half).
        """
        n = len(team_ids)
        if n < 2:
            raise ValueError("Need at least 2 teams to generate a schedule")

        legs = int(legs)  # DynamoDB returns Decimal; range() needs int

        # If odd number of teams, add a bye
        teams = list(team_ids)
        if n % 2 == 1:
            teams.append(None)  # bye
            n += 1

        matches_to_create: list[dict] = []
        matchweek = 0
        start_date = datetime.fromisoformat(body.start_date)

        for leg in range(legs):
            fixed = teams[0]
            rotating = teams[1:]
            for rnd in range(n - 1):
                matchweek += 1
                round_date = start_date + timedelta(days=body.match_interval_days * (matchweek - 1))
                date_str = round_date.isoformat()

                for i in range(n // 2):
                    if i == 0:
                        home, away = fixed, rotating[0]
                    else:
                        home, away = rotating[i], rotating[-(i)]
                    if home is None or away is None:
                        continue  # bye week

                    # Swap home/away for second leg
                    if leg == 1:
                        home, away = away, home

                    match = {
                        "id": f"mtc_{uuid4().hex}",
                        "tournament_id": tournament_id,
                        "home_team_id": home,
                        "away_team_id": away,
                        "date": date_str,
                        "venue": body.default_venue or "",
                        "matchweek": matchweek,
                        "round": "",
                        "group_id": body.group_id or "",
                        "status": "scheduled",
                        "score_home": 0,
                        "score_away": 0,
                        "created_at": datetime.utcnow().isoformat(),
                    }
                    matches_to_create.append(match)

                # Rotate
                rotating.append(rotating.pop(0))

        if matches_to_create:
            self.repo.put_batch(matches_to_create)

        return {
            "matches_created": len(matches_to_create),
            "matchweeks_generated": matchweek,
        }

    def bulk_create(self, tournament_id: str, body: BulkMatchesRequest) -> dict[str, Any]:
        created = []
        errors = []
        for i, m in enumerate(body.matches):
            try:
                if m.home_team_id == m.away_team_id:
                    raise ValueError("home_team_id and away_team_id cannot be the same")
                item = {
                    "id": f"mtc_{uuid4().hex}",
                    "tournament_id": tournament_id,
                    "home_team_id": m.home_team_id,
                    "away_team_id": m.away_team_id,
                    "date": m.date,
                    "venue": m.venue or "",
                    "matchweek": m.matchweek or 0,
                    "round": m.round or "",
                    "group_id": m.group_id or "",
                    "status": "scheduled",
                    "score_home": 0,
                    "score_away": 0,
                    "created_at": datetime.utcnow().isoformat(),
                }
                created.append(item)
            except Exception as e:
                errors.append({"index": i, "error": str(e)})

        if created:
            self.repo.put_batch(created)

        return {"created": len(created), "errors": errors}
