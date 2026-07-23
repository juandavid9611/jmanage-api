import calendar
from uuid import uuid4
from datetime import date, datetime, timezone
from typing import Any
from zoneinfo import ZoneInfo

from repositories.votation_repo_ddb import VotationRepo
from repositories.tour_repo_ddb import TourRepo
from services.notification_orchestator import Notifications

BOGOTA_TZ = ZoneInfo("America/Bogota")  # UTC-5


def _parse_start_date(available: Any) -> datetime | None:
    """
    Parse startDate from the tour's 'available' dict and return a
    timezone-aware datetime in America/Bogota (UTC-5).

    Training tours store startDate as a Unix epoch number (ms or s).
    Match tours store startDate as an ISO-8601 string with offset (e.g. 2026-03-29T10:00:00-05:00).
    """
    if not isinstance(available, dict):
        return None
    raw = available.get("startDate")
    if raw is None:
        return None

    # --- Numeric epoch (training tours) ---
    try:
        ts = float(raw)
        if ts > 1e10:          # milliseconds → seconds
            ts /= 1000
        return datetime.fromtimestamp(ts, tz=timezone.utc).astimezone(BOGOTA_TZ)
    except (TypeError, ValueError):
        pass

    # --- ISO string (match tours) ---
    try:
        dt = datetime.fromisoformat(str(raw))
        if dt.tzinfo is not None:
            return dt.astimezone(BOGOTA_TZ)
        # Naive string (no offset) — treat as already in Bogota local time
        return dt.replace(tzinfo=BOGOTA_TZ)
    except ValueError:
        return None


class VotationService:
    def __init__(
        self,
        repo: VotationRepo,
        tour_repo: TourRepo,
        user_svc,
        notifier: Notifications,
    ):
        self.repo = repo
        self.tour_repo = tour_repo
        self.user_svc = user_svc
        self.notifier = notifier

    # ── Candidate preview ────────────────────────────────────────────────

    def preview_candidates(
        self,
        workspace_id: str,
        min_pct: int,
        account_id: str,
        month: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> list[dict[str, Any]]:
        """
        Compute candidate list from training tour data over a date window.
        Pass either `month` ("YYYY-MM") or both `start_date`/`end_date` ("YYYY-MM-DD").
        """
        if month is not None:
            year, month_num = map(int, month.split("-"))
            window_start, window_end = self._month_bounds(year, month_num)
        else:
            window_start = date.fromisoformat(start_date)
            window_end = date.fromisoformat(end_date)

        training_tours = list(
            self.tour_repo.list_filtered(account_id, group=workspace_id, tour_type="training")
        )
        match_tours = list(
            self.tour_repo.list_filtered(account_id, group=workspace_id, tour_type="match")
        )

        window_tours = [
            t for t in training_tours
            if self._in_window(t.get("available"), window_start, window_end)
        ]
        window_match_tours = [
            t for t in match_tours
            if self._in_window(t.get("available"), window_start, window_end)
        ]

        if not window_tours:
            return []

        total = len(window_tours)
        total_matches = len(window_match_tours)
        player_map: dict[str, dict] = {}

        for session in window_tours:
            bookers = session.get("bookers") or {}
            for booker in bookers.values():
                pid = booker.get("id")
                if not pid:
                    continue
                if pid not in player_map:
                    player_map[pid] = {
                        "id": pid,
                        "name": booker.get("name", ""),
                        "avatar_url": booker.get("avatarUrl") or booker.get("avatar_url"),
                        "attended": 0,
                        "match_attended": 0,
                        "goals": 0,
                        "assists": 0,
                        "mvp": 0,
                    }
                if booker.get("approved"):
                    player_map[pid]["attended"] += 1
                player_map[pid]["goals"] += int(booker.get("goals") or 0)
                player_map[pid]["assists"] += int(booker.get("assists") or 0)

        for session in window_match_tours:
            bookers = session.get("bookers") or {}
            for booker in bookers.values():
                pid = booker.get("id")
                if not pid or pid not in player_map:
                    continue
                if booker.get("approved"):
                    player_map[pid]["match_attended"] += 1
                player_map[pid]["goals"] += int(booker.get("goals") or 0)
                player_map[pid]["assists"] += int(booker.get("assists") or 0)
                if booker.get("mvp"):
                    player_map[pid]["mvp"] += 1

        candidates = []
        for player in player_map.values():
            training_pct = round((player["attended"] / total) * 100)
            if training_pct >= min_pct:
                match_pct = (
                    round((player["match_attended"] / total_matches) * 100)
                    if total_matches > 0
                    else 0
                )
                candidates.append({
                    "id": player["id"],
                    "name": player["name"],
                    "avatar_url": player["avatar_url"],
                    "training_pct": training_pct,
                    "match_pct": match_pct,
                    "goals": player["goals"],
                    "assists": player["assists"],
                    "mvp": player["mvp"],
                    "eligible": True,
                })

        return sorted(candidates, key=lambda c: c["training_pct"], reverse=True)

    # ── CRUD ─────────────────────────────────────────────────────────────

    def create_votation(
        self,
        workspace_id: str,
        min_pct: int,
        candidates: list[dict],
        created_by: str,
        account_id: str,
        period_type: str = "month",
        month: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> dict[str, Any]:
        item = {
            "id": str(uuid4()),
            "account_id": account_id,
            "workspace_id": workspace_id,
            "status": "open",
            "period_type": period_type,
            "min_pct": min_pct,
            "candidates": candidates,
            "votes": {},
            "winner_id": None,
            "created_at": datetime.utcnow().isoformat(),
            "created_by": created_by,
        }
        if period_type == "semester":
            item["start_date"] = start_date
            item["end_date"] = end_date
        else:
            item["month"] = month
        self.repo.put(item)

        # Notify workspace members
        try:
            users = self.user_svc.list_users(account_id, group=workspace_id)
            user_emails = [u["email"] for u in users if u.get("email")]
            if user_emails:
                self.notifier.votation_opened(
                    user_emails=user_emails,
                    period_type=period_type,
                    month=month,
                    start_date=start_date,
                    end_date=end_date,
                )
        except Exception:
            pass  # notification failure should not block creation

        return item

    def get_votation(self, votation_id: str, account_id: str) -> dict[str, Any] | None:
        return self.repo.get(votation_id, account_id)

    def list_votations(self, workspace_id: str, account_id: str) -> list[dict[str, Any]]:
        items = [
            v for v in self.repo.list_by_workspace(workspace_id, account_id)
            if not v.get("parent_votation_id")
        ]
        for item in items:
            if item.get("status") == "tied" and item.get("tiebreaker_votation_id"):
                tb = self.repo.get(item["tiebreaker_votation_id"], account_id)
                if tb and tb.get("status") == "closed" and tb.get("winner_id"):
                    item["tiebreaker_winner"] = next(
                        (c for c in tb.get("candidates", []) if c["id"] == tb["winner_id"]),
                        None,
                    )
        return sorted(items, key=lambda v: v.get("created_at", ""), reverse=True)

    def update_candidates(
        self,
        votation_id: str,
        account_id: str,
        candidates: list[dict],
    ) -> dict[str, Any] | None:
        item = self.repo.get(votation_id, account_id)
        if not item or item.get("status") != "draft":
            return None
        self.repo.update_candidates(votation_id, account_id, candidates)
        return self.repo.get(votation_id, account_id)

    def cast_vote(
        self,
        votation_id: str,
        voter_id: str,
        candidate_id: str,
        account_id: str,
    ) -> dict[str, Any]:
        item = self.repo.get(votation_id, account_id)
        if not item:
            raise ValueError("Votation not found")
        if item.get("status") != "open":
            raise ValueError("Votation is not open")

        eligible_ids = {c["id"] for c in item.get("candidates", []) if c.get("eligible")}
        if candidate_id not in eligible_ids:
            raise ValueError("Candidate not eligible")

        accepted = self.repo.cast_vote(votation_id, account_id, voter_id, candidate_id)
        if not accepted:
            raise ValueError("Votation is not open")

        return self.repo.get(votation_id, account_id)

    def delete_votation(self, votation_id: str, workspace_id: str, account_id: str) -> None:
        item = self.repo.get(votation_id, account_id)
        if item and item.get("tiebreaker_votation_id"):
            try:
                self.repo.delete(item["tiebreaker_votation_id"], account_id)
            except ValueError:
                pass  # tiebreaker already gone
        self.repo.delete(votation_id, account_id)

    def close_votation(self, votation_id: str, account_id: str) -> dict[str, Any] | None:
        item = self.repo.get(votation_id, account_id)
        if not item or item.get("status") != "open":
            return None

        votes: dict = item.get("votes") or {}
        vote_counts: dict[str, int] = {}
        for cid in votes.values():
            vote_counts[cid] = vote_counts.get(cid, 0) + 1

        if not vote_counts:
            self.repo.set_winner(votation_id, account_id, "")
            return self.repo.get(votation_id, account_id)

        max_votes = max(vote_counts.values())
        tied = [cid for cid, cnt in vote_counts.items() if cnt == max_votes]

        if len(tied) >= 2:
            self.repo.set_tied(votation_id, account_id, tied)
            return self.repo.get(votation_id, account_id)

        self.repo.set_winner(votation_id, account_id, tied[0])
        return self.repo.get(votation_id, account_id)

    def create_tiebreaker(
        self,
        votation_id: str,
        workspace_id: str,
        account_id: str,
        created_by: str,
    ) -> dict[str, Any] | None:
        original = self.repo.get(votation_id, account_id)
        if not original:
            return None
        if original.get("status") != "tied":
            return None
        if original.get("tiebreaker_votation_id"):
            return None

        tied_ids = set(original.get("tied_candidate_ids") or [])
        finalists = [c for c in (original.get("candidates") or []) if c["id"] in tied_ids]

        period_type = original.get("period_type", "month")
        new_item = {
            "id": str(uuid4()),
            "account_id": account_id,
            "workspace_id": workspace_id,
            "status": "open",
            "period_type": period_type,
            "min_pct": 0,
            "candidates": finalists,
            "votes": {},
            "winner_id": None,
            "created_at": datetime.utcnow().isoformat(),
            "created_by": created_by,
            "parent_votation_id": votation_id,
        }
        if period_type == "semester":
            new_item["start_date"] = original.get("start_date")
            new_item["end_date"] = original.get("end_date")
        else:
            new_item["month"] = original.get("month")
        self.repo.put(new_item)
        self.repo.set_tiebreaker_id(votation_id, account_id, new_item["id"])
        return new_item

    # ── Internal helpers ─────────────────────────────────────────────────

    @staticmethod
    def _month_bounds(year: int, month_num: int) -> tuple[date, date]:
        last_day = calendar.monthrange(year, month_num)[1]
        return date(year, month_num, 1), date(year, month_num, last_day)

    @staticmethod
    def _in_window(available: Any, window_start: date, window_end: date) -> bool:
        d = _parse_start_date(available)
        if not d:
            return False
        return window_start <= d.date() <= window_end
