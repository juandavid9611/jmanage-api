"""Sanitized, aggregate-only club overview for the unauthenticated public endpoint.

PRIVACY (hard rule): nothing person-level may leave this module. The response is
assembled from scratch out of counters and a few allowlisted scalar fields. Raw
tours (which hold bookers with names, avatars, attendance, goals), memberships,
users and account items are only ever read to be counted or summed, never copied.
"""

import threading
import time
from datetime import datetime, timezone
from typing import Any, Callable

from core.account_type import is_club

CACHE_TTL_SECONDS = 60
CACHE_MAX_ENTRIES = 256
MAX_RECENT_MATCHES = 8
MAX_UPCOMING_MATCHES = 5
MAX_TOURNAMENTS = 20
MAX_TOURS_SCANNED = 5000
MAX_TITLE_LEN = 80
PLAYER_ROLE = "user"

_MISSING = object()


def _clean_text(value: Any, limit: int = MAX_TITLE_LEN) -> str:
    return str(value or "").strip()[:limit]


def _to_int(value: Any) -> int | None:
    """Scores come back from DynamoDB as Decimal/int/str; anything else is 'no score'."""
    if isinstance(value, bool) or value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _parse_start(value: Any) -> datetime | None:
    """available.startDate is a unix timestamp (s or ms) or an ISO string."""
    if value is None or isinstance(value, bool):
        return None
    try:
        if isinstance(value, str):
            text = value.strip()
            try:
                number = float(text)
            except ValueError:
                dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
                return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        else:
            number = float(value)
        if number > 10**10:
            number /= 1000.0
        return datetime.fromtimestamp(number, tz=timezone.utc)
    except (ValueError, OverflowError, OSError):
        return None


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _outcome(home: int | None, away: int | None) -> str:
    if home is None or away is None:
        return "pending"
    if home > away:
        return "win"
    if home < away:
        return "loss"
    return "draw"


class PublicClubService:
    def __init__(
        self,
        account_repo,
        membership_repo,
        workspace_repo,
        tour_repo,
        tournament_repo,
        tournament_service=None,
        s3=None,
        clock: Callable[[], float] = time.time,
    ):
        self.account_repo = account_repo
        self.membership_repo = membership_repo
        self.workspace_repo = workspace_repo
        self.tour_repo = tour_repo
        self.tournament_repo = tournament_repo
        self.tournament_service = tournament_service
        self.s3 = s3
        self._clock = clock
        self._cache: dict[str, tuple[float, dict[str, Any] | None]] = {}
        self._lock = threading.Lock()

    # ── Public entry point ───────────────────────────────────────────

    def get_overview(self, account_id: str) -> dict[str, Any] | None:
        """The overview, or None (=> 404) for any account that is not an opted-in club."""
        now = self._clock()
        with self._lock:
            hit = self._cache.get(account_id, _MISSING)
        if hit is not _MISSING and now - hit[0] < CACHE_TTL_SECONDS:
            return hit[1]

        result = self._build(account_id, now)

        with self._lock:
            if len(self._cache) >= CACHE_MAX_ENTRIES:
                self._cache.clear()  # bounded: unknown ids from the open internet cannot grow it
            self._cache[account_id] = (now, result)
        return result

    # ── Build ────────────────────────────────────────────────────────

    def _build(self, account_id: str, now_ts: float) -> dict[str, Any] | None:
        account = self.account_repo.get(account_id)
        if not self._is_public_club(account):
            return None

        now = datetime.fromtimestamp(now_ts, tz=timezone.utc)
        tours = self.tour_repo.list_for_public_overview(account_id, MAX_TOURS_SCANNED)
        match_stats = self._tour_stats(tours, now)

        return {
            "club": {
                "name": _clean_text(account.get("name"), 120),
                "logoUrl": self._logo_url(account),
            },
            "stats": {
                "activePlayers": self._count_active_players(account_id),
                "categories": len(list(self.workspace_repo.list_all(account_id))),
                **match_stats["stats"],
            },
            "recentMatches": match_stats["recent"],
            "upcomingMatches": match_stats["upcoming"],
            "tournaments": self._public_tournaments(account_id),
        }

    @staticmethod
    def _is_public_club(account: dict[str, Any] | None) -> bool:
        if not account:
            return False
        settings = account.get("settings") or {}
        return settings.get("public_world_enabled") is True and is_club(account)

    def _logo_url(self, account: dict[str, Any]) -> str | None:
        key = ((account.get("branding") or {}).get("logo_url") or "").strip()
        if not key:
            return None
        if key.startswith("http"):
            return key
        if not self.s3:
            return None
        try:
            return self.s3.get_s3_public_url(key)
        except Exception:
            return None

    def _count_active_players(self, account_id: str) -> int:
        memberships = self.membership_repo.list_by_account(account_id)
        # Distinct people, counted only; ids are never emitted.
        return len({
            m.get("user_id")
            for m in memberships
            if m.get("status", "active") == "active" and m.get("role") == PLAYER_ROLE and m.get("user_id")
        })

    # ── Tours -> aggregates ──────────────────────────────────────────

    def _tour_stats(self, tours: list[dict[str, Any]], now: datetime) -> dict[str, Any]:
        wins = draws = losses = goals_for = goals_against = trainings = 0
        approved_total = bookers_total = 0
        past_matches: list[tuple[datetime, dict[str, Any]]] = []
        upcoming: list[tuple[datetime, dict[str, Any]]] = []

        for tour in tours:
            kind = tour.get("event_type")
            if kind not in ("match", "training"):
                continue
            start = _parse_start((tour.get("available") or {}).get("startDate"))
            if start is None:
                continue

            if start > now:
                if kind == "match":
                    upcoming.append((start, {"date": _iso(start), "title": _clean_text(tour.get("tour_name"))}))
                continue

            # Past event: aggregate attendance as bare counts (no booker is read by name).
            bookers = tour.get("bookers") or {}
            if isinstance(bookers, dict) and bookers:
                bookers_total += len(bookers)
                approved_total += sum(
                    1 for b in bookers.values() if isinstance(b, dict) and b.get("approved") is True
                )

            if kind == "training":
                trainings += 1
                continue

            scores = tour.get("scores") or {}
            home, away = _to_int(scores.get("home")), _to_int(scores.get("away"))
            outcome = _outcome(home, away)
            if outcome == "win":
                wins += 1
            elif outcome == "draw":
                draws += 1
            elif outcome == "loss":
                losses += 1
            if outcome != "pending":
                goals_for += home
                goals_against += away
            past_matches.append((start, {
                "date": _iso(start),
                "title": _clean_text(tour.get("tour_name")),
                "homeScore": home if outcome != "pending" else None,
                "awayScore": away if outcome != "pending" else None,
                "outcome": outcome,
            }))

        past_matches.sort(key=lambda p: p[0], reverse=True)
        upcoming.sort(key=lambda p: p[0])
        attendance = round(approved_total * 100 / bookers_total, 1) if bookers_total else None

        return {
            "stats": {
                "matchesPlayed": wins + draws + losses,
                "wins": wins,
                "draws": draws,
                "losses": losses,
                "goalsFor": goals_for,
                "goalsAgainst": goals_against,
                "trainingsHeld": trainings,
                "averageAttendancePct": attendance,
            },
            "recent": [m for _, m in past_matches[:MAX_RECENT_MATCHES]],
            "upcoming": [m for _, m in upcoming[:MAX_UPCOMING_MATCHES]],
        }

    # ── Tournaments ──────────────────────────────────────────────────

    def _public_tournaments(self, account_id: str) -> list[dict[str, Any]]:
        items = [t for t in self.tournament_repo.list_by_account(account_id) if t.get("is_public") is True]
        items.sort(key=lambda t: str(t.get("created_at") or ""), reverse=True)
        out = []
        for item in items[:MAX_TOURNAMENTS]:
            champion = self.tournament_service.public_champion(item) if self.tournament_service else None
            out.append({
                "id": item.get("id"),
                "name": _clean_text(item.get("name"), 120),
                "status": item.get("status"),
                "season": None if item.get("season") is None else str(item.get("season")),
                "champion": (
                    {
                        "name": champion.get("name") or "",
                        "shortName": champion.get("short_name") or "",
                        "logoUrl": champion.get("logo_url") or None,
                    }
                    if champion else None
                ),
            })
        return out
