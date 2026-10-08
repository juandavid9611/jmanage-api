from datetime import date, datetime, timezone
from typing import Any
from uuid import uuid4

from api.schemas.club_tournaments import (
    LineupIn,
    MatchIn,
    RosterBulkIn,
    RosterEntryIn,
    RosterEntryUpdate,
    TournamentIn,
)
from repositories.calendar_repo_ddb import CalendarRepo
from repositories.club_match_repo_ddb import ClubMatchRepo
from repositories.club_roster_repo_ddb import ClubRosterRepo
from repositories.club_tournament_repo_ddb import ClubTournamentRepo

LINEUP_STATUSES = ("titular", "suplente", "")


def normalize_roster_identity(user_id: str | None, guest_name: str | None) -> tuple[str | None, str | None]:
    """A roster entry is a real user XOR a named guest. Returns the cleaned (user_id, guest_name)."""
    user_id = (user_id or "").strip() or None
    guest_name = (guest_name or "").strip() or None
    if user_id and guest_name:
        raise ValueError("A roster entry has either a user or a guest name, not both")
    if not user_id and not guest_name:
        raise ValueError("A roster entry needs a user or a guest name")
    return user_id, guest_name


def validate_match_date(value: str) -> str:
    try:
        date.fromisoformat(value)
    except ValueError:
        raise ValueError(f"Invalid match date '{value}', expected YYYY-MM-DD")
    return value


def validate_lineup(entries: list[dict[str, Any]], roster_ids: set[str]) -> list[dict[str, Any]]:
    """Check lineup entries against the tournament roster and return them normalized."""
    seen: set[str] = set()
    out = []
    for e in entries:
        rid = e["roster_entry_id"]
        if rid not in roster_ids:
            raise ValueError(f"Roster entry {rid} does not belong to this tournament")
        if rid in seen:
            raise ValueError(f"Roster entry {rid} appears more than once in the lineup")
        seen.add(rid)
        status = e.get("status") or ""
        if status not in LINEUP_STATUSES:
            raise ValueError(f"Invalid lineup status '{status}'")
        out.append({
            "roster_entry_id": rid,
            "called_up": bool(e["called_up"]),
            "status": status,
            "minutes": int(e.get("minutes") or 0),
        })
    return out


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class ClubTournamentService:
    def __init__(
        self,
        tournament_repo: ClubTournamentRepo,
        roster_repo: ClubRosterRepo,
        match_repo: ClubMatchRepo,
        calendar_repo: CalendarRepo,
    ):
        self.tournament_repo = tournament_repo
        self.roster_repo = roster_repo
        self.match_repo = match_repo
        self.calendar_repo = calendar_repo

    # ── Tournaments ──────────────────────────────────────────────────

    def list_tournaments(self, workspace_id: str, account_id: str) -> list[dict[str, Any]]:
        items = self.tournament_repo.list_by_workspace(workspace_id, account_id)
        items.sort(key=lambda i: i.get("created_at", ""), reverse=True)
        return [self._map_tournament(i) for i in items]

    def get_tournament(self, tournament_id: str, workspace_id: str, account_id: str) -> dict[str, Any] | None:
        item = self._tournament(tournament_id, workspace_id, account_id)
        return self._map_tournament(item) if item else None

    def create_tournament(self, workspace_id: str, body: TournamentIn, account_id: str) -> dict[str, Any]:
        now = _now()
        item = {
            "id": uuid4().hex,
            "account_id": account_id,
            "workspace_id": workspace_id,
            "name": body.name.strip(),
            "category": body.category.strip(),
            "created_at": now,
            "updated_at": now,
        }
        self.tournament_repo.put(item)
        return self._map_tournament(item)

    def update_tournament(
        self, tournament_id: str, workspace_id: str, account_id: str, body: TournamentIn
    ) -> dict[str, Any] | None:
        if not self._tournament(tournament_id, workspace_id, account_id):
            return None
        self.tournament_repo.update(
            tournament_id,
            account_id,
            {"name": body.name.strip(), "category": body.category.strip(), "updated_at": _now()},
        )
        return self.get_tournament(tournament_id, workspace_id, account_id)

    def delete_tournament(self, tournament_id: str, workspace_id: str, account_id: str) -> bool:
        """Cascade: matches (clearing calendar links) and roster first, the tournament last so a retry still works."""
        if not self._tournament(tournament_id, workspace_id, account_id):
            return False
        for match in self.match_repo.list_by_tournament(tournament_id, account_id):
            self._unlink_calendar_event(match, account_id)
            self.match_repo.delete(match["id"], account_id)
        for entry in self.roster_repo.list_by_tournament(tournament_id, account_id):
            self.roster_repo.delete(entry["id"], account_id)
        self.tournament_repo.delete(tournament_id, account_id)
        return True

    def my_tournaments(self, user_id: str, workspace_id: str, account_id: str) -> list[dict[str, Any]]:
        """Tournaments where the caller is rostered, with their roster entry id."""
        out = []
        for entry in self.roster_repo.list_by_user(user_id, account_id):
            if entry.get("workspace_id") != workspace_id:
                continue
            tournament = self._tournament(entry["tournament_id"], workspace_id, account_id)
            if tournament:
                out.append({"rosterEntryId": entry["id"], "tournament": self._map_tournament(tournament)})
        return out

    # ── Roster ───────────────────────────────────────────────────────

    def list_roster(self, tournament_id: str, workspace_id: str, account_id: str) -> list[dict[str, Any]] | None:
        if not self._tournament(tournament_id, workspace_id, account_id):
            return None
        items = self.roster_repo.list_by_tournament(tournament_id, account_id)
        items.sort(key=lambda i: i.get("created_at", ""))
        return [self._map_roster(i) for i in items]

    def add_roster_entry(
        self, tournament_id: str, workspace_id: str, account_id: str, body: RosterEntryIn
    ) -> dict[str, Any] | None:
        if not self._tournament(tournament_id, workspace_id, account_id):
            return None
        taken = self._rostered_user_ids(tournament_id, account_id)
        return self._add_roster_entry(tournament_id, workspace_id, account_id, body, taken)

    def bulk_add_roster(
        self, tournament_id: str, workspace_id: str, account_id: str, body: RosterBulkIn
    ) -> dict[str, Any] | None:
        """Add each row independently; one bad row does not block the rest."""
        if not self._tournament(tournament_id, workspace_id, account_id):
            return None
        taken = self._rostered_user_ids(tournament_id, account_id)
        results = []
        for index, row in enumerate(body.entries):
            try:
                entry = self._add_roster_entry(tournament_id, workspace_id, account_id, row, taken)
                results.append({"index": index, "ok": True, "entry": entry})
            except ValueError as e:
                results.append({"index": index, "ok": False, "error": str(e)})
        added = sum(1 for r in results if r["ok"])
        return {"results": results, "added": added, "failed": len(results) - added}

    def update_roster_entry(
        self, tournament_id: str, entry_id: str, workspace_id: str, account_id: str, body: RosterEntryUpdate
    ) -> dict[str, Any] | None:
        entry = self._roster_entry(tournament_id, entry_id, workspace_id, account_id)
        if not entry:
            return None
        updates: dict[str, Any] = {}
        fields = body.model_fields_set
        if "number" in fields:
            updates["number"] = body.number
        if "position" in fields and body.position is not None:
            updates["position"] = body.position
        if "guest_name" in fields and body.guest_name is not None:
            if entry.get("user_id"):
                raise ValueError("A rostered user has no guest name")
            name = body.guest_name.strip()
            if not name:
                raise ValueError("Guest name cannot be empty")
            updates["guest_name"] = name
        if updates:
            updates["updated_at"] = _now()
            self.roster_repo.update(entry_id, account_id, updates)
        return self._map_roster(self.roster_repo.get(entry_id, account_id))

    def remove_roster_entry(self, tournament_id: str, entry_id: str, workspace_id: str, account_id: str) -> bool:
        if not self._roster_entry(tournament_id, entry_id, workspace_id, account_id):
            return False
        # Drop the player from saved lineups so stats don't count a ghost.
        for match in self.match_repo.list_by_tournament(tournament_id, account_id):
            lineup = match.get("lineup")
            if lineup and any(e["roster_entry_id"] == entry_id for e in lineup["entries"]):
                lineup = {**lineup, "entries": [e for e in lineup["entries"] if e["roster_entry_id"] != entry_id]}
                self.match_repo.update(match["id"], account_id, {"lineup": lineup})
        self.roster_repo.delete(entry_id, account_id)
        return True

    # ── Matches and lineups ──────────────────────────────────────────

    def list_matches(self, tournament_id: str, workspace_id: str, account_id: str) -> list[dict[str, Any]] | None:
        if not self._tournament(tournament_id, workspace_id, account_id):
            return None
        return [self._map_match(m) for m in self.match_repo.list_by_tournament(tournament_id, account_id)]

    def create_match(
        self, tournament_id: str, workspace_id: str, account_id: str, body: MatchIn
    ) -> dict[str, Any] | None:
        if not self._tournament(tournament_id, workspace_id, account_id):
            return None
        now = _now()
        item = {
            "id": uuid4().hex,
            "account_id": account_id,
            "workspace_id": workspace_id,
            "tournament_id": tournament_id,
            "date": validate_match_date(body.date),
            "rival": body.rival.strip(),
            "calendar_event_id": None,
            "lineup": None,
            "created_at": now,
            "updated_at": now,
        }
        self.match_repo.put(item)
        return self._map_match(item)

    def update_match(
        self, tournament_id: str, match_id: str, workspace_id: str, account_id: str, body: MatchIn
    ) -> dict[str, Any] | None:
        if not self._match(tournament_id, match_id, workspace_id, account_id):
            return None
        self.match_repo.update(
            match_id,
            account_id,
            {"date": validate_match_date(body.date), "rival": body.rival.strip(), "updated_at": _now()},
        )
        return self._map_match(self.match_repo.get(match_id, account_id))

    def delete_match(self, tournament_id: str, match_id: str, workspace_id: str, account_id: str) -> bool:
        match = self._match(tournament_id, match_id, workspace_id, account_id)
        if not match:
            return False
        self._unlink_calendar_event(match, account_id)
        self.match_repo.delete(match_id, account_id)
        return True

    def save_lineup(
        self, tournament_id: str, match_id: str, workspace_id: str, account_id: str, body: LineupIn
    ) -> dict[str, Any] | None:
        if not self._match(tournament_id, match_id, workspace_id, account_id):
            return None
        roster_ids = {r["id"] for r in self.roster_repo.list_by_tournament(tournament_id, account_id)}
        entries = validate_lineup([e.model_dump() for e in body.entries], roster_ids)
        now = _now()
        self.match_repo.update(
            match_id, account_id, {"lineup": {"entries": entries, "saved_at": now}, "updated_at": now}
        )
        return self._map_match(self.match_repo.get(match_id, account_id))

    # ── Internals ────────────────────────────────────────────────────

    def _tournament(self, tournament_id: str, workspace_id: str, account_id: str) -> dict[str, Any] | None:
        item = self.tournament_repo.get(tournament_id, account_id)
        if not item or item.get("workspace_id") != workspace_id:
            return None
        return item

    def _roster_entry(
        self, tournament_id: str, entry_id: str, workspace_id: str, account_id: str
    ) -> dict[str, Any] | None:
        entry = self.roster_repo.get(entry_id, account_id)
        if not entry or entry.get("tournament_id") != tournament_id or entry.get("workspace_id") != workspace_id:
            return None
        return entry

    def _match(self, tournament_id: str, match_id: str, workspace_id: str, account_id: str) -> dict[str, Any] | None:
        match = self.match_repo.get(match_id, account_id)
        if not match or match.get("tournament_id") != tournament_id or match.get("workspace_id") != workspace_id:
            return None
        return match

    def _rostered_user_ids(self, tournament_id: str, account_id: str) -> set[str]:
        return {
            r["user_id"] for r in self.roster_repo.list_by_tournament(tournament_id, account_id) if r.get("user_id")
        }

    def _add_roster_entry(
        self, tournament_id: str, workspace_id: str, account_id: str, body: RosterEntryIn, taken_user_ids: set[str]
    ) -> dict[str, Any]:
        user_id, guest_name = normalize_roster_identity(body.user_id, body.guest_name)
        if user_id and user_id in taken_user_ids:
            raise ValueError("This user is already in the roster of this tournament")
        now = _now()
        item: dict[str, Any] = {
            "id": uuid4().hex,
            "account_id": account_id,
            "workspace_id": workspace_id,
            "tournament_id": tournament_id,
            "number": body.number,
            "position": body.position.strip(),
            "created_at": now,
            "updated_at": now,
        }
        # user_id keys a sparse GSI, so guests must not carry the attribute at all.
        if user_id:
            item["user_id"] = user_id
        else:
            item["guest_name"] = guest_name
        self.roster_repo.put(item)
        if user_id:
            taken_user_ids.add(user_id)
        return self._map_roster(item)

    def _unlink_calendar_event(self, match: dict[str, Any], account_id: str) -> None:
        """Clear the calendar event's pointer to this match so it doesn't dangle."""
        event_id = match.get("calendar_event_id")
        if not event_id:
            return
        event = self.calendar_repo.get(event_id, account_id)
        if event and event.get("club_match_id") == match["id"]:
            self.calendar_repo.update(event_id, account_id, {"club_match_id": None})

    @staticmethod
    def _map_tournament(item: dict[str, Any]) -> dict[str, Any]:
        return {
            "id": item["id"],
            "workspaceId": item.get("workspace_id"),
            "name": item.get("name"),
            "category": item.get("category", ""),
            "createdAt": item.get("created_at"),
        }

    @staticmethod
    def _map_roster(item: dict[str, Any]) -> dict[str, Any]:
        return {
            "id": item["id"],
            "tournamentId": item.get("tournament_id"),
            "userId": item.get("user_id"),
            "guestName": item.get("guest_name"),
            "number": item.get("number"),
            "position": item.get("position", ""),
        }

    @staticmethod
    def _map_match(item: dict[str, Any]) -> dict[str, Any]:
        lineup = item.get("lineup")
        return {
            "id": item["id"],
            "tournamentId": item.get("tournament_id"),
            "date": item.get("date"),
            "rival": item.get("rival"),
            "calendarEventId": item.get("calendar_event_id"),
            "lineup": {
                "entries": [
                    {
                        "rosterEntryId": e["roster_entry_id"],
                        "calledUp": e["called_up"],
                        "status": e.get("status", ""),
                        "minutes": e.get("minutes", 0),
                    }
                    for e in lineup["entries"]
                ],
                "savedAt": lineup.get("saved_at"),
            } if lineup else None,
        }
