import re
from uuid import uuid4
from typing import Any
from repositories.s3_adapter import S3Adapter
from services.user_service import UserService
from services.tour_service import TourService
from api.schemas.calendar import ParticipationRequest, PutCalendarEvent
from repositories.calendar_repo_ddb import CalendarRepo
from repositories.club_match_repo_ddb import ClubMatchRepo
from services.account_service import AccountService
from services.club_dates import event_start_to_local_date
from services.notification_orchestator import Notifications
from builders.tour_builder import build_tour_from_calendar_event


class ClubMatchLinkError(ValueError):
    """The clubMatchId on an event does not point at a match in the event's workspace."""


class CalendarService:
    def __init__(self, repo: CalendarRepo, s3: S3Adapter, notifier: Notifications, tour_svc: TourService, user_svc: UserService, club_match_repo: ClubMatchRepo | None = None, account_svc: AccountService | None = None):
        self.repo = repo
        self.club_match_repo = club_match_repo
        self.account_svc = account_svc
        self.notifier = notifier
        self.s3 = s3
        self.tour_svc = tour_svc
        self.user_svc = user_svc
        self._excluded_fields = ["id", "participants"]
        self._custom_mapping_keys = {"start": "event_start", "end": "event_end", "group": "user_group", "location": "event_location"}
        self._relevant_tour_fields = {"title", "event_start", "event_end", "event_location", "category", "group"}

    def get(self, calendar_event_id: str, account_id: str) -> dict[str, Any] | None:
        item = self.repo.get(calendar_event_id, account_id)
        if item:
            return self._map_calendar_event(item)
        return None

    def list_calendar_events(self, account_id: str, *, group: str | None = None) -> list[dict[str, Any]]:
        if group:
            items = self.repo.list_by_group(group, account_id)
        else:
            items = self.repo.list_all(account_id)
        return [self._map_calendar_event(i) for i in items]

    def create(self, calendar_item: PutCalendarEvent, account_id: str) -> dict[str, Any]:
        put_tour = build_tour_from_calendar_event(calendar_item)
        calendar_item.tourId = put_tour.id
        club_match = self._get_linkable_match(calendar_item.clubMatchId, calendar_item.group, account_id)
        new_calendar_event = self._get_new_calendar_event(calendar_item, account_id)
        self.repo.put(new_calendar_event)
        if club_match:
            self._link_match(club_match, new_calendar_event["id"], account_id)
            self._sync_match_from_event(club_match, calendar_item, account_id)

        users = self.user_svc.list_users(account_id, group=calendar_item.group, include_disabled=False)
        user_emails = [user["email"] for user in users]
        self.notifier.calendar_event_created(user_emails=user_emails, calendar_event=calendar_item)
        
        put_tour.calendarEventId = new_calendar_event["id"]
        self.tour_svc.create(put_tour, account_id)
        
        return self._map_calendar_event(new_calendar_event)

    def update(self, calendar_event_id: str, account_id: str, item: PutCalendarEvent) -> dict[str, Any] | None:
        existing = self.repo.get(calendar_event_id, account_id)
        if not existing:
            return None
        updates = self._get_needed_updates(item)
        # clubMatchId: "" unlinks, an id links, None leaves the link as is.
        if item.clubMatchId is not None:
            self._sync_club_link(existing, calendar_event_id, item, account_id, updates)
        if not updates:
            return self._map_calendar_event(existing)
        self.repo.update(calendar_event_id, account_id, updates)
        new_item = self.repo.get(calendar_event_id, account_id)
        if not new_item:
            raise ValueError(f"Calendar Event {calendar_event_id} not found after update.")
        
        tour_id = new_item.get("tour_id")
        if tour_id and self._relevant_changed(existing, new_item, self._relevant_tour_fields):
            attrs = self._tour_attrs_from_event(item)
            self.tour_svc.update_attributes(tour_id, account_id, **attrs)

        linked_match_id = new_item.get("club_match_id")
        if linked_match_id:
            match = self._get_match(linked_match_id, account_id)
            if match:
                self._sync_match_from_event(match, item, account_id)

        return self._map_calendar_event(new_item)
    
    def delete(self, calendar_event_id: str, account_id: str) -> None:
        existing = self.repo.get(calendar_event_id, account_id)
        match_id = existing.get("club_match_id") if existing else None
        match = self._get_match(match_id, account_id) if match_id else None
        if match and match.get("calendar_event_id") == calendar_event_id:
            self.club_match_repo.update(match["id"], account_id, {"calendar_event_id": None})
        self.repo.delete(calendar_event_id, account_id)


    def participate(self, calendar_event_id: str, account_id: str, user, participate_data: ParticipationRequest) -> dict[str, Any] | None:
        existing = self.repo.get(calendar_event_id, account_id)
        if not existing:
            return None
        
        user_id = user.get("sub")
        user_name = user.get("name")
        if not user_id or not user_name:
            raise ValueError("User ID and User Name must be provided in participate_data.")
        
        participants = existing.get("participants", {})
        if participate_data.value == True and user_id not in participants:
            participants[user_id] = user_name
        if participate_data.value == False and user_id in participants:
            del participants[user_id]
        
        self.repo.update(
            calendar_event_id,
            account_id,
            {"participants": participants}
        )
        
        tour_id = existing.get("tour_id")
        if tour_id:
            tour = self.tour_svc.get(tour_id, account_id)
            if not tour:
                print(f"Tour {tour_id} not found")
            else:
                bookers = tour.get("bookers", {})
                if participate_data.value == True and user_id not in bookers:
                    user_booked = {
                        "id": user_id,
                        "name": user_name,
                        "avatarUrl": None,
                        "guests": 1,
                        "approved": True,
                        "late": False,
                        "yellowCard": False,
                        "redCard": False,
                        "mvp": False,
                        "goals": 0,
                        "assists": 0,
                    }
                    bookers[user_id] = user_booked
                if participate_data.value == False and user_id in bookers:
                    del bookers[user_id]
                
                self.tour_svc.update_attributes(tour_id, account_id, bookers=bookers)
        return existing


    def _map_calendar_event(self, item: dict[str, Any]):
        item["allDay"] = item.pop("all_day", None)
        item["start"] = item.pop("event_start", None)
        item["end"] = item.pop("event_end", None)
        item["group"] = item.pop("user_group", None)
        item["tourId"] = item.pop("tour_id", None)
        item["location"] = item.pop("event_location", None)
        item["createTour"] = item.pop("create_tour", None)
        item["clubMatchId"] = item.pop("club_match_id", None)
        return item

    def _get_new_calendar_event(self, item: PutCalendarEvent, account_id: str) -> dict[str, Any]:
        return {
            "id": f"{uuid4().hex}",
            "account_id": account_id,
            "all_day": item.allDay,
            "color": item.color,
            "description": item.description,
            "event_location": item.location,
            "event_start": item.start,
            "event_end": item.end,
            "title": item.title,
            "category": item.category,
            "participants": {},
            "user_group": item.group,
            "create_tour": True,
            "tour_id": item.tourId,
            "club_match_id": item.clubMatchId or None,
        }

    def _get_needed_updates(self, item: PutCalendarEvent) -> dict[str, Any]:
        data = item.dict(exclude_unset=True, exclude_none=True)
        updates: dict[str, Any] = {}
        for field, value in data.items():
            if field in self._excluded_fields or field == "clubMatchId":
                continue  # the club link is handled by _sync_club_link
            updates[self._map_attribute_key(field)] = value
        return updates

    # ── Club match link ──────────────────────────────────────────────

    def _get_match(self, match_id: str, account_id: str) -> dict[str, Any] | None:
        if not self.club_match_repo:
            return None
        return self.club_match_repo.get(match_id, account_id)

    def _get_linkable_match(self, match_id: str | None, group: str, account_id: str) -> dict[str, Any] | None:
        """The match to link, validated to exist in this account and workspace. None when no link is requested."""
        if not match_id:
            return None
        match = self._get_match(match_id, account_id)
        if not match or match.get("workspace_id") != group:
            raise ClubMatchLinkError(f"Club match {match_id} not found in this workspace")
        return match

    def _account_timezone(self, account_id: str) -> str | None:
        account = self.account_svc.get(account_id) if self.account_svc else None
        return ((account or {}).get("settings") or {}).get("timezone")

    def _sync_match_from_event(self, match: dict[str, Any], event: PutCalendarEvent, account_id: str) -> None:
        """Copy the event's date (account timezone) and title (as rival) onto the linked match."""
        self.club_match_repo.update(
            match["id"],
            account_id,
            {
                "date": event_start_to_local_date(event.start, self._account_timezone(account_id)),
                "rival": event.title,
            },
        )

    def _link_match(self, match: dict[str, Any], event_id: str, account_id: str) -> None:
        if match.get("calendar_event_id") and match["calendar_event_id"] != event_id:
            self._clear_event_link(match["calendar_event_id"], match["id"], account_id)
        self.club_match_repo.update(match["id"], account_id, {"calendar_event_id": event_id})

    def _clear_event_link(self, event_id: str, match_id: str, account_id: str) -> None:
        """Unlink a (previous) event that still points at this match."""
        other = self.repo.get(event_id, account_id)
        if other and other.get("club_match_id") == match_id:
            self.repo.update(event_id, account_id, {"club_match_id": None})

    def _sync_club_link(
        self, existing: dict[str, Any], event_id: str, item: PutCalendarEvent, account_id: str, updates: dict[str, Any]
    ) -> None:
        """Apply an explicit clubMatchId on update ("" = unlink), adding the event-side change to `updates`."""
        old_match_id = existing.get("club_match_id")
        new_match_id = item.clubMatchId or None
        if new_match_id == old_match_id:
            return
        if old_match_id:
            old_match = self._get_match(old_match_id, account_id)
            if old_match and old_match.get("calendar_event_id") == event_id:
                self.club_match_repo.update(old_match_id, account_id, {"calendar_event_id": None})
        updates["club_match_id"] = new_match_id
        if new_match_id:
            match = self._get_linkable_match(new_match_id, item.group, account_id)
            self._link_match(match, event_id, account_id)  # date/rival sync happens after the update
    
    def _map_attribute_key(self, key: str) -> str:
        if key in self._custom_mapping_keys:
            return self._custom_mapping_keys[key]
        return re.sub(r'(?<!^)(?=[A-Z])', '_', key).lower()

    def _tour_attrs_from_event(self, evt: PutCalendarEvent) -> dict[str, Any]:
        """
        Compute the tour fields to SET from the current event payload.
        Uses your pure builder for consistency, but returns only a dict of attrs.
        """
        draft = build_tour_from_calendar_event(evt)
        return {
            "name": draft.name,
            "services": draft.services,
            "available": draft.available,
            "location": draft.location,
            "eventType": draft.eventType,
            "group": draft.group,
            "calendarEventId": draft.calendarEventId,
        }

    def _relevant_changed(self, old: dict[str, Any], new: dict[str, Any], relevant_fields: set[str]) -> bool:
        """
        Compare old vs new using *stored* attribute names for relevant fields only.
        """
        for api_field in relevant_fields:
            stored = self._map_attribute_key(api_field)
            if old.get(stored) != new.get(stored):
                return True
        return False
