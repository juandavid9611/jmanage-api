import re
from uuid import uuid4
from typing import Any
from repositories.s3_adapter import S3Adapter
from services.user_service import UserService
from services.tour_service import TourService
from api.schemas.calendar import ParticipationRequest, PutCalendarEvent
from repositories.calendar_repo_ddb import CalendarRepo
from services.notification_orchestator import Notifications
from builders.tour_builder import build_tour_from_calendar_event


class CalendarService:
    def __init__(self, repo: CalendarRepo, s3: S3Adapter, notifier: Notifications, tour_svc: TourService, user_svc: UserService):
        self.repo = repo
        self.notifier = notifier
        self.s3 = s3
        self.tour_svc = tour_svc
        self.user_svc = user_svc
        self._excluded_fields = ["id", "participants"]
        self._custom_mapping_keys = {"start": "event_start", "end": "event_end", "group": "user_group", "location": "event_location"}
        self._relevant_tour_fields = {"title", "event_start", "event_end", "event_location", "category", "group"}

    def get(self, calendar_event_id: str) -> dict[str, Any] | None:
        item = self.repo.get(calendar_event_id)
        if item:
            return self._map_calendar_event(item)
        return None

    def list_calendar_events(self, *, group: str | None = None) -> list[dict[str, Any]]:
        if group:
            items = self.repo.list_by_group(group)
        else:
            items = self.repo.list_all()
        return [self._map_calendar_event(i) for i in items]

    def create(self, calendar_item: PutCalendarEvent) -> dict[str, Any]:
        put_tour = build_tour_from_calendar_event(calendar_item)
        calendar_item.tourId = put_tour.id
        new_calendar_event = self._get_new_calendar_event(calendar_item)
        self.repo.put(new_calendar_event)

        users = self.user_svc.list_users(group=calendar_item.group, include_disabled=False)
        user_emails = [user["email"] for user in users]
        self.notifier.calendar_event_created(user_emails=user_emails, calendar_event=calendar_item)
        
        put_tour.calendarEventId = new_calendar_event["id"]
        self.tour_svc.create(put_tour)
        
        return self._map_calendar_event(new_calendar_event)

    def update(self, calendar_event_id: str, item: PutCalendarEvent) -> dict[str, Any] | None:
        existing = self.repo.get(calendar_event_id)
        if not existing:
            return None
        updates = self._get_needed_updates(item)
        if not updates:
            return self._map_calendar_event(existing)
        self.repo.update(calendar_event_id, updates)
        new_item = self.repo.get(calendar_event_id)
        if not new_item:
            raise ValueError(f"Calendar Event {calendar_event_id} not found after update.")
        
        tour_id = new_item.get("tour_id")
        if tour_id and self._relevant_changed(existing, new_item, self._relevant_tour_fields):
            attrs = self._tour_attrs_from_event(item)
            self.tour_svc.update_attributes(tour_id, **attrs)

        return self._map_calendar_event(new_item)
    
    def delete(self, calendar_event_id: str) -> None:
        self.repo.delete(calendar_event_id)


    def participate(self, calendar_event_id: str, user, participate_data: ParticipationRequest) -> dict[str, Any] | None:
        existing = self.repo.get(calendar_event_id)
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
            {"participants": participants}
        )
        
        tour_id = existing.get("tour_id")
        if tour_id:
            tour = self.tour_svc.get(tour_id)
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
                
                self.tour_svc.update_attributes(tour_id, bookers=bookers)
        return existing


    def _map_calendar_event(self, item: dict[str, Any]):
        item["allDay"] = item.pop("all_day", None)
        item["start"] = item.pop("event_start", None)
        item["end"] = item.pop("event_end", None)
        item["group"] = item.pop("user_group", None)
        item["tourId"] = item.pop("tour_id", None)
        item["location"] = item.pop("event_location", None)
        item["createTour"] = item.pop("create_tour", None)
        return item

    def _get_new_calendar_event(self, item: PutCalendarEvent) -> dict[str, Any]:
        return {
            "id": f"{uuid4().hex}",
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
        }

    def _get_needed_updates(self, item: PutCalendarEvent) -> dict[str, Any]:
        data = item.dict(exclude_unset=True, exclude_none=True)
        updates: dict[str, Any] = {}
        for field, value in data.items():
            if field in self._excluded_fields:
                continue
            updates[self._map_attribute_key(field)] = value
        return updates
    
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
