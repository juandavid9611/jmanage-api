import re
from uuid import uuid4
from typing import Any
from datetime import datetime
from api.schemas.files import FileSpec
from repositories.s3_adapter import S3Adapter
from repositories.tour_repo_ddb import TourRepo
from api.schemas.calendar import PutCalendarEvent
from api.schemas.tours import PatchProperty, PutTour
from services.notification_orchestator import Notifications
from utils.datetime_utils import format_datetime_pretty_es, parse_timestamp_to_datetime


class TourService:
    def __init__(self, repo: TourRepo, s3: S3Adapter, notifier: Notifications):
        self.repo = repo
        self.notifier = notifier
        self.s3 = s3
        self._excluded_fields = ["id"]
        self._custom_mapping_keys = {"name": "tour_name", "location": "event_location"}
        self._booker_bool_fields = {"approved", "late", "yellowCard", "redCard", "mvp"}
        self._booker_int_fields = {"goals", "assists"}

    def get(self, tour_id: str, account_id: str) -> dict[str, Any] | None:
        item = self.repo.get(tour_id, account_id)
        if item:
            return self._map_tour(item)
        return None

    def list_tours(self, account_id: str, *, group: str | None = None, tour_type: str | None = None) -> list[dict[str, Any]]:
        if group:
            items = self.repo.list_by_group(group, account_id)
        elif tour_type:
            items = self.repo.list_by_type(tour_type, account_id)
        else:
            items = self.repo.list_all(account_id)
        return [self._map_tour(i) for i in items]

    def create(self, item: PutTour, account_id: str) -> dict[str, Any]:
        new_tour = self._get_new_tour(item, account_id)
        self.repo.put(new_tour)
        return self._map_tour(new_tour)

    def update(self, tour_id: str, account_id: str, item: PutTour) -> dict[str, Any] | None:
        existing = self.repo.get(tour_id, account_id)
        if not existing:
            return None
        updates = self._get_needed_updates(item)
        if not updates:
            return self._map_tour(existing)
        self.repo.update(tour_id, account_id, updates)
        new_item = self.repo.get(tour_id, account_id)
        if not new_item:
            raise ValueError(f"Tour {tour_id} not found after update.")
        return self._map_tour(new_item)

    def update_attributes(self, tour_id: str, account_id: str, **attrs) -> dict[str, Any] | None:
        existing = self.repo.get(tour_id, account_id)
        if not existing:
            return None
        updates: dict[str, Any] = {}
        for k, v in attrs.items():
            if k in self._excluded_fields or v is None:
                continue
            updates[self._map_attribute_key(k)] = v

        if not updates:
            return self._map_tour(existing)

        self.repo.update(tour_id, account_id, updates)
        new_item = self.repo.get(tour_id, account_id)
        if not new_item:
            raise ValueError(f"Tour {tour_id} not found after update.")
        return self._map_tour(new_item)

    def delete(self, tour_id: str, account_id: str) -> None:
        self.repo.delete(tour_id, account_id)

    def generate_put_presigned_urls(self, tour_id: str, account_id: str, files: list[FileSpec]) -> dict[str, dict[str, str]]:
        # TODO failing with multiple files in different calls - check why
        presigned_urls = {}
        tour = self.get(tour_id, account_id)
        if not tour:
            raise ValueError(f"Tour {tour_id} not found")
        # TODO Extract into S3Adapter method or other service. Works for payments too
        for file in files:
            if not isinstance(file, FileSpec):
                raise TypeError("Each file must be a FileSpec instance.")

            file_name = file.file_name
            file_content_type = file.content_type
            if not file_name or not file_content_type:
                raise ValueError("File 'name' and 'content_type' cannot be empty.")
            
            result = self.s3.presign_tour_image_put(
                account_id=account_id,
                tour_id=tour_id, 
                filename=file_name, 
                content_type=file_content_type
            )
            presigned_urls[file_name] = result["url"]
        return presigned_urls

    def add_images(self, tour_id: str, account_id: str, file_names: list[str]) -> list[str]:
        existing = self.get(tour_id, account_id)
        if not existing:
            raise ValueError(f"Tour {tour_id} not found")
        images = []
        for file_name in file_names:
            key = self.s3._kb.tour_image(account_id, tour_id, file_name)
            images.append(key)
        self.repo.update(
            tour_id, 
            account_id,
            {
                "images": images
            }
        )
        return images

    def update_booker_property(self, tour_id: str, account_id: str, booker_id: str, patch_property: PatchProperty) -> dict[str, Any] | None:
        existing = self.repo.get(tour_id, account_id)
        if not existing:
            return None
        bookers = existing.get("bookers", {})
        if booker_id not in bookers:
            raise ValueError(f"Booker {booker_id} not found in Tour {tour_id}")
        booker = bookers[booker_id]
        if patch_property.name in self._booker_bool_fields:
            booker[patch_property.name] = self._parse_bool(patch_property.value)
        elif patch_property.name in self._booker_int_fields:
            try:
                booker[patch_property.name] = int(patch_property.value)
            except (TypeError, ValueError):
                raise ValueError(f"Invalid integer value for {patch_property.name}: {patch_property.value}")
            
        else:
            raise ValueError(f"Property {patch_property.name} not found")
        bookers[booker_id] = booker
        self.repo.update(tour_id, account_id, {"bookers": bookers})
        return bookers

    def _get_new_tour(self, item: PutTour, account_id: str) -> dict[str, Any]:
        return {
            "id": item.id or f"tour_{uuid4().hex}",
            "account_id": account_id,
            "tour_name": item.name,
            "images": item.images,
            "publish": item.publish,
            "services": item.services,
            "available": item.available,
            "tour_guides": item.tourGuides,
            "bookers": item.bookers,
            "content": item.content,
            "tags": item.tags,
            "event_location": item.location,
            "scores": item.scores,
            "created_at": datetime.now().isoformat(),
            "calendar_event_id": item.calendarEventId,
            "event_type": item.eventType,
            "user_group": item.group,
    }
    def _map_tour(self, item: dict[str, Any], get_presigned_url=True) -> dict[str, Any]:
        item["name"] = item.pop("tour_name", None)
        item["createdAt"] = item.pop("created_at", None)
        item["tourGuides"] = item.pop("tour_guides", None)
        item["location"] = item.pop("event_location", None)
        item["calendarEventId"] = item.pop("calendar_event_id", None)
        item["eventType"] = item.pop("event_type", None)
        item["group"] = item.pop("user_group", None)
        if get_presigned_url:
            item["images"] = [self.s3.get_s3_public_url(key=image) for image in item.get("images", [])]
        return item

    def _get_needed_updates(self, item: PutTour) -> dict[str, Any]:
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

    def _parse_bool(self, v) -> bool:
        if isinstance(v, bool):
            return v
        if isinstance(v, str):
            return v.strip().lower() in {"true", "1", "yes", "y", "t"}
        if isinstance(v, (int, float)):
            return bool(v)
        return False

    def _get_tour_from_calendar_event(self, put_calendar_event: PutCalendarEvent) -> PutTour:
        services = []
        if put_calendar_event.group == "male":
            services.append("Vittoria Masculino")
        if put_calendar_event.group == "female":
            services.append("Vittoria Femenino")
        services.append(put_calendar_event.category)
        title = put_calendar_event.title
        if put_calendar_event.category == "training":
            if put_calendar_event.start is not None:
                event_datetime = parse_timestamp_to_datetime(put_calendar_event.start)
                event_day = format_datetime_pretty_es(event_datetime)
                title = f"Entrenamiento {event_day}"
            else:
                title = "Entrenamiento"

        put_tour = PutTour(
            name=title,
            images=[],
            publish="draft",
            services=services,
            available={
                "startDate": put_calendar_event.start,
                "endDate": put_calendar_event.end
            },
            tourGuides=[],
            bookers={},
            content="",
            tags=[],
            location=put_calendar_event.location,
            scores={"home": 0, "away": 0},
            calendarEventId=put_calendar_event.id,
            eventType=put_calendar_event.category,
            group=put_calendar_event.group
        )
        return put_tour

    def _build_set_update(self, attrs: dict[str, Any]) -> tuple[str, dict[str, Any], dict[str, str]]:
        parts = []
        eav: dict[str, Any] = {}
        ean: dict[str, str] = {}
        for i, (attr, val) in enumerate(attrs.items(), start=1):
            nk = f"#n{i}"; vk = f":v{i}"
            ean[nk] = attr
            eav[vk] = val
            parts.append(f"{nk} = {vk}")
        return "SET " + ", ".join(parts), eav, ean
    