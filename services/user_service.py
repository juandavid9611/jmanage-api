import re
from time import time
from typing import Any
from datetime import datetime
from api.schemas.files import FileSpec
from repositories.cognito_idp_actions import CognitoIdentityProviderWrapper
from repositories.s3_adapter import S3Adapter
from repositories.user_repo_ddb import UserRepo
from api.schemas.users import CreateUser, PutUser, PutUserAvatar, PutUserMetrics, UserConfirmationStatus, UserStatus
from services.notification_orchestator import Notifications

from services.tour_service import TourService

class UserService:
    def __init__(
            self, repo: UserRepo, s3: S3Adapter, notifier: Notifications, cog_wrapper: CognitoIdentityProviderWrapper, tour_svc: TourService
            ):
        self.repo = repo
        self.notifier = notifier
        self.s3 = s3
        self.cog_wrapper = cog_wrapper
        self.tour_svc = tour_svc
        self._excluded_fields = ["id", "email"]
        self._custom_mapping_keys = {"name": "user_name", "status": "user_status", "group": "user_group"}

    def get(self, user_id: str) -> dict[str, Any] | None:
        item = self.repo.get(user_id)
        if item:
            cog_user = self.cog_wrapper.get_user(item["email"])
            item["confirmation_status"] = UserConfirmationStatus.CONFIRMED if cog_user["UserStatus"] == "CONFIRMED" else UserConfirmationStatus.PENDING
            return self._map_user(item)
        return None

    def list_users(self, *, group: str | None = None, include_disabled: bool = False) -> list[dict[str, Any]]:
        if group:
            items = self.repo.list_by_group(group)
        else:
            items = self.repo.list_all()
        if not include_disabled:
            items = [item for item in items if item.get("user_status") == UserStatus.ACTIVE]

        cog_users = self.cog_wrapper.list_users()
        return self._map_users(items, cog_users)

    def create(self, item: CreateUser) -> dict[str, Any]:
        #TODO Check if user exists in Cognito
        new_user = self._get_new_user(item)
        self.repo.put(new_user)
        self.notifier.send_user_welcome(email=item.email, name=item.name)
        return self._map_user(new_user)

    def update(self, user_id: str, item: PutUser) -> dict[str, Any] | None:
        cog_user = self.cog_wrapper.get_user(item.email)
        if not cog_user:
            raise ValueError(f"Cognito user with email {item.email} not found.")
        if cog_user.get("name") != item.name:
            self.cog_wrapper.update_user_field(item.email, "name", item.name)
        existing = self.repo.get(user_id)
        if not existing:
            return None
        updates = self._get_needed_updates(item)
        if not updates:
            return self._map_user(existing)
        self.repo.update(user_id, updates)
        new_item = self.repo.get(user_id)
        if not new_item:
            raise ValueError(f"User {user_id} not found after update.")
        return self._map_user(new_item)

    def delete(self, user_id: str) -> None:
        self.cog_wrapper.delete_user(user_id)
        self.repo.delete(user_id)

    def enable(self, user_id: str) -> None:
        user = self.repo.get(user_id)
        if not user:
            raise ValueError(f"User {user_id} not found.")
        self.cog_wrapper.enable_user(user["email"])
        self.repo.update(user_id, {"user_status": UserStatus.ACTIVE})

    def disable(self, user_id: str) -> None:
        user = self.repo.get(user_id)
        if not user:
            raise ValueError(f"User {user_id} not found.")
        self.cog_wrapper.disable_user(user["email"])
        self.repo.update(user_id, {"user_status": UserStatus.DISABLED})

    def update_user_avatar_url(self, user_id: str, item: PutUserAvatar) -> dict[str, Any] | None:
        existing = self.repo.get(user_id)
        if not existing:
            return None
        self.repo.update(user_id, {"avatar_url": item.avatar_url})
        new_item = self.repo.get(user_id)
        if not new_item:
            raise ValueError(f"User {user_id} not found after update.")
        return self._map_user(new_item, get_presigned_url=False)

    def get_user_metrics(self, user_id: str) -> dict[str, Any] | None:
        item = self.repo.get(user_id)
        if item:
            return item.get("user_metrics", {})
        return None

    def update_metrics(self, user_id: str, put_user_metrics: PutUserMetrics) -> dict[str, Any] | None:
        existing = self.repo.get(user_id)
        last_update = datetime.now().isoformat()
        put_user_metrics.last_update = last_update
        dic_metrics = put_user_metrics.dict()
        if not existing:
            return None
        updates = {"user_metrics": dic_metrics}
        self.repo.update(user_id, updates)
        new_item = self.repo.get(user_id)
        if not new_item:
            raise ValueError(f"User {user_id} not found after update.")
        return dic_metrics

    def generate_presigned_urls(self, user_id: str, files: list[FileSpec]) -> dict[str, dict[str, str]]:
        presigned_urls = {}
        user = self.get(user_id)

        if not user:
            raise ValueError(f"User {user_id} not found")
        file = files[0]
        if not isinstance(file, FileSpec):
            raise TypeError("Each file must be a FileSpec instance.")
        file_name = file.file_name
        file_content_type = file.content_type
        if not file_name or not file_content_type:
            raise ValueError("File 'name' and 'content_type' cannot be empty.")
        result = self.s3.presign_user_profile_photo_put(
            user_id=user_id, 
            filename=file_name, 
            content_type=file_content_type
        )
        presigned_urls['get_presigned_url'] = self.s3.presign_get_from_explicit_key(key=result["key"])
        presigned_urls['put_presigned_url'] = result["url"]
        presigned_urls['key'] = result["key"]
        return presigned_urls
    
    #TODO Optimize this to avoid loading all users
    def get_late_arrives(self, user_id: str) -> list[dict[str, Any]]:
        items = self.repo.list_all()
        #TODO Check why cog_users is needed
        cog_users = self.cog_wrapper.list_users()
        users_mapped = self._map_users(items, cog_users)
        late_arrives = []
        for user in users_mapped:
            late_arrive = {
                "id": user["id"],
                "avatarUrl": user.get("avatarUrl"), 
                "name": user.get("name"), 
                "description": user["user_metrics"].get("puntaje_asistencia_description"), 
                "rating": user["user_metrics"].get("puntaje_asistencia"), 
                "postedAt": user["user_metrics"].get("last_update"),
                "group": user.get("group"),
            }
            if user["id"] == user_id:
                late_arrives.insert(0, late_arrive)
            elif user["user_metrics"].get("puntaje_asistencia", 3) < 3:
                late_arrives.append(late_arrive)
        return late_arrives

    def send_christmas_greetings(self) -> None:
        users = self.list_users(include_disabled=False)
        for user in users:
            self.notifier.send_christmas_greeting(email=user["email"], name=user["name"])

    def get_assists_stats(self, user_id: str) -> list[dict[str, Any]]:
        user = self.get(user_id)
        if not user:
            raise ValueError(f"User {user_id} not found.")
        stats = {
            "match_assists": 0,
            "total_matches": 0,
            "match_late_arrives": 0,
            "training_assists": 0,
            "total_trainings": 0,
            "training_late_arrives": 0,
        }
        items = self.tour_svc.list_tours(group=user["group"], tour_type=None)
        for item in items:
            if item.get("eventType") == "match":
                stats["total_matches"] += 1
            elif item.get("eventType") == "training":
                stats["total_trainings"] += 1
            bookers = item.get("bookers", {})
            if user_id in bookers:
                if item.get("eventType") == "match" and bookers[user_id].get("approved", False):
                    stats["match_assists"] += 1
                    stats["match_late_arrives"] += 1 if bookers[user_id]["late"] else 0
                elif item.get("eventType") == "training" and bookers[user_id].get("approved", False):
                    stats["training_assists"] += 1
                    stats["training_late_arrives"] += 1 if bookers[user_id]["late"] else 0
        response = [
            {
                'id': 'Partidos',
                'coverUrl': 'assets/images/about/testimonials.webp',
                'title': 'Partidos', 
                'current': stats["match_assists"],
                'total': stats["total_matches"], 
                'late_arrives': stats["match_late_arrives"]
            },
            {'id': 'Entrenamientos', 
                'coverUrl': 'assets/images/about/vision.webp',
                'title': 'Entrenamientos', 
                'current': stats["training_assists"],
                'total': stats["total_trainings"], 
                'late_arrives': stats["training_late_arrives"]
            }
        ]
        return response

    def get_top_goals_and_assists(self, workspace_id: str) -> list[dict[str, Any]]:
        items = self.tour_svc.list_tours(group=workspace_id, tour_type=None)
        top_goals_and_assists = {}
        for item in items:
            bookers = item.get("bookers", {})
            for booker_id in bookers:
                booker = bookers[booker_id]
                if booker["goals"] > 0 or booker["assists"] > 0:
                    if booker_id not in top_goals_and_assists:
                        top_goals_and_assists[booker_id] = {
                            "id": booker_id,
                            "name": booker["name"],
                            "avatarUrl": booker.get("avatarUrl"),
                            "goals": 0,
                            "assists": 0,
                        }
                    top_goals_and_assists[booker_id]["goals"] += booker["goals"]
                    top_goals_and_assists[booker_id]["assists"] += booker["assists"]
        sorted_top = sorted(top_goals_and_assists.values(), key=lambda x: (x["goals"], x["assists"]), reverse=True)
        return sorted_top

    def _get_new_user(self, item: CreateUser) -> dict[str, Any]:
        created_time = int(time())

        return {
            "id": item.id,
            "user_name": item.name,
            "email": item.email,
            "user_status": UserStatus.ACTIVE,
            "created_time": created_time,
            #TODO Resolve user group
            "user_group": "male",
            "user_metrics": {
                "asistencia_entrenos": 0,
                "asistencia_partidos": 0,
                "puntualidad_pagos": 0,
                "llegadas_tarde": 0,
                "deuda_acumulada": 0,
                "total": 0,
                "puntaje_asistencia_description": "",
                "puntaje_asistencia": 3 ,
                "last_update": datetime.now().isoformat()
            },
            "shirt_number": "0",
        }
    
    def _map_users(self, db_users, cog_users):
        cog_users_map = {user["Username"]: user for user in cog_users}
        users = []
        for db_user in db_users:
            cog_user = cog_users_map[db_user["id"]]
            db_user["confirmation_status"] = UserConfirmationStatus.CONFIRMED if cog_user["UserStatus"] == "CONFIRMED" else UserConfirmationStatus.PENDING
            user = self._map_user(db_user)
            users.append(user)
        return users

    def _map_user(self, item, get_presigned_url=True) -> dict[str, Any]:
        item["name"] = item.pop("user_name", None)
        item["phoneNumber"] = item.pop("phone_number", None)
        item["avatarUrl"] = item.pop("avatar_url", None)
        if get_presigned_url and item.get("avatarUrl", None):
            item["avatarUrl"] = self.s3.presign_get_from_explicit_key(key=item["avatarUrl"])
        item["status"] = item.pop("user_status", None)
        item["group"] = item.pop("user_group", None)
        item["confirmationStatus"] = item.pop("confirmation_status", None)
        item["identityCardNumber"] = item.pop("identity_card_number", None)
        item["emergencyContactName"] = item.pop("emergency_contact_name", None)
        item["emergencyContactPhoneNumber"] = item.pop("emergency_contact_phone_number", None)
        item["emergencyContactRelationship"] = item.pop("emergency_contact_relationship", None)
        item["shirtNumber"] = item.pop("shirt_number", None)
        return item

    def _get_needed_updates(self, item: PutUser) -> dict[str, Any]:
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
