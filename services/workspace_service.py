import re
from services.user_service import UserService
from api.schemas.workspaces import PutWorkspace
from typing import Any, Dict, List, Optional, Tuple
from repositories.workspace_repo_ddb import WorkspaceRepo


class WorkspaceService:
    def __init__(self, repo: WorkspaceRepo, user_svc: UserService):
        self.repo = repo
        self.user_svc = user_svc
        self._excluded_fields = ["id"]
        self._custom_mapping_keys = {"name": "tour_name", "location": "event_location"}
        self._booker_bool_fields = {"approved", "late", "yellowCard", "redCard", "mvp"}
        self._booker_int_fields = {"goals", "assists"}

    def get(self, workspace_id: str) -> Optional[Dict[str, Any]]:
        item = self.repo.get(workspace_id)
        if item:
            return item
        return None

    def get_related(self, user) -> List[Dict[str, Any]]:
        items = self.repo.list_all()
        if user["custom:role"] == "admin":
            return [item for item in items]
        user_db = self.user_svc.get(user["sub"])
        if not user_db:
            raise ValueError(f"User {user['sub']} not found")
        related_items = []
        for item in items:
            if user_db.get("user_group", None):
                raise ValueError(f"User {user['sub']} has no user_group assigned") 
            if item["id"] == user_db["user_group"]:
                related_items.append(item)
        return related_items

    def list(self) -> List[Dict[str, Any]]:
        return [item for item in self.repo.list_all()]

    def create(self, item: PutWorkspace) -> Dict[str, Any]:
        new_workspace = self._get_new_workspace(item)
        self.repo.put(new_workspace)
        return new_workspace

    def update(self, workspace_id: str, item: PutWorkspace) -> Optional[Dict[str, Any]]:
        existing = self.repo.get(workspace_id)
        if not existing:
            return None
        updates = self._get_needed_updates(item)
        if not updates:
            return existing
        self.repo.update(workspace_id, updates)
        new_item = self.repo.get(workspace_id)
        if not new_item:
            raise ValueError(f"Workspace {workspace_id} not found after update.")
        return new_item

    def delete(self, tour_id: str) -> None:
        self.repo.delete(tour_id)

    def _get_new_workspace(self, item: PutWorkspace) -> Dict[str, Any]:
        return {
            "id": item.id,
            "name": item.name,
            "logo": item.logo,
            "plan": item.plan,
        }

    def _get_needed_updates(self, item: PutWorkspace) -> Dict[str, Any]:
        data = item.dict(exclude_unset=True, exclude_none=True)
        updates: Dict[str, Any] = {}
        for field, value in data.items():
            if field in self._excluded_fields:
                continue
            updates[self._map_attribute_key(field)] = value
        return updates

    def _map_attribute_key(self, key: str) -> str:
        if key in self._custom_mapping_keys:
            return self._custom_mapping_keys[key]
        return re.sub(r'(?<!^)(?=[A-Z])', '_', key).lower()
