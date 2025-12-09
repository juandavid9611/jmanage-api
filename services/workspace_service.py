import re
from uuid import uuid4
from services.user_service import UserService
from api.schemas.workspaces import PutWorkspace
from typing import Any
from repositories.workspace_repo_ddb import WorkspaceRepo


class WorkspaceService:
    def __init__(self, repo: WorkspaceRepo, user_svc: UserService):
        self.repo = repo
        self.user_svc = user_svc
        self._excluded_fields = ["id"]  # Prevent id from being updated

    def get(self, workspace_id: str, account_id: str) -> dict[str, Any] | None:
        item = self.repo.get(workspace_id, account_id)
        if item:
            return item
        return None

    def get_related(self, user, account_id: str) -> list[dict[str, Any]]:
        items = self.repo.list_all(account_id)
        if user["custom:role"] == "admin":
            return [item for item in items]
        user_db = self.user_svc.get(user["sub"], account_id)
        if not user_db:
            raise ValueError(f"User {user['sub']} not found")
        related_items = []
        for item in items:
            if user_db.get("group") is None:
                raise ValueError(f"User {user['sub']} has no group assigned") 
            if item["id"] == user_db["group"]:
                related_items.append(item)
        return related_items

    def list_workspaces(self, account_id: str) -> list[dict[str, Any]]:
        return [item for item in self.repo.list_all(account_id)]

    def create(self, item: PutWorkspace, account_id: str) -> dict[str, Any]:
        new_workspace = self._get_new_workspace(item, account_id)
        self.repo.put(new_workspace)
        return new_workspace

    def update(self, workspace_id: str, account_id: str, item: PutWorkspace) -> dict[str, Any] | None:
        existing = self.repo.get(workspace_id, account_id)
        if not existing:
            return None
        updates = self._get_needed_updates(item)
        if not updates:
            return existing
        self.repo.update(workspace_id, account_id, updates)
        new_item = self.repo.get(workspace_id, account_id)
        if not new_item:
            raise ValueError(f"Workspace {workspace_id} not found after update.")
        return new_item

    def delete(self, tour_id: str, account_id: str) -> None:
        self.repo.delete(tour_id, account_id)

    def _get_new_workspace(self, item, account_id: str) -> dict[str, Any]:
        return {
            "id": f"{uuid4().hex}",  # Auto-generate workspace ID
            "account_id": account_id,
            "name": item.name,
            "logo": item.logo,
            "plan": item.plan,
        }

    def _get_needed_updates(self, item: PutWorkspace) -> dict[str, Any]:
        data = item.dict(exclude_unset=True, exclude_none=True)
        updates: dict[str, Any] = {}
        for field, value in data.items():
            if field in self._excluded_fields:
                continue
            updates[self._map_attribute_key(field)] = value
        return updates

    def _map_attribute_key(self, key: str) -> str:
        """Convert camelCase to snake_case"""
        return re.sub(r'(?<!^)(?=[A-Z])', '_', key).lower()
