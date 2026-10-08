import logging
import re
from uuid import uuid4
from services.membership_service import MembershipService
from api.schemas.workspaces import PutWorkspace
from typing import Any
from repositories.workspace_repo_ddb import WorkspaceRepo

logger = logging.getLogger(__name__)

CREATOR_ROLE = "admin"


class WorkspaceNameConflict(Exception):
    """A workspace with the same (case-insensitive, trimmed) name already exists in the account."""


class WorkspaceCreationError(Exception):
    """Workspace creation failed and was rolled back."""


def normalize_workspace_name(name: str | None) -> str:
    """Trim and collapse nothing else; raises ValueError when empty or longer than 80 chars."""
    cleaned = (name or "").strip()
    if not cleaned:
        raise ValueError("Workspace name is required")
    if len(cleaned) > 80:
        raise ValueError("Workspace name must be at most 80 characters")
    return cleaned


def name_key(name: str | None) -> str:
    """Comparison key for duplicate detection (trimmed, case-insensitive)."""
    return (name or "").strip().casefold()


class WorkspaceService:
    def __init__(self, repo: WorkspaceRepo, membership_svc: MembershipService):
        self.repo = repo
        self.membership_svc = membership_svc
        self._excluded_fields = ["id"]  # Prevent id from being updated

    def get(self, workspace_id: str, account_id: str) -> dict[str, Any] | None:
        item = self.repo.get(workspace_id, account_id)
        if item:
            return item
        return None

    def get_related(self, user, account_id: str) -> list[dict[str, Any]]:
        """Get all workspaces user has access to via memberships"""
        user_id = user["sub"]
        memberships = self.membership_svc.get_user_account_memberships(user_id, account_id)
        
        if not memberships:
            return []
        
        # Create a mapping of workspace_id to role
        workspace_roles = {m["workspace_id"]: m.get("role", "user") for m in memberships if m.get("workspace_id")}
        
        all_workspaces = self.repo.list_all(account_id)
        
        # Add role to each workspace
        related_workspaces = []
        for ws in all_workspaces:
            if ws["id"] in workspace_roles:
                workspace_with_role = {**ws, "role": workspace_roles[ws["id"]]}
                related_workspaces.append(workspace_with_role)
        
        return related_workspaces

    def list_workspaces(self, account_id: str) -> list[dict[str, Any]]:
        return [item for item in self.repo.list_all(account_id)]

    def create(self, item: PutWorkspace, account_id: str) -> dict[str, Any]:
        new_workspace = self._get_new_workspace(item, account_id)
        self.repo.put(new_workspace)
        return new_workspace

    def create_with_admin(self, name: str, logo: str | None, account_id: str, creator_user_id: str) -> dict[str, Any]:
        """Create a workspace and an active admin membership for the creator.

        Returns the workspace in the same shape as GET /workspaces (item + per-user `role`).
        Raises ValueError (invalid name), WorkspaceNameConflict (duplicate name in the account)
        or WorkspaceCreationError (membership write failed; the workspace is deleted again).
        """
        clean_name = normalize_workspace_name(name)
        key = name_key(clean_name)
        if any(name_key(w.get("name")) == key for w in self.repo.list_all(account_id)):
            raise WorkspaceNameConflict(f"A workspace named '{clean_name}' already exists")

        workspace = {
            "id": uuid4().hex,
            "account_id": account_id,
            "name": clean_name,
            "logo": logo,
            "plan": None,
        }
        self.repo.put(workspace)
        try:
            self.membership_svc.create_membership(
                creator_user_id, account_id, workspace["id"], role=CREATOR_ROLE, status="active"
            )
        except Exception as exc:
            logger.exception("Membership creation failed for workspace %s; rolling back", workspace["id"])
            try:
                self.repo.delete(workspace["id"], account_id)
            except Exception:
                logger.exception("Rollback failed: orphan workspace %s in account %s", workspace["id"], account_id)
            raise WorkspaceCreationError("Could not create workspace") from exc
        return {**workspace, "role": CREATOR_ROLE}

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
