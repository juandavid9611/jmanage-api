from repositories.membership_repo_ddb import MembershipRepo
from typing import Dict, Any, List

class MembershipService:
    def __init__(self, repo: MembershipRepo):
        self.repo = repo

    def get_user_memberships(self, user_id: str) -> List[Dict[str, Any]]:
        """Get active memberships for a user across all accounts"""
        return self.repo.get_active_memberships(user_id)
    
    def get_user_account_memberships(self, user_id: str, account_id: str) -> List[Dict[str, Any]]:
        """Get all memberships for a user in a specific account"""
        return self.repo.get_user_account_memberships(user_id, account_id)
    
    def list_account_memberships(self, account_id: str) -> List[Dict[str, Any]]:
        """List all memberships for an account"""
        return self.repo.list_by_account(account_id)
    
    def list_workspace_memberships(self, workspace_id: str) -> List[Dict[str, Any]]:
        """List all memberships for a workspace"""
        return self.repo.get_workspace_memberships(workspace_id)
    
    def create_membership(self, user_id: str, account_id: str, workspace_id: str, role: str = "user", status: str = "active") -> None:
        """Create a new membership - workspace_id is now REQUIRED"""
        self.repo.create(user_id, account_id, workspace_id, role, status)
    
    def delete_membership(self, user_id: str, account_id: str, workspace_id: str) -> None:
        """Delete a specific membership - now requires workspace_id"""
        self.repo.delete(user_id, account_id, workspace_id)
    
    def enable_membership(self, user_id: str, account_id: str, workspace_id: str) -> None:
        """Enable a membership - now requires workspace_id"""
        self.repo.update_status(user_id, account_id, workspace_id, "active")
    
    def disable_membership(self, user_id: str, account_id: str, workspace_id: str) -> None:
        """Disable a membership - now requires workspace_id"""
        self.repo.update_status(user_id, account_id, workspace_id, "disabled")
    
    def delete_all_user_memberships(self, user_id: str) -> None:
        """Delete all memberships for a user (used when deleting user)"""
        self.repo.delete_all_for_user(user_id)
    
    def get_user_workspaces(self, user_id: str, account_id: str) -> List[str]:
        """Get all workspace IDs user has access to in an account"""
        memberships = self.repo.get_user_account_memberships(user_id, account_id)
        return [m["workspace_id"] for m in memberships if m.get("workspace_id")]
    
    def get_user_role_in_workspace(self, user_id: str, account_id: str, workspace_id: str) -> str | None:
        """Get user's role in a specific workspace"""
        memberships = self.repo.get_user_account_memberships(user_id, account_id)
        for m in memberships:
            if m.get("workspace_id") == workspace_id:
                return m.get("role")
        return None

    def update_user_active_workspace(self, user_id: str, account_id: str, new_workspace_id: str) -> Dict[str, Any] | None:
        """Switch the user's membership to a different workspace.
        
        Finds the user's current active membership, deletes it, and creates
        a new membership for the target workspace_id, preserving the role.
        
        Returns the new membership info, or None if no active membership found.
        """
        memberships = self.repo.get_user_account_memberships(user_id, account_id)
        
        # Find current active membership
        current = None
        for m in memberships:
            if m.get("status") == "active":
                current = m
                break
        
        if not current:
            return None
        
        old_workspace_id = current.get("workspace_id")
        role = current.get("role", "user")
        
        # If already on the target workspace, no-op
        if old_workspace_id == new_workspace_id:
            return {
                "user_id": user_id,
                "account_id": account_id,
                "workspace_id": new_workspace_id,
                "role": role
            }
        
        # Delete old membership and create new one with the new workspace
        self.repo.delete(user_id, account_id, old_workspace_id)
        self.repo.create(user_id, account_id, new_workspace_id, role, "active")
        
        return {
            "user_id": user_id,
            "account_id": account_id,
            "workspace_id": new_workspace_id,
            "role": role
        }

