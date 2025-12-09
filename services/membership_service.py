from repositories.membership_repo_ddb import MembershipRepo
from typing import Dict, Any, List

class MembershipService:
    def __init__(self, repo: MembershipRepo):
        self.repo = repo

    def get_user_memberships(self, user_id: str) -> List[Dict[str, Any]]:
        """Get active memberships for a user"""
        return self.repo.get_active_memberships(user_id)
    
    def list_account_memberships(self, account_id: str) -> List[Dict[str, Any]]:
        """List all memberships for an account"""
        return self.repo.list_by_account(account_id)
    
    def create_membership(self, user_id: str, account_id: str, role: str = "user", status: str = "active", workspace_id: str | None = None) -> None:
        """Create a new membership for a user in an account"""
        self.repo.create(user_id, account_id, role, status, workspace_id)
    
    def delete_membership(self, user_id: str, account_id: str) -> None:
        """Delete a specific membership"""
        self.repo.delete(user_id, account_id)
    
    def enable_membership(self, user_id: str, account_id: str) -> None:
        """Enable a membership (set status to active)"""
        self.repo.update_status(user_id, account_id, "active")
    
    def disable_membership(self, user_id: str, account_id: str) -> None:
        """Disable a membership (set status to disabled)"""
        self.repo.update_status(user_id, account_id, "disabled")
    
    def delete_all_user_memberships(self, user_id: str) -> None:
        """Delete all memberships for a user (used when deleting user)"""
        self.repo.delete_all_for_user(user_id)
    
    def update_workspace(self, user_id: str, account_id: str, workspace_id: str | None) -> None:
        """Assign user to a workspace in an account"""
        self.repo.update_workspace(user_id, account_id, workspace_id)
    
    def get_user_workspace(self, user_id: str, account_id: str) -> str | None:
        """Get user's workspace assignment for an account"""
        memberships = self.repo.get_active_memberships(user_id)
        for membership in memberships:
            if membership.get("account_id") == account_id:
                return membership.get("workspace_id")
        return None
