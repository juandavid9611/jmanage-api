from repositories.membership_repo_ddb import MembershipRepo
from typing import Dict, Any

class MembershipService:
    def __init__(self, repo: MembershipRepo):
        self.repo = repo

    def get_user_memberships(self, user_id: str) -> Dict[str, Any]:
        """Get active memberships for a user"""
        return self.repo.get_active_memberships(user_id)
