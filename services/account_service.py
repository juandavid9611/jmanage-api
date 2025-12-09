from uuid import uuid4
from datetime import datetime
from typing import Any
from repositories.account_repo_ddb import AccountRepo
from api.schemas.accounts import CreateAccount, UpdateAccount
from services.membership_service import MembershipService

class AccountService:
    def __init__(self, repo: AccountRepo, membership_svc: MembershipService):
        self.repo = repo
        self.membership_svc = membership_svc
    
    def get(self, account_id: str) -> dict[str, Any] | None:
        """Get account by ID"""
        return self.repo.get(account_id)
    
    def create(self, item: CreateAccount) -> dict[str, Any]:
        """Create new account"""
        now = datetime.now().isoformat()
        
        new_account = {
            "id": item.id,
            "name": item.name,
            "created_at": now,
            "updated_at": now,
            "settings": item.settings.dict() if item.settings else {
                "default_workspace": "main",
                "timezone": "America/Bogota",
                "language": "es",
                "currency": "COP"
            },
            "subscription": item.subscription.dict() if item.subscription else {
                "plan": "free",
                "status": "active",
                "trial_end": None
            },
            "branding": item.branding.dict() if item.branding else {}
        }
        
        self.repo.put(new_account)
        return new_account
    
    def update(self, account_id: str, item: UpdateAccount) -> dict[str, Any] | None:
        """Update account"""
        existing = self.repo.get(account_id)
        if not existing:
            return None
        
        updates = item.dict(exclude_unset=True, exclude_none=True)
        if not updates:
            return existing
        
        updates["updated_at"] = datetime.now().isoformat()
        self.repo.update(account_id, updates)
        
        return self.repo.get(account_id)
    
    def get_user_accounts(self, user_id: str) -> list[dict[str, Any]]:
        """Get all accounts a user belongs to via memberships"""
        memberships = self.membership_svc.get_user_memberships(user_id)
        
        accounts = []
        for membership in memberships:
            account_id = membership.get("account_id")
            if account_id:
                account = self.repo.get(account_id)
                if account:
                    # Add membership info to account
                    account["membership"] = {
                        "role": membership.get("role"),
                        "status": membership.get("status"),
                        "workspace_id": membership.get("workspace_id")
                    }
                    accounts.append(account)
        
        return accounts
