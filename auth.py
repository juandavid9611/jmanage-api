import os

import requests
from dotenv import load_dotenv
from fastapi import Depends, HTTPException, Header, Query
from starlette.status import HTTP_403_FORBIDDEN, HTTP_401_UNAUTHORIZED
from typing import Optional

from JWTBearer import JWKS, JWTBearer, JWTAuthorizationCredentials

load_dotenv()

jwks = JWKS.parse_obj(
    requests.get(
        f"https://cognito-idp.us-west-2.amazonaws.com/{os.environ.get('USER_POOL_ID')}/.well-known/jwks.json",
        headers={"x-api-key": os.environ.get("USER_POOL_API_CLIENT_ID")},
    ).json()
)

auth = JWTBearer(jwks)

async def get_current_user(
    credentials: JWTAuthorizationCredentials = Depends(auth)
) -> dict:
    try:
        return credentials.claims
    except KeyError:
        raise HTTPException(status_code=HTTP_403_FORBIDDEN, detail="Username missing")

from di import get_membership_service
from services.membership_service import MembershipService

async def get_user_accounts(
    credentials: JWTAuthorizationCredentials = Depends(auth),
    membership_service: MembershipService = Depends(get_membership_service)
) -> dict:
    """Extract account membership from DynamoDB (Real-time lookup)
    
    Instead of relying on potentially stale JWT claims, we query the 
    Memberships table directly using the user's 'sub' from the token.
    
    Returns:
        dict with keys:
            - account_ids: list of account IDs user belongs to
            - accounts_roles: dict mapping account_id to role
            - user_id: user's ID from token
    """
    user_id = credentials.claims.get('sub')
    if not user_id:
        raise HTTPException(status_code=HTTP_403_FORBIDDEN, detail="No user ID in token")
    
    memberships = membership_service.get_user_memberships(user_id)
    
    if not memberships:
        raise HTTPException(
            status_code=HTTP_403_FORBIDDEN, 
            detail="No account memberships found"
        )
    
    # Transform list of memberships to expected format
    account_ids = [m["account_id"] for m in memberships]
    accounts_roles = {m["account_id"]: m["role"] for m in memberships}
    
    return {
        'account_ids': account_ids,
        'accounts_roles': accounts_roles,
        'user_id': user_id
    }

async def get_account_id(
    x_account_id: Optional[str] = Header(None, alias="X-Account-Id"),
    account_id: Optional[str] = Query(None),
    user_accounts: dict = Depends(get_user_accounts)
) -> str:
    """Extract and validate account_id from header or query param
    
    Priority:
        1. X-Account-Id header
        2. account_id query parameter
        3. First account in user's account_ids list
    
    Args:
        x_account_id: Account ID from X-Account-Id header
        account_id: Account ID from query parameter
        user_accounts: User's account membership info
        
    Returns:
        Validated account_id
        
    Raises:
        HTTPException: If user doesn't have access to requested account
    """
    # Prefer header, fallback to query param, then to first account
    requested_account = x_account_id or account_id or user_accounts['account_ids'][0]
    
    # Validate user has access to this account
    if requested_account not in user_accounts['account_ids']:
        raise HTTPException(
            status_code=HTTP_403_FORBIDDEN,
            detail=f"User does not have access to account {requested_account}"
        )
    
    return requested_account

def get_account_role(
    account_id: str = Depends(get_account_id),
    user_accounts: dict = Depends(get_user_accounts)
) -> str:
    """Get user's role for the current account
    
    Args:
        account_id: Current account ID
        user_accounts: User's account membership info
        
    Returns:
        User's role for the account (defaults to 'user' if not found)
    """
    return user_accounts['accounts_roles'].get(account_id, 'user')

async def get_workspace_id(
    workspace_id: Optional[str] = Query(None)
) -> Optional[str]:
    """Extract workspace_id from query parameter
    
    Args:
        workspace_id: Workspace ID from query parameter
        
    Returns:
        Workspace ID if provided, None otherwise
    """
    return workspace_id

async def get_workspace_role(
    workspace_id: Optional[str] = Depends(get_workspace_id),
    user: dict = Depends(get_current_user),
    account_id: str = Depends(get_account_id),
    membership_service: MembershipService = Depends(get_membership_service)
) -> Optional[str]:
    """Get user's role in the specified workspace
    
    Args:
        workspace_id: Workspace ID from query parameter
        user: Current authenticated user
        account_id: Current account ID
        membership_service: Membership service for role lookup
        
    Returns:
        User's role in the workspace, or None if workspace_id not provided
        
    Raises:
        HTTPException: If workspace_id provided but user has no access
    """
    if not workspace_id:
        return None
    
    role = membership_service.get_user_role_in_workspace(
        user["sub"], 
        account_id, 
        workspace_id
    )
    
    if not role:
        raise HTTPException(
            status_code=HTTP_403_FORBIDDEN,
            detail=f"User does not have access to workspace {workspace_id}"
        )
    
    return role

class PermissionChecker:
    """Check if user has required permissions for the current account"""
    
    def __init__(self, required_permissions: list[str]) -> None:
        self.required_permissions = required_permissions

    def __call__(
        self, 
        account_role: str = Depends(get_account_role)
    ) -> bool:
        """Validate user has required role for the current account
        
        Args:
            account_role: User's role for current account
            
        Returns:
            True if user has required permissions
            
        Raises:
            HTTPException: If user doesn't have required permissions
        """
        if account_role not in self.required_permissions:
            raise HTTPException(
                status_code=HTTP_401_UNAUTHORIZED,
                detail='User does not have the required permissions for this account.'
            )
        return True

class WorkspacePermissionChecker:
    """Check if user has required permissions for a specific workspace"""
    
    def __init__(self, required_permissions: list[str]) -> None:
        """
        Initialize workspace permission checker
        
        Args:
            required_permissions: List of roles allowed (e.g., ['admin'] or ['admin', 'user'])
        """
        self.required_permissions = required_permissions

    def __call__(
        self,
        workspace_role: Optional[str] = Depends(get_workspace_role),
        account_role: str = Depends(get_account_role)
    ) -> bool:
        """Validate user has required role for the workspace
        
        Account admins automatically bypass workspace-level checks.
        
        Args:
            workspace_role: User's role in the workspace (None if no workspace_id provided)
            account_role: User's role for the account
            
        Returns:
            True if user has required permissions
            
        Raises:
            HTTPException: If user doesn't have required permissions
        """
        # Account admins bypass workspace checks
        # if account_role == 'admin':
        #     return True
        
        # If no workspace_role, user doesn't have access
        if not workspace_role:
            raise HTTPException(
                status_code=HTTP_403_FORBIDDEN,
                detail='User does not have access to this workspace.'
            )
        
        # Check workspace role
        if workspace_role not in self.required_permissions:
            raise HTTPException(
                status_code=HTTP_403_FORBIDDEN,
                detail=f'Workspace {self.required_permissions[0]} permission required.'
            )
        
        return True