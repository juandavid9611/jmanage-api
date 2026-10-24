"""Authorisation helper shared by the finance flows.

The finance endpoints are authorised per workspace (`WorkspacePermissionChecker`, which has no
account-admin bypass). When a flow also touches a bank account that belongs to ANOTHER workspace
(an expense paid from it, a player's installment credited to it), the caller must be admin of that
account's workspace too, otherwise an admin of one category could move money in another one.
"""
from fastapi import HTTPException
from starlette.status import HTTP_403_FORBIDDEN


def require_workspace_admin(membership_service, user_id: str, account_id: str, workspace_id: str) -> None:
    role = membership_service.get_user_role_in_workspace(user_id, account_id, workspace_id)
    if role != "admin":
        raise HTTPException(
            status_code=HTTP_403_FORBIDDEN,
            detail="Workspace admin permission required for the bank account's workspace.",
        )


def require_account_workspace_access(
    membership_service, *, user_id: str, account_id: str, request_workspace_id: str, bank_account: dict
) -> None:
    """No-op when the bank account lives in the request's workspace, else admin of its workspace."""
    if bank_account.get("workspace_id") != request_workspace_id:
        require_workspace_admin(membership_service, user_id, account_id, bank_account["workspace_id"])
