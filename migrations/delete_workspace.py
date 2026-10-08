#!/usr/bin/env python3
"""
Script: Delete a workspace (category) safely, outside the API.

Reuses WorkspaceService.check_deletable, so it refuses in the same cases as
DELETE /workspaces/{id}: default_workspace, has_members, has_events. There is no
caller here, so ANY membership counts as a member (remove them first).

The operator must set the table names explicitly on the command line, e.g.:
    WORKSPACE_TABLE_NAME=... MEMBERSHIPS_TABLE_NAME=... CALENDAR_TABLE_NAME=... \
    TOUR_TABLE_NAME=... ACCOUNT_TABLE_NAME=... AWS_DEFAULT_REGION=... \
    python migrations/delete_workspace.py --account-id ACC --workspace-id WS [--confirm]

Dry-run by default; --confirm deletes. Prints only ids, counts and the reason.
"""
import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

REQUIRED_ENV = ("WORKSPACE_TABLE_NAME", "MEMBERSHIPS_TABLE_NAME", "CALENDAR_TABLE_NAME",
                "TOUR_TABLE_NAME", "ACCOUNT_TABLE_NAME")


def build_service():
    # Imported lazily so the env check happens before any boto3 table object is created
    from repositories.account_repo_ddb import AccountRepo
    from repositories.calendar_repo_ddb import CalendarRepo
    from repositories.membership_repo_ddb import MembershipRepo
    from repositories.tour_repo_ddb import TourRepo
    from repositories.workspace_repo_ddb import WorkspaceRepo
    from services.account_service import AccountService
    from services.membership_service import MembershipService
    from services.workspace_service import WorkspaceService

    membership_svc = MembershipService(MembershipRepo())
    return WorkspaceService(
        WorkspaceRepo(),
        membership_svc,
        account_svc=AccountService(AccountRepo(), membership_svc),
        calendar_repo=CalendarRepo(),
        tour_repo=TourRepo(),
    )


def run(svc, account_id: str, workspace_id: str, confirm: bool) -> int:
    from services.workspace_service import WorkspaceDeleteBlocked, WorkspaceDeleteError, WorkspaceNotFound

    try:
        svc.check_deletable(workspace_id, account_id, None)
    except WorkspaceNotFound:
        print(f"REFUSED account={account_id} workspace={workspace_id} reason=not_found")
        return 1
    except WorkspaceDeleteBlocked as e:
        print(f"REFUSED account={account_id} workspace={workspace_id} reason={e.code} ({e.message})")
        return 1
    if not confirm:
        print(f"DRY-RUN ok to delete account={account_id} workspace={workspace_id}; re-run with --confirm")
        return 0
    try:
        svc.delete_safely(workspace_id, account_id, None)
    except (WorkspaceNotFound, WorkspaceDeleteError) as e:
        print(f"FAILED account={account_id} workspace={workspace_id} reason={type(e).__name__}")
        return 2
    print(f"DELETED account={account_id} workspace={workspace_id}")
    return 0


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--account-id", required=True)
    ap.add_argument("--workspace-id", required=True)
    ap.add_argument("--confirm", action="store_true", help="actually delete (default is dry-run)")
    args = ap.parse_args(argv)
    missing = [k for k in REQUIRED_ENV if not os.getenv(k)]
    if missing:
        print(f"Missing env vars: {', '.join(missing)}")
        return 2
    return run(build_service(), args.account_id, args.workspace_id, args.confirm)


if __name__ == "__main__":
    sys.exit(main())
