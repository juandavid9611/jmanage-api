#!/usr/bin/env python3
"""
Script: Enable / disable the public "club world" for an account

Sets settings.public_world_enabled on an account. When true (and the account is a
club), the unauthenticated GET /public/clubs/{account_id}/overview serves sanitized
club aggregates (no person-level data). When false or absent it answers 404.
Uses a nested UpdateExpression so other settings (default_workspace, timezone, ...)
are left untouched. The API caches the overview in-process for 60s, so a change can
take up to a minute to show.

Usage (dry run by default):
    python migrations/set_public_world_enabled.py --account-id vittoriacd --enabled true
    python migrations/set_public_world_enabled.py --account-id vittoriacd --enabled true --confirm
    python migrations/set_public_world_enabled.py --account-id vittoriacd --enabled false --confirm
"""

import os
import sys
import argparse
from dotenv import load_dotenv

load_dotenv()
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def _bool(value: str) -> bool:
    if value.strip().lower() in ("true", "false"):
        return value.strip().lower() == "true"
    raise argparse.ArgumentTypeError("--enabled must be 'true' or 'false'")


def run(account_id: str, enabled: bool, confirm: bool) -> None:
    from repositories.ddb_session import account_table

    table = account_table()
    account = table.get_item(Key={"id": account_id}).get("Item")
    if not account:
        print(f"Account {account_id} not found")
        return

    settings = account.get("settings") or {}
    print(f"Account: {account.get('name')} ({account_id})")
    print(f"account_type: {settings.get('account_type', 'club (default)')}")
    print(f"Current public_world_enabled: {settings.get('public_world_enabled')}")

    if settings.get("public_world_enabled") is enabled:
        print("Already set. Nothing to do.")
        return
    if enabled and settings.get("account_type", "club") != "club":
        print("Warning: not a club account; the endpoint will still answer 404.")

    if not confirm:
        print(f"\n[DRY RUN] Would SET settings.public_world_enabled = {enabled}")
        print("Run with --confirm to apply.")
        return

    table.update_item(
        Key={"id": account_id},
        UpdateExpression="SET settings.public_world_enabled = :v",
        ExpressionAttributeValues={":v": enabled},
        ReturnValues="ALL_NEW",
    )
    updated = table.get_item(Key={"id": account_id}).get("Item")
    print(f"Updated settings: {updated.get('settings')}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Set an account's settings.public_world_enabled")
    parser.add_argument("--account-id", required=True)
    parser.add_argument("--enabled", required=True, type=_bool, help="true or false")
    parser.add_argument("--confirm", action="store_true", help="Apply the change (default: dry run)")
    args = parser.parse_args()
    run(args.account_id, args.enabled, args.confirm)


if __name__ == "__main__":
    main()
