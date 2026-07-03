#!/usr/bin/env python3
"""
Script: Set Account Type to "tournament"

Flags an account as a tournament-only account (settings.account_type = "tournament"),
which hides the club-management nav tabs (Calendario, Partidos, Analitica, Tienda,
Entrenamientos, Votaciones, Productos, Ordenes) on the frontend. Uses a nested
UpdateExpression so other settings fields (default_workspace, timezone, ...) are
left untouched.

Usage:
    python migrations/set_account_type_tournament.py --account-id ACCOUNT_ID
    Add --execute to apply changes.
"""

import os
import sys
import argparse
from dotenv import load_dotenv

load_dotenv()
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from repositories.ddb_session import account_table  # noqa: E402


def run(account_id: str, execute: bool) -> None:
    table = account_table()
    account = table.get_item(Key={"id": account_id}).get("Item")
    if not account:
        print(f"❌ Account {account_id} not found")
        return

    print(f"Account: {account.get('name')} ({account_id})")
    print(f"Current settings: {account.get('settings')}")

    if account.get("settings", {}).get("account_type") == "tournament":
        print("ℹ️  Already set to 'tournament'. Nothing to do.")
        return

    if not execute:
        print("\n[DRY RUN] Would SET settings.account_type = 'tournament'")
        print("Run with --execute to apply.")
        return

    table.update_item(
        Key={"id": account_id},
        UpdateExpression="SET settings.account_type = :v",
        ExpressionAttributeValues={":v": "tournament"},
        ReturnValues="ALL_NEW",
    )
    updated = table.get_item(Key={"id": account_id}).get("Item")
    print(f"✅ Updated settings: {updated.get('settings')}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Set an account's settings.account_type to 'tournament'")
    parser.add_argument("--account-id", required=True, help="Account ID to flag as tournament-only")
    parser.add_argument("--execute", action="store_true", help="Execute the change (default: dry run)")
    args = parser.parse_args()
    run(args.account_id, args.execute)


if __name__ == "__main__":
    main()
