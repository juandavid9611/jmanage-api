#!/usr/bin/env python3
"""
Migration: Backfill missing user_status on existing User records

Team-owner accounts created via the tournament invitation-accept flow
(services/tournament_invitation_service.py) were written with only
{id, email, name} — no user_status. Every other place that lists users
filters by user_status == "active" unless include_disabled is explicitly
passed, so these otherwise-legitimate, confirmed users were silently
excluded from things like the "Crear nuevo cobro" recipient picker
(GET /users?workspace_id=X&include_disabled=false, the default).

The invitation-accept flow now sets user_status: "active" for new users —
this backfills rows created before that fix. Sets user_status="active" for
any User item missing the attribute entirely; does not touch rows that
already have a user_status (active or disabled).

Usage:
    source .venv/bin/activate
    python migrations/backfill_missing_user_status.py           # Dry-run
    python migrations/backfill_missing_user_status.py --execute # Apply
"""

import os
import sys
import argparse
from dotenv import load_dotenv

load_dotenv()
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from boto3.dynamodb.conditions import Attr

from repositories.ddb_session import user_table  # noqa: E402


def _scan_all(table, **kwargs) -> list[dict]:
    items: list[dict] = []
    start_key = None
    while True:
        if start_key:
            kwargs["ExclusiveStartKey"] = start_key
        resp = table.scan(**kwargs)
        items.extend(resp.get("Items", []))
        start_key = resp.get("LastEvaluatedKey")
        if not start_key:
            break
    return items


def run(execute: bool) -> None:
    table = user_table()

    missing = _scan_all(table, FilterExpression=Attr("user_status").not_exists())
    print(f"Found {len(missing)} user record(s) missing user_status")

    for item in missing:
        print(f"  {item['id']} -> user_status = 'active'")

    if not execute:
        print("\n[DRY RUN] Run with --execute to apply.")
        return

    for item in missing:
        table.update_item(
            Key={"id": item["id"]},
            UpdateExpression="SET user_status = :v",
            ExpressionAttributeValues={":v": "active"},
        )
    print(f"✅ Updated {len(missing)} user record(s)")


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill missing user_status to 'active'")
    parser.add_argument("--execute", action="store_true", help="Apply the updates (default: dry run)")
    args = parser.parse_args()
    run(args.execute)


if __name__ == "__main__":
    main()
