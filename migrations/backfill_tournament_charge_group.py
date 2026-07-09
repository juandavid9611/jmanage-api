#!/usr/bin/env python3
"""
Migration: Backfill user_group on existing tournament-generated payment requests

"Generar Cobros" (POST /payment_requests/tournament-match-charges) used to write
user_group=tournamentId. Every other payment-request code path treats "group" as
workspace_id, so those charges never matched a real workspace filter and were
invisible in the Pagos / Pagos Totales pages. The endpoint now writes
user_group=<account's settings.default_workspace> instead — this script backfills
rows created before that fix.

Scans the PaymentRequest table for category="tournament_fine" items whose
user_group doesn't already match their account's default_workspace, and updates
them in place. Idempotent — re-running after a partial/failed run only touches
rows still out of sync.

Usage:
    source .venv/bin/activate
    python migrations/backfill_tournament_charge_group.py                  # Dry-run
    python migrations/backfill_tournament_charge_group.py --execute        # Apply
    python migrations/backfill_tournament_charge_group.py --account-id X   # Scope to one account
"""

import os
import sys
import argparse
from dotenv import load_dotenv

load_dotenv()
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from boto3.dynamodb.conditions import Attr

from repositories.ddb_session import account_table, payment_request_table  # noqa: E402


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


def run(account_id: str | None, execute: bool) -> None:
    pr_table = payment_request_table()
    acc_table = account_table()

    filter_expr = Attr("category").eq("tournament_fine")
    if account_id:
        filter_expr = filter_expr & Attr("account_id").eq(account_id)

    charges = _scan_all(pr_table, FilterExpression=filter_expr)
    print(f"Found {len(charges)} tournament_fine payment request(s)"
          + (f" for account {account_id}" if account_id else " across all accounts"))

    default_workspace_cache: dict[str, str | None] = {}
    to_update: list[tuple[str, str, str]] = []  # (payment_request_id, old_group, new_group)
    skipped_no_workspace: list[str] = []

    for item in charges:
        acc_id = item.get("account_id")
        if acc_id not in default_workspace_cache:
            account = acc_table.get_item(Key={"id": acc_id}).get("Item") or {}
            default_workspace_cache[acc_id] = (account.get("settings") or {}).get("default_workspace")
        default_workspace = default_workspace_cache[acc_id]

        if not default_workspace:
            skipped_no_workspace.append(item["id"])
            continue

        current_group = item.get("user_group")
        if current_group != default_workspace:
            to_update.append((item["id"], current_group, default_workspace))

    print(f"{len(to_update)} item(s) need a group update; "
          f"{len(charges) - len(to_update) - len(skipped_no_workspace)} already correct; "
          f"{len(skipped_no_workspace)} skipped (account has no default_workspace)")

    for pr_id, old_group, new_group in to_update:
        print(f"  {pr_id}: user_group {old_group!r} -> {new_group!r}")

    if skipped_no_workspace:
        print(f"⚠️  Skipped (no default_workspace configured): {skipped_no_workspace}")

    if not execute:
        print("\n[DRY RUN] Run with --execute to apply.")
        return

    for pr_id, _old_group, new_group in to_update:
        pr_table.update_item(
            Key={"id": pr_id},
            UpdateExpression="SET user_group = :v",
            ExpressionAttributeValues={":v": new_group},
        )
    print(f"✅ Updated {len(to_update)} payment request(s)")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Backfill user_group on tournament-generated payment requests to the account's default_workspace"
    )
    parser.add_argument("--account-id", help="Limit to a single account (default: all accounts)")
    parser.add_argument("--execute", action="store_true", help="Apply the updates (default: dry run)")
    args = parser.parse_args()
    run(args.account_id, args.execute)


if __name__ == "__main__":
    main()
