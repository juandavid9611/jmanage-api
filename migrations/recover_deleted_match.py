#!/usr/bin/env python3
"""
Recover a match that was accidentally hard-deleted from the TournamentMatch
DynamoDB table, using point-in-time recovery (PITR).

The app deletes matches with a plain delete_item and keeps no soft-delete
or history (see repositories/tournament_match_repo_ddb.py), so the only way
back is a PITR restore of the table to a temp table, followed by copying the
recovered item back into the live table.

This script never touches the live table's other data and never deletes
anything from it — it only restores a temp copy, reads from it, and (with
--apply) writes the single recovered match back with put_item.

Usage:
    # 1. Restore a temp copy of the table to a point in time BEFORE the delete,
    #    and list candidate matches between the two given teams:
    python recover_deleted_match.py \\
        --restore-timestamp 2026-08-22T18:30:00 \\
        --home-team-id ttm_dcd3f4e5f50b41048e0b0c8410f0e8f4 \\
        --away-team-id ttm_7c32e88a31e64327bf7e11eee2b02093

    # 2. Once you've confirmed the match id from the listing, write it back
    #    to the live table:
    python recover_deleted_match.py \\
        --restore-timestamp 2026-08-22T18:30:00 \\
        --home-team-id ttm_dcd3f4e5f50b41048e0b0c8410f0e8f4 \\
        --away-team-id ttm_7c32e88a31e64327bf7e11eee2b02093 \\
        --match-id <id-from-listing> \\
        --apply

    # 3. Clean up the temp restored table once you're done:
    python recover_deleted_match.py --cleanup-only

Notes:
    - --restore-timestamp must be BEFORE the accidental deletion (naive
      local time, interpreted as UTC unless you pass a timezone offset).
      If unsure of the exact time, err earlier — the item's own fields
      (created_at/updated_at) won't be affected by picking an earlier point,
      as long as it's after the match was originally created.
    - Restoring a table takes several minutes; the script polls until the
      temp table is ACTIVE.
    - Requires AWS credentials with dynamodb:RestoreTableToPointInTime,
      DescribeTable, Scan, and (for --apply) PutItem on the live table.
    - Set AWS_PROFILE / AWS_REGION env vars as needed before running.
"""

import argparse
import os
import sys
import time
from datetime import datetime, timezone

import boto3
from boto3.dynamodb.conditions import Attr
from dotenv import load_dotenv

load_dotenv()

TABLE_ENV_VAR = "TOURNAMENT_MATCH_TABLE_NAME"
TEMP_TABLE_SUFFIX = "-recovery-tmp"


def get_source_table_name() -> str:
    table_name = os.getenv(TABLE_ENV_VAR)
    if not table_name:
        sys.exit(f"Missing {TABLE_ENV_VAR} env var — set it to the live TournamentMatch table name.")
    return table_name


def temp_table_name(source_table_name: str) -> str:
    return f"{source_table_name}{TEMP_TABLE_SUFFIX}"


def restore_temp_table(ddb_client, source_table_name: str, restore_timestamp: datetime, dest_table_name: str) -> None:
    try:
        ddb_client.describe_table(TableName=dest_table_name)
        print(f"Temp table {dest_table_name} already exists, skipping restore.")
        return
    except ddb_client.exceptions.ResourceNotFoundException:
        pass

    print(f"Restoring {source_table_name} to point-in-time {restore_timestamp.isoformat()} as {dest_table_name} ...")
    ddb_client.restore_table_to_point_in_time(
        SourceTableName=source_table_name,
        TargetTableName=dest_table_name,
        RestoreDateTime=restore_timestamp,
        BillingModeOverride="PAY_PER_REQUEST",
    )

    while True:
        desc = ddb_client.describe_table(TableName=dest_table_name)["Table"]
        status = desc["TableStatus"]
        print(f"  temp table status: {status}")
        if status == "ACTIVE":
            break
        time.sleep(15)


def find_candidate_matches(dynamodb_resource, dest_table_name: str, home_team_id: str, away_team_id: str) -> list:
    table = dynamodb_resource.Table(dest_table_name)
    filter_expr = (
        (Attr("home_team_id").eq(home_team_id) & Attr("away_team_id").eq(away_team_id))
        | (Attr("home_team_id").eq(away_team_id) & Attr("away_team_id").eq(home_team_id))
    )

    items = []
    scan_kwargs = {"FilterExpression": filter_expr}
    while True:
        resp = table.scan(**scan_kwargs)
        items.extend(resp.get("Items", []))
        start_key = resp.get("LastEvaluatedKey")
        if not start_key:
            break
        scan_kwargs["ExclusiveStartKey"] = start_key
    return items


def cleanup_temp_table(ddb_client, dest_table_name: str) -> None:
    try:
        ddb_client.describe_table(TableName=dest_table_name)
    except ddb_client.exceptions.ResourceNotFoundException:
        print(f"Temp table {dest_table_name} does not exist, nothing to clean up.")
        return
    confirm = input(f"Delete temp table {dest_table_name}? [y/N] ").strip().lower()
    if confirm == "y":
        ddb_client.delete_table(TableName=dest_table_name)
        print("Delete requested.")
    else:
        print("Left temp table in place.")


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--restore-timestamp", help="ISO8601 timestamp before the deletion, e.g. 2026-08-22T18:30:00")
    parser.add_argument("--home-team-id")
    parser.add_argument("--away-team-id")
    parser.add_argument("--match-id", help="Once known, the id of the match to write back to the live table")
    parser.add_argument("--apply", action="store_true", help="Actually put the recovered match back into the live table")
    parser.add_argument("--cleanup-only", action="store_true", help="Just delete the temp restored table and exit")
    return parser.parse_args()


def main():
    args = parse_args()

    session = boto3.Session()
    ddb_client = session.client("dynamodb")
    dynamodb_resource = session.resource("dynamodb")

    source_table_name = get_source_table_name()
    dest_table_name = temp_table_name(source_table_name)

    if args.cleanup_only:
        cleanup_temp_table(ddb_client, dest_table_name)
        return

    if not args.restore_timestamp or not args.home_team_id or not args.away_team_id:
        sys.exit("--restore-timestamp, --home-team-id, and --away-team-id are required (unless --cleanup-only).")

    restore_timestamp = datetime.fromisoformat(args.restore_timestamp)
    if restore_timestamp.tzinfo is None:
        restore_timestamp = restore_timestamp.replace(tzinfo=timezone.utc)

    restore_temp_table(ddb_client, source_table_name, restore_timestamp, dest_table_name)

    candidates = find_candidate_matches(dynamodb_resource, dest_table_name, args.home_team_id, args.away_team_id)
    if not candidates:
        print("No matches found between these two teams in the restored snapshot.")
        print("Try an earlier --restore-timestamp, or double check the team ids.")
        return

    print(f"\nFound {len(candidates)} candidate match(es):")
    for item in candidates:
        print(
            f"  id={item.get('id')} tournament_id={item.get('tournament_id')} "
            f"date={item.get('date')} matchweek={item.get('matchweek')} "
            f"status={item.get('status')} home={item.get('home_team_id')} away={item.get('away_team_id')}"
        )

    if not args.apply:
        print("\nDry run only. Re-run with --match-id <id> --apply to restore the item into the live table.")
        return

    if not args.match_id:
        sys.exit("--match-id is required together with --apply.")

    match = next((item for item in candidates if item.get("id") == args.match_id), None)
    if not match:
        sys.exit(f"Match id {args.match_id} not found among the candidates listed above.")

    live_table = dynamodb_resource.Table(source_table_name)
    existing = live_table.get_item(Key={"id": match["id"]}).get("Item")
    if existing:
        sys.exit(f"A match with id {match['id']} already exists in the live table — refusing to overwrite.")

    live_table.put_item(Item=match)
    print(f"Restored match {match['id']} into {source_table_name}.")


if __name__ == "__main__":
    main()
