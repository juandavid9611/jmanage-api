#!/usr/bin/env python3
"""
Check for orphaned match events: tournament_match_event rows whose match_id
no longer points to any row in the tournament_match table.

This happens because delete_match only deletes the match row itself and
never cascades to its events (see services/tournament_match_service.py
delete_match), so if a deleted match already had recorded goals/cards/
substitutions, those event rows are left behind pointing at a match_id
that no longer exists.

This script is read-only — it never writes or deletes anything.

Usage:
    # Full scan: report every orphaned match_id found across the whole table
    python migrations/check_orphan_match_events.py

    # Narrow to events involving specific teams (e.g. the two teams from a
    # match you know was accidentally deleted):
    python migrations/check_orphan_match_events.py \\
        --team-id ttm_dcd3f4e5f50b41048e0b0c8410f0e8f4 \\
        --team-id ttm_7c32e88a31e64327bf7e11eee2b02093
"""

import argparse
import os
import sys
from collections import defaultdict

import boto3
from boto3.dynamodb.conditions import Attr
from dotenv import load_dotenv

load_dotenv()

MATCH_TABLE_ENV_VAR = "TOURNAMENT_MATCH_TABLE_NAME"
EVENT_TABLE_ENV_VAR = "TOURNAMENT_MATCH_EVENT_TABLE_NAME"


def get_table(dynamodb, env_var: str):
    table_name = os.getenv(env_var)
    if not table_name:
        sys.exit(f"Missing {env_var} env var.")
    return dynamodb.Table(table_name)


def scan_all(table, filter_expr=None) -> list:
    items = []
    kwargs = {"FilterExpression": filter_expr} if filter_expr is not None else {}
    while True:
        resp = table.scan(**kwargs)
        items.extend(resp.get("Items", []))
        start_key = resp.get("LastEvaluatedKey")
        if not start_key:
            break
        kwargs["ExclusiveStartKey"] = start_key
    return items


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--team-id",
        action="append",
        default=None,
        help="Only report orphaned events for these team id(s). Repeatable. Default: check all events.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    dynamodb = boto3.resource("dynamodb")

    match_table = get_table(dynamodb, MATCH_TABLE_ENV_VAR)
    event_table = get_table(dynamodb, EVENT_TABLE_ENV_VAR)

    print("Scanning live matches...")
    live_match_ids = {item["id"] for item in scan_all(match_table)}
    print(f"  {len(live_match_ids)} live matches found.")

    print("Scanning match events...")
    event_filter = None
    if args.team_id:
        event_filter = Attr("team_id").is_in(args.team_id)
    events = scan_all(event_table, event_filter)
    print(f"  {len(events)} events found{' (filtered by team_id)' if args.team_id else ''}.")

    events_by_match = defaultdict(list)
    for event in events:
        events_by_match[event.get("match_id")].append(event)

    orphaned_match_ids = [mid for mid in events_by_match if mid not in live_match_ids]

    if not orphaned_match_ids:
        print("\nNo orphaned events found.")
        return

    print(f"\nFound {len(orphaned_match_ids)} orphaned match_id(s) with events pointing at a deleted match:\n")
    for match_id in orphaned_match_ids:
        evts = events_by_match[match_id]
        print(f"match_id={match_id}  ({len(evts)} event(s))")
        for e in sorted(evts, key=lambda e: e.get("minute", 0)):
            print(
                f"    id={e.get('id')} type={e.get('type')} minute={e.get('minute')} "
                f"team_id={e.get('team_id')} player_id={e.get('player_id')} "
                f"assist_player_id={e.get('assist_player_id')}"
            )
        print()


if __name__ == "__main__":
    main()
