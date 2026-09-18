#!/usr/bin/env python3
"""
Migration: Fix malformed name/id_number fields on 3 Villa Jeronimo players

Villa Jeronimo (team ttm_9dc440c338f04564bf576ad35108f649, tournament
"Copa Samba 2026" / trn_37ac25fb8a2b4532a1e9551887319a5c) reported being
unable to delete certain players. Root cause was a CSS grid layout bug
(fixed separately in jmanage-web) triggered by one player's `name` field
containing a full registration string (name + cedula + birthdate + EPS +
phone) pasted in by mistake during data entry, which inflated the shared
grid column and pushed every other player's actions menu off-screen.

This script cleans up that record plus two others found with malformed
`id_number` formatting during the same investigation. It never touches
`team_id`/`tournament_id`/`position` — only the two free-text fields
below.

Usage:
    source .venv/bin/activate
    python migrations/fix_villa_jeronimo_malformed_player_fields.py            # Dry-run
    python migrations/fix_villa_jeronimo_malformed_player_fields.py --execute  # Apply
"""

import os
import sys
import argparse
from dotenv import load_dotenv

load_dotenv()
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from repositories.ddb_session import tournament_player_table  # noqa: E402

FIXES = {
    "tpl_700e43ebd1f240cab795265aaca0e93c": {
        "name": "William Miguel Rapalino Campo",
    },
    "tpl_ce20d1312ef74eee9a81409c9af14254": {
        "id_number": "1013100032",
    },
    "tpl_80b6740cca4740b998fc37d9105e8328": {
        "id_number": "1022945528",
    },
}


def run(execute: bool) -> None:
    table = tournament_player_table()

    for player_id, fields in FIXES.items():
        resp = table.get_item(Key={"id": player_id})
        item = resp.get("Item")
        if not item:
            print(f"  SKIP {player_id}: not found")
            continue
        for field, new_value in fields.items():
            print(f"  {player_id}.{field}:")
            print(f"    before: {item.get(field)!r}")
            print(f"    after:  {new_value!r}")

    if not execute:
        print("\n[DRY RUN] Run with --execute to apply.")
        return

    for player_id, fields in FIXES.items():
        resp = table.get_item(Key={"id": player_id})
        if not resp.get("Item"):
            continue
        update_expr = "SET " + ", ".join(f"#{k} = :{k}" for k in fields)
        table.update_item(
            Key={"id": player_id},
            UpdateExpression=update_expr,
            ExpressionAttributeNames={f"#{k}": k for k in fields},
            ExpressionAttributeValues={f":{k}": v for k, v in fields.items()},
        )
    print(f"\n✅ Updated {len(FIXES)} player record(s)")


def main() -> None:
    parser = argparse.ArgumentParser(description="Fix malformed Villa Jeronimo player fields")
    parser.add_argument("--execute", action="store_true", help="Apply the updates (default: dry run)")
    args = parser.parse_args()
    run(args.execute)


if __name__ == "__main__":
    main()
