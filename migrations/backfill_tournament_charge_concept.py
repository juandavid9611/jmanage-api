#!/usr/bin/env python3
"""
Migration: Backfill richer concept/description on existing tournament charges

Generar Cobros used to write concept="Tarjeta Amarilla - <tournament name>" for
every card charge, with no player or match info — every row for a tournament
looked identical in Pagos Totales. The endpoint now includes the player's name
and match info; this script re-derives the same text for rows created before
that fix, using each row's `reference` (the card event id) to look up the
event -> match -> teams/player.

Usage:
    source .venv/bin/activate
    python migrations/backfill_tournament_charge_concept.py                # Dry-run
    python migrations/backfill_tournament_charge_concept.py --execute      # Apply
    python migrations/backfill_tournament_charge_concept.py --account-id X # Scope to one account
"""

import os
import sys
import argparse
from dotenv import load_dotenv

load_dotenv()
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from boto3.dynamodb.conditions import Attr

from repositories.ddb_session import payment_request_table  # noqa: E402
from repositories.tournament_repo_ddb import TournamentRepo  # noqa: E402
from repositories.tournament_team_repo_ddb import TournamentTeamRepo  # noqa: E402
from repositories.tournament_match_repo_ddb import TournamentMatchRepo  # noqa: E402
from repositories.tournament_match_event_repo_ddb import TournamentMatchEventRepo  # noqa: E402
from repositories.tournament_player_repo_ddb import TournamentPlayerRepo  # noqa: E402

_CARD_RED_TYPES = {"red_card", "second_yellow"}


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


def _derive_concept_description(item, event_repo, match_repo, team_repo, player_repo, tournament_repo):
    reference = item.get("reference")
    if not reference:
        return None, None, "no reference (card event id) on this row"

    event = event_repo.get(reference)
    if not event:
        return None, None, f"card event {reference} not found"

    match = match_repo.get(event.get("match_id"))
    if not match:
        return None, None, f"match {event.get('match_id')} not found"

    tournament = tournament_repo.get(match.get("tournament_id")) if match.get("tournament_id") else None
    tournament_name = (tournament or {}).get("name", "Torneo")

    team = team_repo.get(event.get("team_id")) if event.get("team_id") else None
    team_name = (team or {}).get("name", "Equipo")

    player = player_repo.get(event.get("player_id")) if event.get("player_id") else None
    player_name = (player or {}).get("name") or "Jugador sin nombre"

    home_team = team_repo.get(match.get("home_team_id")) if match.get("home_team_id") else None
    away_team = team_repo.get(match.get("away_team_id")) if match.get("away_team_id") else None
    home_name = (home_team or {}).get("name") or match.get("home_team_id", "")
    away_name = (away_team or {}).get("name") or match.get("away_team_id", "")

    is_red = event.get("type") in _CARD_RED_TYPES
    card_label = "Tarjeta Roja" if is_red else "Tarjeta Amarilla"

    concept = f"{card_label} - {player_name} ({team_name})"
    description = f"{tournament_name} - Partido {match.get('date', '')[:10]}: {home_name} vs {away_name}"
    return concept, description, None


def run(account_id: str | None, execute: bool) -> None:
    pr_table = payment_request_table()
    event_repo = TournamentMatchEventRepo()
    match_repo = TournamentMatchRepo()
    team_repo = TournamentTeamRepo()
    player_repo = TournamentPlayerRepo()
    tournament_repo = TournamentRepo()

    filter_expr = Attr("category").eq("tournament_fine")
    if account_id:
        filter_expr = filter_expr & Attr("account_id").eq(account_id)

    charges = _scan_all(pr_table, FilterExpression=filter_expr)
    print(f"Found {len(charges)} tournament_fine payment request(s)"
          + (f" for account {account_id}" if account_id else " across all accounts"))

    to_update: list[tuple[str, str, str]] = []  # (id, new_concept, new_description)
    skipped: list[tuple[str, str]] = []
    already_current = 0

    for item in charges:
        concept, description, skip_reason = _derive_concept_description(
            item, event_repo, match_repo, team_repo, player_repo, tournament_repo
        )
        if skip_reason:
            skipped.append((item["id"], skip_reason))
            continue
        if item.get("concept") == concept and item.get("description") == description:
            already_current += 1
            continue
        to_update.append((item["id"], concept, description))

    print(f"{len(to_update)} item(s) need updating; {already_current} already current; "
          f"{len(skipped)} skipped")

    for pr_id, concept, description in to_update:
        print(f"  {pr_id}: concept -> {concept!r} | description -> {description!r}")

    if skipped:
        print("\nSkipped:")
        for pr_id, reason in skipped:
            print(f"  {pr_id}: {reason}")

    if not execute:
        print("\n[DRY RUN] Run with --execute to apply.")
        return

    for pr_id, concept, description in to_update:
        pr_table.update_item(
            Key={"id": pr_id},
            UpdateExpression="SET concept = :c, description = :d",
            ExpressionAttributeValues={":c": concept, ":d": description},
        )
    print(f"✅ Updated {len(to_update)} payment request(s)")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Backfill richer concept/description on existing tournament_fine payment requests"
    )
    parser.add_argument("--account-id", help="Limit to a single account (default: all accounts)")
    parser.add_argument("--execute", action="store_true", help="Apply the updates (default: dry run)")
    args = parser.parse_args()
    run(args.account_id, args.execute)


if __name__ == "__main__":
    main()
