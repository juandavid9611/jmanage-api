#!/usr/bin/env python3
"""One-off: rebuild the materialized `stats` fields on Tournament /
TournamentTeam / TournamentPlayer items by walking every match and event.

Usage (from jmanage-api/):
    source .venv/bin/activate
    python -m migrations.recompute_tournament_stats              # all tournaments
    python -m migrations.recompute_tournament_stats --id trn_xx  # one tournament
    python -m migrations.recompute_tournament_stats --dry-run    # show what would run

The HTTP endpoint POST /tournaments/{id}:recompute-stats does the same
thing per-tournament but requires admin auth; this script bypasses HTTP
for local seeding.
"""

import argparse
import sys
from dotenv import load_dotenv

load_dotenv()

from repositories.tournament_match_event_repo_ddb import TournamentMatchEventRepo  # noqa: E402
from repositories.tournament_match_repo_ddb import TournamentMatchRepo  # noqa: E402
from repositories.tournament_player_repo_ddb import TournamentPlayerRepo  # noqa: E402
from repositories.tournament_repo_ddb import TournamentRepo  # noqa: E402
from repositories.tournament_team_repo_ddb import TournamentTeamRepo  # noqa: E402
from services.tournament_aggregator import recompute_tournament  # noqa: E402


def _list_all_tournament_ids(repo: TournamentRepo) -> list[str]:
    """Scan the tournament table for every id. Local-dev convenience —
    in prod you'd scope by account_id."""
    table = repo._table  # noqa: SLF001
    ids: list[str] = []
    scan_kwargs = {"ProjectionExpression": "id"}
    while True:
        resp = table.scan(**scan_kwargs)
        ids.extend(item["id"] for item in resp.get("Items", []))
        last = resp.get("LastEvaluatedKey")
        if not last:
            break
        scan_kwargs["ExclusiveStartKey"] = last
    return ids


def main() -> int:
    parser = argparse.ArgumentParser(description="Recompute tournament stats")
    parser.add_argument(
        "--id",
        dest="tournament_id",
        help="Recompute a single tournament. Omit to recompute every tournament.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="List tournaments that would be processed, but don't write.",
    )
    args = parser.parse_args()

    tournament_repo = TournamentRepo()
    match_repo = TournamentMatchRepo()
    event_repo = TournamentMatchEventRepo()
    team_repo = TournamentTeamRepo()
    player_repo = TournamentPlayerRepo()

    if args.tournament_id:
        tournament_ids = [args.tournament_id]
    else:
        tournament_ids = _list_all_tournament_ids(tournament_repo)

    if not tournament_ids:
        print("no tournaments found")
        return 0

    print(f"recomputing {len(tournament_ids)} tournament(s)…")
    totals = {"teams_updated": 0, "players_updated": 0, "events_processed": 0, "matches_processed": 0}
    failures: list[tuple[str, str]] = []

    for tid in tournament_ids:
        if args.dry_run:
            print(f"  [dry-run] {tid}")
            continue
        try:
            summary = recompute_tournament(
                tid,
                match_repo=match_repo,
                event_repo=event_repo,
                team_repo=team_repo,
                player_repo=player_repo,
                tournament_repo=tournament_repo,
            )
            for k, v in summary.items():
                totals[k] = totals.get(k, 0) + v
            print(
                f"  {tid}: teams={summary['teams_updated']}, "
                f"players={summary['players_updated']}, "
                f"matches={summary['matches_processed']}, "
                f"events={summary['events_processed']}"
            )
        except Exception as e:  # noqa: BLE001
            failures.append((tid, str(e)))
            print(f"  {tid}: FAILED — {e}")

    if args.dry_run:
        return 0

    print("---")
    print(
        f"done. totals: teams={totals['teams_updated']}, "
        f"players={totals['players_updated']}, "
        f"matches={totals['matches_processed']}, "
        f"events={totals['events_processed']}"
    )
    if failures:
        print(f"failures: {len(failures)}")
        for tid, err in failures:
            print(f"  {tid}: {err}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
