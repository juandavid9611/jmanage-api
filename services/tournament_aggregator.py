"""Tournament aggregator — pure functions that compute the deltas to apply
to materialized `stats` fields on Tournament / TournamentTeam / TournamentPlayer
items when an event or match changes.

Design notes:
- `event_delta` returns a dict of {player, team, tournament} deltas. Caller
  fetches current `stats` for each and applies the delta (`apply_delta`),
  then persists via `*_repo.update_stats`.
- `match_outcome_delta` covers the W/D/L + goals_for/against + points + form
  side of the picture. Only **group-stage** matches contribute to team
  standings (matches with no `round` set, i.e. not knockouts). Tournament
  totals (`matches_played`, `total_goals`) include all matches.
- `second_yellow` events count toward `red_cards` (matches the existing
  convention in stats_service:60 and player_service:133).
- All functions here are pure — no I/O. They return dicts of deltas.

Sign convention: `sign=+1` adds the contribution, `sign=-1` reverses it.
This lets callers handle event/match deletion and updates symmetrically
(reverse the old, apply the new).
"""

from __future__ import annotations

from typing import Any

# ── Event-type sets ────────────────────────────────────────────────────

GOAL_TYPES = ("goal", "penalty_scored")  # counted as goals_for
RED_TYPES = ("red_card", "second_yellow")
YELLOW_TYPES = ("yellow_card",)


def _zero_player_stats() -> dict[str, int]:
    return {
        "appearances": 0,
        "goals": 0,
        "penalties": 0,
        "own_goals": 0,
        "assists": 0,
        "yellow_cards": 0,
        "red_cards": 0,
    }


def _zero_team_stats() -> dict[str, Any]:
    return {
        "played": 0,
        "won": 0,
        "drawn": 0,
        "lost": 0,
        "goals_for": 0,
        "goals_against": 0,
        "goal_difference": 0,
        "points": 0,
        "yellow_cards": 0,
        "red_cards": 0,
        "form": [],
    }


def _zero_tournament_stats() -> dict[str, Any]:
    # Note: `average_goals_per_match` is intentionally NOT persisted —
    # DDB rejects floats, and it's a derived metric. Computed on read in
    # the stats service.
    return {
        "total_goals": 0,
        "total_yellow_cards": 0,
        "total_red_cards": 0,
        "total_matches": 0,
        "matches_played": 0,
    }


def default_player_stats() -> dict[str, int]:
    return _zero_player_stats()


def default_team_stats() -> dict[str, Any]:
    return _zero_team_stats()


def default_tournament_stats() -> dict[str, Any]:
    return _zero_tournament_stats()


# ── Event-driven deltas ────────────────────────────────────────────────


def event_delta(event: dict[str, Any], sign: int = 1) -> dict[str, dict[str, Any]]:
    """Compute the deltas to apply to player/team/tournament stats for a
    single event being added (`sign=+1`) or removed (`sign=-1`).

    Returns: {
        "player_id": str | None,    # which player's stats to mutate
        "assist_player_id": str | None,
        "team_id": str | None,
        "player_delta": dict,        # additive delta
        "assist_player_delta": dict, # additive delta for the assist player
        "team_delta": dict,
        "tournament_delta": dict,
    }
    """
    etype = event.get("type", "")
    player_id = event.get("player_id")
    assist_player_id = event.get("assist_player_id")
    team_id = event.get("team_id")

    player = _zero_player_stats()
    assist = _zero_player_stats()
    team = _zero_team_stats()
    tournament = _zero_tournament_stats()

    s = int(sign)

    if etype in GOAL_TYPES:
        player["goals"] += 1 * s
        if etype == "penalty_scored":
            player["penalties"] += 1 * s
        # team's goals_for is reconciled via match_outcome_delta when the
        # match flips to/from finished — don't double-count here.
        tournament["total_goals"] += 1 * s
        if assist_player_id:
            assist["assists"] += 1 * s

    elif etype == "own_goal":
        # An own_goal is scored against the player's own team. The
        # scoring side (the opponent) gets the goal counted via match
        # outcome aggregation. The player gets credit for the own goal.
        player["own_goals"] += 1 * s
        tournament["total_goals"] += 1 * s

    elif etype in YELLOW_TYPES:
        player["yellow_cards"] += 1 * s
        team["yellow_cards"] += 1 * s
        tournament["total_yellow_cards"] += 1 * s

    elif etype in RED_TYPES:
        player["red_cards"] += 1 * s
        team["red_cards"] += 1 * s
        tournament["total_red_cards"] += 1 * s

    return {
        "player_id": player_id,
        "assist_player_id": assist_player_id,
        "team_id": team_id,
        "player_delta": player,
        "assist_player_delta": assist,
        "team_delta": team,
        "tournament_delta": tournament,
    }


# ── Match-driven deltas ────────────────────────────────────────────────


def _is_group_stage_match(match: dict[str, Any]) -> bool:
    """Group-stage matches are matchweek-driven and have no `round` set.
    Knockout matches carry a `round` like 'quarterfinals' / 'semiFinals' /
    'final' — those don't contribute to team standings."""
    return not match.get("round")


def match_outcome_delta(
    match: dict[str, Any],
    rules: dict[str, Any],
    sign: int = 1,
) -> dict[str, Any]:
    """Compute team-stats + tournament-stats deltas for a match transitioning
    in/out of finished status.

    `sign=+1`: apply outcome (e.g. when match becomes finished).
    `sign=-1`: revert outcome (e.g. when match goes back to scheduled or is deleted).

    Returns: {
        "home_team_id": str | None,
        "away_team_id": str | None,
        "home_delta": dict,
        "away_delta": dict,
        "tournament_delta": dict,
    }

    Notes:
    - W/D/L + goals_for/against + points + form: only applied if it's a
      group-stage match (no `round`). Tournament `matches_played` and goals
      count for all matches.
    - `form` entries are tagged with the match's id (e.g.
      `{"match_id": ..., "result": "W"}`) for `sign=+1`. `sign=-1` emits
      `"form_remove"` instead of a form entry, so `apply_delta` can find
      and strip the exact entry this match previously added — this keeps
      reopen/re-finish cycles from leaving stale form entries behind.
    """
    home_id = match.get("home_team_id")
    away_id = match.get("away_team_id")
    match_id = match.get("id")
    sh = match.get("score_home")
    sa = match.get("score_away")
    s = int(sign)

    home = _zero_team_stats()
    away = _zero_team_stats()
    tournament = _zero_tournament_stats()

    if sh is None or sa is None or sh == -1 or sa == -1:
        # Match flipped to finished without scores — don't apply W/D/L
        # deltas. Tournament matches_played still increments.
        tournament["matches_played"] += 1 * s
        return {
            "home_team_id": home_id,
            "away_team_id": away_id,
            "home_delta": home,
            "away_delta": away,
            "tournament_delta": tournament,
        }

    sh = int(sh)
    sa = int(sa)

    tournament["matches_played"] += 1 * s

    if not _is_group_stage_match(match):
        return {
            "home_team_id": home_id,
            "away_team_id": away_id,
            "home_delta": home,
            "away_delta": away,
            "tournament_delta": tournament,
        }

    ppw = int(rules.get("points_per_win", 3))
    ppd = int(rules.get("points_per_draw", 1))
    ppl = int(rules.get("points_per_loss", 0))

    home["played"] += 1 * s
    home["goals_for"] += sh * s
    home["goals_against"] += sa * s
    home["goal_difference"] += (sh - sa) * s

    away["played"] += 1 * s
    away["goals_for"] += sa * s
    away["goals_against"] += sh * s
    away["goal_difference"] += (sa - sh) * s

    if sh > sa:
        home["won"] += 1 * s
        home["points"] += ppw * s
        away["lost"] += 1 * s
        away["points"] += ppl * s
        home_result, away_result = "W", "L"
    elif sh < sa:
        away["won"] += 1 * s
        away["points"] += ppw * s
        home["lost"] += 1 * s
        home["points"] += ppl * s
        home_result, away_result = "L", "W"
    else:
        home["drawn"] += 1 * s
        away["drawn"] += 1 * s
        home["points"] += ppd * s
        away["points"] += ppd * s
        home_result, away_result = "D", "D"

    # `form` entries are tagged with `match_id` so a reversal (sign=-1, e.g.
    # reopening a finished match) can remove the exact entry it added
    # instead of leaving it stranded — see `apply_delta`'s "form_remove"
    # handling. Only sign=+1 appends; sign=-1 requests a removal instead.
    if s == 1:
        home["form"] = [{"match_id": match_id, "result": home_result}]
        away["form"] = [{"match_id": match_id, "result": away_result}]
    else:
        home["form_remove"] = match_id
        away["form_remove"] = match_id

    return {
        "home_team_id": home_id,
        "away_team_id": away_id,
        "home_delta": home,
        "away_delta": away,
        "tournament_delta": tournament,
    }


# ── Stats arithmetic ───────────────────────────────────────────────────


def apply_delta(stats: dict[str, Any] | None, delta: dict[str, Any]) -> dict[str, Any]:
    """Add `delta` into `stats` (creates a new dict). Numeric fields are
    summed; list fields (form) are appended and tail-trimmed to length 5.
    Returns the merged dict; never mutates inputs.

    `"form_remove"` is a special, non-additive key: instead of being summed
    or appended, it strips the form entry tagged with that match_id (see
    `match_outcome_delta`'s reversal path) so reopening a finished match
    doesn't leave a stale form entry behind once the outcome is reapplied."""
    result = dict(stats or {})
    remove_match_id = delta.get("form_remove")
    if remove_match_id is not None:
        current_form = result.get("form") or []
        result["form"] = [
            e for e in current_form
            if not (isinstance(e, dict) and e.get("match_id") == remove_match_id)
        ]

    for key, value in delta.items():
        if key == "form_remove":
            continue
        current = result.get(key, 0 if not isinstance(value, list) else [])
        if isinstance(value, list):
            merged = (current or []) + value
            # Keep rolling window of last 5 outcomes
            if key == "form":
                merged = merged[-5:]
            result[key] = merged
        else:
            result[key] = (current or 0) + value
    # Keep goal_difference consistent if both goals_for and goals_against present
    if "goals_for" in result and "goals_against" in result:
        result["goal_difference"] = result["goals_for"] - result["goals_against"]
    return result


def update_average_goals_per_match(tournament_stats: dict[str, Any]) -> dict[str, Any]:
    """Compatibility shim — `average_goals_per_match` is no longer
    persisted (DDB rejects floats). Returns the input unchanged so
    callers don't need to be updated. Derive the value on read in
    `TournamentStatsService.get_stats`."""
    return dict(tournament_stats or {})


def derive_average_goals_per_match(tournament_stats: dict[str, Any] | None) -> float:
    """Compute the average goals per match from persisted counters."""
    s = tournament_stats or {}
    played = s.get("matches_played") or 0
    total = s.get("total_goals") or 0
    if played <= 0:
        return 0.0
    return round(float(total) / float(played), 2)


# ── Full recompute (admin helper) ──────────────────────────────────────


def recompute_tournament(
    tournament_id: str,
    *,
    match_repo: Any,
    event_repo: Any,
    team_repo: Any,
    player_repo: Any,
    tournament_repo: Any,
) -> dict[str, int]:
    """Walk all matches + events for a tournament, rebuild stats from
    scratch, and persist them on the tournament/team/player items.

    Returns a summary dict: {teams_updated, players_updated, events_processed,
    matches_processed}.
    """
    tournament = tournament_repo.get(tournament_id) or {}
    rules = tournament.get("rules") or {}

    teams = team_repo.list_by_tournament(tournament_id)
    players = player_repo.list_by_tournament(tournament_id)
    matches = match_repo.list_by_tournament(tournament_id)

    team_stats: dict[str, dict] = {t["id"]: default_team_stats() for t in teams}
    player_stats: dict[str, dict] = {p["id"]: default_player_stats() for p in players}
    tournament_stats = default_tournament_stats()
    tournament_stats["total_matches"] = len(matches)

    counted_matches = [m for m in matches if m.get("status") == "finished"]

    # Apply outcome deltas for finished matches
    for m in counted_matches:
        d = match_outcome_delta(m, rules, sign=1)
        if d["home_team_id"] in team_stats:
            team_stats[d["home_team_id"]] = apply_delta(
                team_stats[d["home_team_id"]], d["home_delta"]
            )
        if d["away_team_id"] in team_stats:
            team_stats[d["away_team_id"]] = apply_delta(
                team_stats[d["away_team_id"]], d["away_delta"]
            )
        tournament_stats = apply_delta(tournament_stats, d["tournament_delta"])

    # Apply event deltas across all matches (so player cards in non-finished
    # matches still register — though typically events are added during/after
    # the match).
    counted_match_ids = [m["id"] for m in matches if m.get("status") in ("finished", "live")]
    events_by_match = event_repo.batch_list_by_matches(counted_match_ids)
    events_processed = 0
    for events in events_by_match.values():
        for ev in events:
            events_processed += 1
            d = event_delta(ev, sign=1)
            if d["player_id"] and d["player_id"] in player_stats:
                player_stats[d["player_id"]] = apply_delta(
                    player_stats[d["player_id"]], d["player_delta"]
                )
            if d["assist_player_id"] and d["assist_player_id"] in player_stats:
                player_stats[d["assist_player_id"]] = apply_delta(
                    player_stats[d["assist_player_id"]], d["assist_player_delta"]
                )
            if d["team_id"] and d["team_id"] in team_stats:
                team_stats[d["team_id"]] = apply_delta(
                    team_stats[d["team_id"]], d["team_delta"]
                )
            tournament_stats = apply_delta(tournament_stats, d["tournament_delta"])

    tournament_stats = update_average_goals_per_match(tournament_stats)

    # Persist
    for tid, stats in team_stats.items():
        team_repo.update_stats(tid, stats)
    for pid, stats in player_stats.items():
        player_repo.update_stats(pid, stats)
    tournament_repo.update_stats(tournament_id, tournament_stats)

    return {
        "teams_updated": len(team_stats),
        "players_updated": len(player_stats),
        "events_processed": events_processed,
        "matches_processed": len(counted_matches),
    }
