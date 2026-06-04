"""Standings service — reads materialized `stats` from TournamentTeam items.

Team aggregates (W/D/L, goals_for/against, points, form) are kept up to
date by the match service when matches flip in/out of `finished`. The
standings endpoint therefore only needs to read teams and sort them.

For per-group standings, we filter the team list by `group_id`. We rely
on the invariant that during group stage each team plays only within its
own group — knockout-round matches don't contribute to `team.stats` (the
match service skips matches with a `round` value). So `team.stats` is
the team's group-stage record.
"""

from datetime import datetime
from typing import Any

from repositories.tournament_match_repo_ddb import TournamentMatchRepo
from repositories.tournament_team_repo_ddb import TournamentTeamRepo
from services.tournament_aggregator import default_team_stats


class StandingsService:
    def __init__(
        self,
        match_repo: TournamentMatchRepo,
        team_repo: TournamentTeamRepo | None = None,
    ):
        self.match_repo = match_repo
        self.team_repo = team_repo

    def get_standings(
        self,
        tournament_id: str,
        rules: dict[str, Any],
        group_id: str | None = None,
        group_team_ids: list[str] | None = None,
    ) -> dict[str, Any]:
        """Return ranked standings for the tournament (or a group).

        Cost: 1 query (list teams) when team_repo is wired. Falls back to
        the pre-materialization match-scan path if team_repo is missing
        — only used by tests / older wiring.
        """
        if self.team_repo is None:
            return self._fallback_compute(tournament_id, rules, group_id, group_team_ids)

        teams = self.team_repo.list_by_tournament(tournament_id)
        teams = self._filter_for_group(teams, group_id=group_id, group_team_ids=group_team_ids)
        entries = [self._row_from_team(t) for t in teams]
        return self._rank_and_pack(entries)

    def get_all_standings(
        self,
        tournament_id: str,
        rules: dict[str, Any],
        groups: list[dict[str, Any]],
        teams: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """Return per-group + tournament-wide standings in one shot."""
        result: dict[str, Any] = {"groups": {}}

        for group in groups:
            gid = group["id"]
            group_teams = [t for t in teams if t.get("group_id") == gid]
            entries = [self._row_from_team(t) for t in group_teams]
            result["groups"][gid] = {
                "group_name": group.get("name", ""),
                **self._rank_and_pack(entries),
            }

        all_entries = [self._row_from_team(t) for t in teams]
        result["tournament"] = self._rank_and_pack(all_entries)
        return result

    # ── helpers ──────────────────────────────────────────────────────

    @staticmethod
    def _filter_for_group(
        teams: list[dict[str, Any]],
        *,
        group_id: str | None,
        group_team_ids: list[str] | None,
    ) -> list[dict[str, Any]]:
        if group_team_ids is not None:
            team_set = set(group_team_ids)
            return [t for t in teams if t["id"] in team_set]
        if group_id:
            return [t for t in teams if t.get("group_id") == group_id]
        return teams

    @staticmethod
    def _row_from_team(team: dict[str, Any]) -> dict[str, Any]:
        s = team.get("stats") or default_team_stats()
        gd = s.get("goal_difference", s.get("goals_for", 0) - s.get("goals_against", 0))
        return {
            "team_id": team["id"],
            "played": s.get("played", 0),
            "won": s.get("won", 0),
            "drawn": s.get("drawn", 0),
            "lost": s.get("lost", 0),
            "goals_for": s.get("goals_for", 0),
            "goals_against": s.get("goals_against", 0),
            "goal_difference": gd,
            "points": s.get("points", 0),
            "form": list(s.get("form") or [])[-5:],
        }

    @staticmethod
    def _rank_and_pack(entries: list[dict[str, Any]]) -> dict[str, Any]:
        entries.sort(key=lambda e: (-e["points"], -e["goal_difference"], -e["goals_for"]))
        for i, e in enumerate(entries, 1):
            e["rank"] = i
        return {"as_of": datetime.utcnow().isoformat(), "items": entries}

    # ── Fallback (old match-scan path) ───────────────────────────────

    def _fallback_compute(
        self,
        tournament_id: str,
        rules: dict[str, Any],
        group_id: str | None,
        group_team_ids: list[str] | None,
    ) -> dict[str, Any]:
        """Legacy path — used only when team_repo isn't wired. Kept short
        because new wiring always passes a team_repo."""
        finished = self.match_repo.list_by_tournament(
            tournament_id, status="finished", group_id=group_id if not group_team_ids else None
        )
        live = self.match_repo.list_by_tournament(
            tournament_id, status="live", group_id=group_id if not group_team_ids else None
        )
        matches = finished + live
        if group_team_ids:
            ts = set(group_team_ids)
            matches = [
                m for m in matches
                if m.get("home_team_id") in ts and m.get("away_team_id") in ts
            ]

        ppw = rules.get("points_per_win", 3)
        ppd = rules.get("points_per_draw", 1)
        ppl = rules.get("points_per_loss", 0)

        teams: dict[str, dict] = {}
        for m in matches:
            home = m.get("home_team_id")
            away = m.get("away_team_id")
            sh = m.get("score_home")
            sa = m.get("score_away")
            if sh is None or sa is None or sh == -1 or sa == -1:
                continue
            sh, sa = int(sh), int(sa)
            for tid in (home, away):
                if tid not in teams:
                    teams[tid] = {
                        "team_id": tid, "played": 0, "won": 0, "drawn": 0, "lost": 0,
                        "goals_for": 0, "goals_against": 0, "points": 0, "results": [],
                    }
            teams[home]["played"] += 1
            teams[home]["goals_for"] += sh
            teams[home]["goals_against"] += sa
            teams[away]["played"] += 1
            teams[away]["goals_for"] += sa
            teams[away]["goals_against"] += sh
            if sh > sa:
                teams[home]["won"] += 1; teams[home]["points"] += ppw; teams[home]["results"].append("W")
                teams[away]["lost"] += 1; teams[away]["points"] += ppl; teams[away]["results"].append("L")
            elif sh < sa:
                teams[away]["won"] += 1; teams[away]["points"] += ppw; teams[away]["results"].append("W")
                teams[home]["lost"] += 1; teams[home]["points"] += ppl; teams[home]["results"].append("L")
            else:
                teams[home]["drawn"] += 1; teams[home]["points"] += ppd; teams[home]["results"].append("D")
                teams[away]["drawn"] += 1; teams[away]["points"] += ppd; teams[away]["results"].append("D")

        entries = []
        for t in teams.values():
            entries.append({
                "team_id": t["team_id"], "played": t["played"], "won": t["won"],
                "drawn": t["drawn"], "lost": t["lost"], "goals_for": t["goals_for"],
                "goals_against": t["goals_against"],
                "goal_difference": t["goals_for"] - t["goals_against"],
                "points": t["points"], "form": t["results"][-5:],
            })
        return self._rank_and_pack(entries)
