"""Computed standings service — no DDB table.

Reads finished matches for a tournament (or group), applies
tournament rules, and returns ranked standings.
"""

from datetime import datetime
from typing import Any

from repositories.tournament_match_repo_ddb import TournamentMatchRepo


class StandingsService:
    def __init__(self, match_repo: TournamentMatchRepo):
        self.match_repo = match_repo

    def get_standings(
        self,
        tournament_id: str,
        rules: dict[str, Any],
        group_id: str | None = None,
        group_team_ids: list[str] | None = None,
    ) -> dict[str, Any]:
        """Compute standings from finished matches."""
        # When group_team_ids is provided, always filter by team membership —
        # matches may not have group_id set (e.g. generated without group context).
        if group_team_ids:
            all_matches = self.match_repo.list_by_tournament(
                tournament_id, status="finished"
            )
            team_set = set(group_team_ids)
            matches = [
                m for m in all_matches
                if m.get("home_team_id") in team_set and m.get("away_team_id") in team_set
            ]
        else:
            matches = self.match_repo.list_by_tournament(
                tournament_id, status="finished", group_id=group_id
            )

        ppw = rules.get("points_per_win", 3)
        ppd = rules.get("points_per_draw", 1)
        ppl = rules.get("points_per_loss", 0)

        # Accumulator per team
        teams: dict[str, dict] = {}

        for m in matches:
            home_id = m.get("home_team_id")
            away_id = m.get("away_team_id")
            sh = m.get("score_home")
            sa = m.get("score_away")

            if sh is None or sa is None or sh == -1 or sa == -1:
                continue

            sh = int(sh)
            sa = int(sa)

            for tid in (home_id, away_id):
                if tid not in teams:
                    teams[tid] = {
                        "team_id": tid,
                        "played": 0,
                        "won": 0,
                        "drawn": 0,
                        "lost": 0,
                        "goals_for": 0,
                        "goals_against": 0,
                        "points": 0,
                        "results": [],  # for form computation
                    }

            # Home
            teams[home_id]["played"] += 1
            teams[home_id]["goals_for"] += sh
            teams[home_id]["goals_against"] += sa

            # Away
            teams[away_id]["played"] += 1
            teams[away_id]["goals_for"] += sa
            teams[away_id]["goals_against"] += sh

            if sh > sa:
                teams[home_id]["won"] += 1
                teams[home_id]["points"] += ppw
                teams[home_id]["results"].append("W")
                teams[away_id]["lost"] += 1
                teams[away_id]["points"] += ppl
                teams[away_id]["results"].append("L")
            elif sh < sa:
                teams[away_id]["won"] += 1
                teams[away_id]["points"] += ppw
                teams[away_id]["results"].append("W")
                teams[home_id]["lost"] += 1
                teams[home_id]["points"] += ppl
                teams[home_id]["results"].append("L")
            else:
                teams[home_id]["drawn"] += 1
                teams[home_id]["points"] += ppd
                teams[home_id]["results"].append("D")
                teams[away_id]["drawn"] += 1
                teams[away_id]["points"] += ppd
                teams[away_id]["results"].append("D")

        # Build sorted standings
        entries = []
        for t in teams.values():
            gd = t["goals_for"] - t["goals_against"]
            entries.append({
                "team_id": t["team_id"],
                "played": t["played"],
                "won": t["won"],
                "drawn": t["drawn"],
                "lost": t["lost"],
                "goals_for": t["goals_for"],
                "goals_against": t["goals_against"],
                "goal_difference": gd,
                "points": t["points"],
                "form": t["results"][-5:],  # last 5 results
            })

        # Sort: points desc, goal_difference desc, goals_for desc
        entries.sort(key=lambda e: (-e["points"], -e["goal_difference"], -e["goals_for"]))

        # Add rank
        for i, e in enumerate(entries, 1):
            e["rank"] = i

        return {
            "as_of": datetime.utcnow().isoformat(),
            "items": entries,
        }

    def get_all_standings(
        self,
        tournament_id: str,
        rules: dict[str, Any],
        groups: list[dict[str, Any]],
        teams: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """Return standings for all groups + tournament level in one DB round trip."""
        finished = self.match_repo.list_by_tournament(tournament_id, status="finished")

        # Build group membership lookup: group_id -> set of team_ids
        group_team_ids: dict[str, set[str]] = {}
        for team in teams:
            gid = team.get("group_id")
            if gid:
                group_team_ids.setdefault(gid, set()).add(team["id"])

        result: dict[str, Any] = {"groups": {}}

        for group in groups:
            gid = group["id"]
            team_set = group_team_ids.get(gid, set())
            group_matches = [
                m for m in finished
                if m.get("home_team_id") in team_set and m.get("away_team_id") in team_set
            ]
            group_standings = self._compute_standings(group_matches, rules)
            result["groups"][gid] = {
                "group_name": group.get("name", ""),
                **group_standings,
            }

        # Tournament-level (all finished matches)
        result["tournament"] = self._compute_standings(finished, rules)
        return result

    def _compute_standings(self, matches: list[dict[str, Any]], rules: dict[str, Any]) -> dict[str, Any]:
        """Compute and return sorted standings from a list of matches."""
        ppw = rules.get("points_per_win", 3)
        ppd = rules.get("points_per_draw", 1)
        ppl = rules.get("points_per_loss", 0)

        teams: dict[str, dict] = {}

        for m in matches:
            home_id = m.get("home_team_id")
            away_id = m.get("away_team_id")
            sh = m.get("score_home")
            sa = m.get("score_away")

            if sh is None or sa is None or sh == -1 or sa == -1:
                continue

            sh = int(sh)
            sa = int(sa)

            for tid in (home_id, away_id):
                if tid not in teams:
                    teams[tid] = {
                        "team_id": tid,
                        "played": 0, "won": 0, "drawn": 0, "lost": 0,
                        "goals_for": 0, "goals_against": 0, "points": 0, "results": [],
                    }

            teams[home_id]["played"] += 1
            teams[home_id]["goals_for"] += sh
            teams[home_id]["goals_against"] += sa
            teams[away_id]["played"] += 1
            teams[away_id]["goals_for"] += sa
            teams[away_id]["goals_against"] += sh

            if sh > sa:
                teams[home_id]["won"] += 1; teams[home_id]["points"] += ppw; teams[home_id]["results"].append("W")
                teams[away_id]["lost"] += 1; teams[away_id]["points"] += ppl; teams[away_id]["results"].append("L")
            elif sh < sa:
                teams[away_id]["won"] += 1; teams[away_id]["points"] += ppw; teams[away_id]["results"].append("W")
                teams[home_id]["lost"] += 1; teams[home_id]["points"] += ppl; teams[home_id]["results"].append("L")
            else:
                teams[home_id]["drawn"] += 1; teams[home_id]["points"] += ppd; teams[home_id]["results"].append("D")
                teams[away_id]["drawn"] += 1; teams[away_id]["points"] += ppd; teams[away_id]["results"].append("D")

        entries = []
        for t in teams.values():
            gd = t["goals_for"] - t["goals_against"]
            entries.append({
                "team_id": t["team_id"],
                "played": t["played"], "won": t["won"], "drawn": t["drawn"], "lost": t["lost"],
                "goals_for": t["goals_for"], "goals_against": t["goals_against"],
                "goal_difference": gd, "points": t["points"],
                "form": t["results"][-5:],
            })

        entries.sort(key=lambda e: (-e["points"], -e["goal_difference"], -e["goals_for"]))
        for i, e in enumerate(entries, 1):
            e["rank"] = i

        return {"as_of": datetime.utcnow().isoformat(), "items": entries}
