"""Business logic for Tournaments, Groups, and Bracket management."""

from uuid import uuid4
from datetime import datetime
from typing import Any

from api.schemas.tournaments import (
    CreateTournament,
    PatchTournament,
    CreateGroup,
    PatchGroup,
    TournamentRules,
    GenerateBracketRequest,
    BracketOverride,
)
from repositories.tournament_repo_ddb import TournamentRepo


_DEFAULT_RULES = TournamentRules().dict()


class TournamentService:
    def __init__(self, repo: TournamentRepo, standings_service=None):
        self.repo = repo
        self.standings_service = standings_service

    # ── Tournament CRUD ──────────────────────────────────────────────

    def create_tournament(self, body: CreateTournament, account_id: str) -> dict[str, Any]:
        rules = body.rules.dict() if body.rules else dict(_DEFAULT_RULES)
        item = {
            "id": f"trn_{uuid4().hex}",
            "account_id": account_id,
            "name": body.name,
            "season": body.season,
            "type": body.type.value,
            "status": "draft",
            "current_matchweek": 0,
            "rules": rules,
            "groups": [],
            "bracket": {},
            "created_at": datetime.utcnow().isoformat(),
        }
        self.repo.put(item)
        return item

    def get_tournament(self, tournament_id: str, account_id: str) -> dict[str, Any] | None:
        item = self.repo.get(tournament_id)
        if item and item.get("account_id") == account_id:
            return item
        return None

    def list_tournaments(self, account_id: str, status: str | None = None) -> list[dict[str, Any]]:
        return self.repo.list_by_account(account_id, status=status)

    def update_tournament(self, tournament_id: str, account_id: str, body: PatchTournament) -> dict[str, Any] | None:
        existing = self.get_tournament(tournament_id, account_id)
        if not existing:
            return None
        updates = body.dict(exclude_unset=True, exclude_none=True)
        if "rules" in updates and isinstance(updates["rules"], dict):
            merged = {**existing.get("rules", {}), **updates["rules"]}
            updates["rules"] = merged
        if "status" in updates:
            updates["status"] = updates["status"]  # already a string via .value in Pydantic
        if not updates:
            return existing
        return self.repo.update(tournament_id, updates)

    def delete_tournament(self, tournament_id: str, account_id: str) -> bool:
        existing = self.get_tournament(tournament_id, account_id)
        if not existing:
            return False
        self.repo.delete(tournament_id)
        return True

    # ── Groups (embedded in tournament item) ─────────────────────────

    def list_groups(self, tournament_id: str, account_id: str) -> list[dict] | None:
        t = self.get_tournament(tournament_id, account_id)
        if not t:
            return None
        return t.get("groups", [])

    def create_group(self, tournament_id: str, account_id: str, body: CreateGroup) -> dict | None:
        t = self.get_tournament(tournament_id, account_id)
        if not t:
            return None
        group = {
            "id": f"grp_{uuid4().hex}",
            "name": body.name,
            "advancement_slots": body.advancement_slots,
            "teams": [],
        }
        groups = t.get("groups", [])
        groups.append(group)
        self.repo.update(tournament_id, {"groups": groups})
        return group

    def update_group(self, tournament_id: str, account_id: str, group_id: str, body: PatchGroup) -> dict | None:
        t = self.get_tournament(tournament_id, account_id)
        if not t:
            return None
        groups = t.get("groups", [])
        target = next((g for g in groups if g["id"] == group_id), None)
        if not target:
            return None
        updates = body.dict(exclude_unset=True, exclude_none=True)
        target.update(updates)
        self.repo.update(tournament_id, {"groups": groups})
        return target

    def delete_group(self, tournament_id: str, account_id: str, group_id: str) -> bool:
        t = self.get_tournament(tournament_id, account_id)
        if not t:
            return False
        groups = t.get("groups", [])
        new_groups = [g for g in groups if g["id"] != group_id]
        if len(new_groups) == len(groups):
            return False
        self.repo.update(tournament_id, {"groups": new_groups})
        return True

    def assign_team_to_group(
        self, tournament_id: str, account_id: str, group_id: str, team_id: str, seed: int | None = None
    ) -> dict | None:
        t = self.get_tournament(tournament_id, account_id)
        if not t:
            return None
        groups = t.get("groups", [])
        target = next((g for g in groups if g["id"] == group_id), None)
        if not target:
            return None
        # Avoid duplicates
        if any(te["team_id"] == team_id for te in target.get("teams", [])):
            return target
        entry = {"team_id": team_id}
        if seed is not None:
            entry["seed"] = seed
        target.setdefault("teams", []).append(entry)
        self.repo.update(tournament_id, {"groups": groups})
        return target

    def remove_team_from_group(
        self, tournament_id: str, account_id: str, group_id: str, team_id: str
    ) -> bool:
        t = self.get_tournament(tournament_id, account_id)
        if not t:
            return False
        groups = t.get("groups", [])
        target = next((g for g in groups if g["id"] == group_id), None)
        if not target:
            return False
        original = len(target.get("teams", []))
        target["teams"] = [te for te in target.get("teams", []) if te["team_id"] != team_id]
        if len(target["teams"]) == original:
            return False
        self.repo.update(tournament_id, {"groups": groups})
        return True

    # ── Bracket (embedded in tournament item) ────────────────────────

    def get_bracket(self, tournament_id: str, account_id: str) -> dict | None:
        t = self.get_tournament(tournament_id, account_id)
        if not t:
            return None
        return t.get("bracket", {})

    def generate_bracket(
        self, tournament_id: str, account_id: str, body: GenerateBracketRequest
    ) -> dict | None:
        """Generate a knockout bracket from seeds or group results.

        For now, produces a simple single-elimination structure
        from the provided teams (sorted by seed).
        """
        t = self.get_tournament(tournament_id, account_id)
        if not t:
            return None

        if body.source == "seeds" and body.teams:
            seeded = sorted(body.teams, key=lambda x: x.get("seed", 999))
            team_ids = [s["team_id"] for s in seeded]
        elif body.source == "groups":
            # Rank teams using real standings per group
            team_ids = []
            groups = t.get("groups", [])
            rules = t.get("rules", {})
            for g in groups:
                slots = int(g.get("advancement_slots", 2))
                if self.standings_service:
                    standings = self.standings_service.get_standings(
                        tournament_id, rules, group_id=g["id"]
                    )
                    ranked = standings.get("items", [])
                    for entry in ranked[:slots]:
                        team_ids.append(entry["team_id"])
                else:
                    # Fallback: use embedded order
                    for te in g.get("teams", [])[:slots]:
                        team_ids.append(te["team_id"])
        else:
            team_ids = []

        # Build bracket rounds
        bracket = self._build_bracket_structure(team_ids)
        self.repo.update(tournament_id, {"bracket": bracket})
        return bracket

    def update_bracket(
        self, tournament_id: str, account_id: str, body: BracketOverride
    ) -> dict | None:
        t = self.get_tournament(tournament_id, account_id)
        if not t:
            return None
        bracket = t.get("bracket", {})
        round_matches = bracket.get(body.round, [])
        if body.match_index < 0 or body.match_index >= len(round_matches):
            return None
        match_slot = round_matches[body.match_index]
        if body.team1_id is not None:
            match_slot["team1_id"] = body.team1_id
        if body.team2_id is not None:
            match_slot["team2_id"] = body.team2_id
        self.repo.update(tournament_id, {"bracket": bracket})
        return bracket

    def advance_winner(
        self, tournament_id: str, account_id: str, match_id: str, winner_team_id: str
    ) -> dict | None:
        """Advance the winner of a knockout match to the next bracket round.

        Finds the match_id in the bracket structure and places the winner
        in the corresponding next-round slot.
        """
        t = self.get_tournament(tournament_id, account_id)
        if not t:
            return None
        bracket = t.get("bracket", {})
        round_order = list(bracket.keys())

        for ri, round_name in enumerate(round_order):
            matches = bracket[round_name]
            for mi, slot in enumerate(matches):
                if slot.get("match_id") == match_id:
                    slot["winner_team_id"] = winner_team_id
                    # Advance to next round
                    if ri + 1 < len(round_order):
                        next_round = round_order[ri + 1]
                        next_idx = mi // 2
                        next_matches = bracket[next_round]
                        if next_idx < len(next_matches):
                            next_slot = next_matches[next_idx]
                            if mi % 2 == 0:
                                next_slot["team1_id"] = winner_team_id
                            else:
                                next_slot["team2_id"] = winner_team_id
                    self.repo.update(tournament_id, {"bracket": bracket})
                    return bracket
        return None

    # ── Helpers ──────────────────────────────────────────────────────

    @staticmethod
    def _build_bracket_structure(team_ids: list[str]) -> dict:
        """Build a simple single-elimination bracket."""
        n = len(team_ids)
        if n < 2:
            return {}

        # Pad to next power of 2
        size = 1
        while size < n:
            size *= 2

        padded = team_ids + [None] * (size - n)

        round_names = []
        remaining = size
        while remaining > 1:
            if remaining == 2:
                round_names.append("final")
            elif remaining == 4:
                round_names.append("semiFinals")
            elif remaining == 8:
                round_names.append("quarterFinals")
            else:
                round_names.append(f"roundOf{remaining}")
            remaining //= 2

        bracket: dict = {}
        current_teams = padded
        for rnd in round_names:
            matches = []
            for i in range(0, len(current_teams), 2):
                matches.append({
                    "team1_id": current_teams[i],
                    "team2_id": current_teams[i + 1] if i + 1 < len(current_teams) else None,
                    "match_id": None,
                    "winner_team_id": None,
                    "score": {"team1": None, "team2": None},
                    "status": "pending",
                })
            bracket[rnd] = matches
            current_teams = [None] * len(matches)

        return bracket
