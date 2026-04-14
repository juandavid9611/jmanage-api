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
from repositories.s3_adapter import S3Adapter
from repositories.tournament_repo_ddb import TournamentRepo
from repositories.tournament_team_repo_ddb import TournamentTeamRepo
from repositories.tournament_match_repo_ddb import TournamentMatchRepo


_DEFAULT_RULES = TournamentRules().dict()

# Canonical progression order — used to sort bracket rounds correctly regardless
# of the key order DynamoDB returns (which is not guaranteed to match insertion order).
_BRACKET_ROUND_ORDER = ['roundOf32', 'roundOf16', 'quarterFinals', 'semiFinals', 'final']


class TournamentService:
    def __init__(
        self,
        repo: TournamentRepo,
        standings_service=None,
        team_repo: TournamentTeamRepo | None = None,
        match_repo: TournamentMatchRepo | None = None,
        s3: S3Adapter | None = None,
    ):
        self.repo = repo
        self.standings_service = standings_service
        self.team_repo = team_repo
        self.match_repo = match_repo
        self.s3 = s3

    # ── Tournament CRUD ──────────────────────────────────────────────

    # ── Logo upload ──────────────────────────────────────────────────

    def generate_logo_upload_url(
        self, tournament_id: str, account_id: str, filename: str, content_type: str
    ) -> dict[str, str]:
        if not self.s3:
            raise ValueError("S3 adapter not configured")
        return self.s3.presign_tournament_logo_put(
            account_id=account_id,
            tournament_id=tournament_id,
            filename=filename,
            content_type=content_type,
        )

    def _resolve_logo(self, item: dict[str, Any]) -> dict[str, Any]:
        """Convert stored S3 key to presigned GET URL in-place."""
        key = item.get("logo_url")
        if key and self.s3 and not key.startswith("http"):
            item["logo_url"] = self.s3.presign_get_from_explicit_key(
                key=key, content_type="image/jpeg"
            )
        return item

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
            "is_public": body.is_public,
            "current_matchweek": 0,
            "rules": rules,
            "groups": [],
            "bracket": {},
            "sport": body.sport or "",
            "teams_per_group": body.teams_per_group,
            "num_teams": body.num_teams,
            "tiebreaker_order": body.tiebreaker_order or [],
            "options": body.options or {},
            "description": body.description or "",
            "logo_url": body.logo_url or "",
            "start_date": body.start_date or "",
            "end_date": body.end_date or "",
            "location": body.location or "",
            "created_at": datetime.utcnow().isoformat(),
        }
        self.repo.put(item)
        return self._resolve_logo(item)

    def get_tournament(self, tournament_id: str, account_id: str) -> dict[str, Any] | None:
        item = self.repo.get(tournament_id)
        if item and item.get("account_id") == account_id:
            return self._resolve_logo(item)
        return None

    def get_public_tournament(self, tournament_id: str) -> dict[str, Any] | None:
        item = self.repo.get(tournament_id)
        if item and item.get("is_public"):
            return self._resolve_logo(item)
        return None

    def list_public_tournaments(self, status: str | None = None) -> dict[str, Any]:
        all_items = self.repo.list_public()
        counts: dict[str, int] = {}
        for item in all_items:
            s = item.get("status", "")
            counts[s] = counts.get(s, 0) + 1
        items = [i for i in all_items if i.get("status") == status] if status else all_items
        return {"items": [self._resolve_logo(i) for i in items], "counts_by_status": counts}

    def list_tournaments(self, account_id: str, status: str | None = None) -> dict[str, Any]:
        all_items = self.repo.list_by_account(account_id)
        counts: dict[str, int] = {}
        for item in all_items:
            s = item.get("status", "")
            counts[s] = counts.get(s, 0) + 1
        items = [i for i in all_items if i.get("status") == status] if status else all_items
        return {"items": [self._resolve_logo(i) for i in items], "counts_by_status": counts}

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
        updated = self.repo.update(tournament_id, updates)
        return self._resolve_logo(updated) if updated else None

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
        # Keep team record in sync so group_index GSI stays accurate
        if self.team_repo:
            self.team_repo.update(team_id, {"group_id": group_id})
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
        # Clear group_id on the team record so group_index GSI stays accurate
        if self.team_repo:
            self.team_repo.clear_group(team_id)
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

        seed_map: dict | None = None

        if body.source == "seeds" and body.teams:
            seeded = sorted(body.teams, key=lambda x: x.get("seed", 999))
            team_ids = [s["team_id"] for s in seeded]
            seed_map = {s["team_id"]: s["seed"] for s in seeded}
        elif body.source == "groups":
            # Rank teams using real standings per group; interleave to avoid
            # same-group first-round matchups: [A1, B1, A2, B2, ...]
            groups = t.get("groups", [])
            rules = t.get("rules", {})
            group_qualifiers: list[list[str]] = []
            for g in groups:
                slots = int(g.get("advancement_slots", 2))
                group_team_ids = [te["team_id"] for te in g.get("teams", [])]
                if self.standings_service:
                    standings = self.standings_service.get_standings(
                        tournament_id, rules, group_id=g["id"],
                        group_team_ids=group_team_ids
                    )
                    ranked = standings.get("items", [])
                    group_qualifiers.append([e["team_id"] for e in ranked[:slots]])
                else:
                    # Fallback: use embedded order
                    group_qualifiers.append(
                        [te["team_id"] for te in g.get("teams", [])[:slots]]
                    )
            # Interleave by slot position so cross-group matchups in round 1
            max_slots = max((len(q) for q in group_qualifiers), default=0)
            team_ids = []
            current_seed = 1
            seed_map = {}
            for slot_idx in range(max_slots):
                for g_qualified in group_qualifiers:
                    if slot_idx < len(g_qualified):
                        tid = g_qualified[slot_idx]
                        team_ids.append(tid)
                        seed_map[tid] = current_seed
                        current_seed += 1
        else:
            team_ids = []

        # Build bracket rounds
        bracket = self._build_bracket_structure(team_ids, seed_map=seed_map)
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
        if body.match_id is not None:
            match_slot["match_id"] = body.match_id
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
        # Build round order using canonical progression; unknown rounds go at the end.
        known = [r for r in _BRACKET_ROUND_ORDER if r in bracket]
        unknown = [r for r in bracket if r not in _BRACKET_ROUND_ORDER]
        round_order = known + unknown

        # Look up match data once — used for both fallback search and score sync
        match_data = None
        if self.match_repo:
            match_data = self.match_repo.get(match_id)

        def _apply_winner(ri, round_name, mi, slot):
            valid_teams = {slot.get("team1_id"), slot.get("team2_id")} - {None}
            if valid_teams and winner_team_id not in valid_teams:
                raise ValueError(
                    f"winner_team_id '{winner_team_id}' is not a participant in this match"
                )
            # Self-heal: persist match_id if it was missing from the slot
            if not slot.get("match_id"):
                slot["match_id"] = match_id
            slot["winner_team_id"] = winner_team_id
            slot["status"] = "finished"
            # Sync match score into the bracket slot for display
            if match_data:
                sh = match_data.get("score_home", -1)
                sa = match_data.get("score_away", -1)
                if sh is not None and sa is not None and int(sh) >= 0:
                    home_id = match_data.get("home_team_id")
                    if slot.get("team1_id") == home_id:
                        slot["score"] = {"team1": int(sh), "team2": int(sa)}
                    else:
                        slot["score"] = {"team1": int(sa), "team2": int(sh)}
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

        # Primary search: match by match_id stored in the bracket slot
        for ri, round_name in enumerate(round_order):
            for mi, slot in enumerate(bracket[round_name]):
                if slot.get("match_id") == match_id:
                    return _apply_winner(ri, round_name, mi, slot)

        # Fallback: match_id was never saved to the slot (created before the fix).
        # Identify the slot by the participating teams from the actual match record.
        if match_data:
            home_id = match_data.get("home_team_id")
            away_id = match_data.get("away_team_id")
            if home_id and away_id:
                for ri, round_name in enumerate(round_order):
                    for mi, slot in enumerate(bracket[round_name]):
                        if {slot.get("team1_id"), slot.get("team2_id")} == {home_id, away_id}:
                            return _apply_winner(ri, round_name, mi, slot)

        return None

    # ── Helpers ──────────────────────────────────────────────────────

    @staticmethod
    def _bracket_seed_positions(size: int) -> list[int]:
        """Return the position indices for standard bracket seeding.

        Produces the ordering so that consecutive pairs give:
          (seed 1 vs seed N), (seed 4 vs seed N-3), (seed 2 vs seed N-1), ...
        ensuring top seeds never meet until later rounds.
        """
        if size == 1:
            return [0]
        half = size // 2
        top = TournamentService._bracket_seed_positions(half)
        bottom = [size - 1 - i for i in top]
        return [x for pair in zip(top, bottom) for x in pair]

    @staticmethod
    def _build_bracket_structure(
        team_ids: list[str], seed_map: dict | None = None
    ) -> dict:
        """Build a simple single-elimination bracket.

        When seed_map is provided teams are reordered using standard bracket
        seeding so that seed 1 faces the lowest seed, seed 2 faces the second
        lowest, etc.  This produces correct cross-group matchups for hybrid
        tournaments (e.g. Group A 1st vs Group B 2nd).
        """
        n = len(team_ids)
        if n < 2:
            return {}

        # Pad to next power of 2
        size = 1
        while size < n:
            size *= 2

        if seed_map:
            # Sort teams by seed (unseeded / None last), then apply standard
            # bracket seeding order so top seeds are on opposite sides.
            seeded_teams = sorted(
                [t for t in team_ids if t is not None],
                key=lambda t: seed_map.get(t, 9999),
            )
            seed_ordered = seeded_teams + [None] * (size - len(seeded_teams))
            positions = TournamentService._bracket_seed_positions(size)
            padded = [seed_ordered[i] for i in positions]
        else:
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
        first_round = True
        for rnd in round_names:
            matches = []
            for i in range(0, len(current_teams), 2):
                t1 = current_teams[i]
                t2 = current_teams[i + 1] if i + 1 < len(current_teams) else None
                slot: dict = {
                    "team1_id": t1,
                    "team2_id": t2,
                    "match_id": None,
                    "winner_team_id": None,
                    "score": {"team1": None, "team2": None},
                    "status": "pending",
                }
                if first_round and seed_map:
                    slot["seed1"] = seed_map.get(t1) if t1 else None
                    slot["seed2"] = seed_map.get(t2) if t2 else None
                matches.append(slot)
            bracket[rnd] = matches
            current_teams = [None] * len(matches)
            first_round = False

        return bracket
