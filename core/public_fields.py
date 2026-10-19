"""Allowlists for what the unauthenticated /public/tournaments endpoints may return.

The stored team and player records carry personal data (manager name, email and
phone, national ID numbers, internal user ids, uploaded documents). Public
responses must be built from an explicit allowlist, never from the raw item, so a
new private field added to a record later stays private by default.
"""

from typing import Any

PUBLIC_TEAM_FIELDS = (
    "id",
    "tournament_id",
    "name",
    "short_name",
    "logo_url",
    "primary_color",
    "seed",
    "group_id",
    "stats",
)

PUBLIC_PLAYER_FIELDS = (
    "id",
    "tournament_id",
    "team_id",
    "name",
    "number",
    "position",
    "avatar_url",
    "stats",
)


def _only(item: dict[str, Any], fields: tuple[str, ...]) -> dict[str, Any]:
    return {key: item[key] for key in fields if key in item}


def public_team(item: dict[str, Any]) -> dict[str, Any]:
    return _only(item, PUBLIC_TEAM_FIELDS)


def public_player(item: dict[str, Any]) -> dict[str, Any]:
    return _only(item, PUBLIC_PLAYER_FIELDS)
