"""Account type helpers. Pure, so they can be unit-tested without importing auth."""

from typing import Any

DEFAULT_ACCOUNT_TYPE = "club"


def is_club(account: dict[str, Any] | None) -> bool:
    """True when the account is a club. A missing setting counts as club, like the web default."""
    settings = (account or {}).get("settings") or {}
    return (settings.get("account_type") or DEFAULT_ACCOUNT_TYPE) == "club"
