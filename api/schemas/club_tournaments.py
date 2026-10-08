"""Pydantic models for club tournaments, roster, matches and lineups (camelCase on the wire)."""

from __future__ import annotations

from pydantic import BaseModel, Field

from core.casing import camel_alias


class CamelModel(BaseModel):
    model_config = {
        "alias_generator": camel_alias,
        "populate_by_name": True,
    }


class TournamentIn(CamelModel):
    name: str = Field(min_length=1, max_length=120)
    category: str = Field(default="", max_length=60)


class RosterEntryIn(CamelModel):
    user_id: str | None = None        # real user, or...
    guest_name: str | None = Field(default=None, max_length=120)  # ...a guest without an account
    number: int | None = Field(default=None, ge=0, le=999)
    position: str = Field(default="", max_length=40)


class RosterEntryUpdate(CamelModel):
    guest_name: str | None = Field(default=None, max_length=120)  # guests only
    number: int | None = Field(default=None, ge=0, le=999)
    position: str | None = Field(default=None, max_length=40)


class RosterBulkIn(CamelModel):
    entries: list[RosterEntryIn] = Field(min_length=1, max_length=100)


class MatchIn(CamelModel):
    date: str = Field(pattern=r"^\d{4}-\d{2}-\d{2}$")  # YYYY-MM-DD
    rival: str = Field(min_length=1, max_length=120)


class LineupEntryIn(CamelModel):
    roster_entry_id: str
    called_up: bool
    status: str = ""                  # 'titular' | 'suplente' | ''
    minutes: int = Field(default=0, ge=0, le=300)


class LineupIn(CamelModel):
    entries: list[LineupEntryIn] = Field(max_length=100)
