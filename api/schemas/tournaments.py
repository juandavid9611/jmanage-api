"""Pydantic models for the Tournaments domain."""

from __future__ import annotations

from enum import Enum

from pydantic import BaseModel, Field


# ── Enums ────────────────────────────────────────────────────────────────

class TournamentType(str, Enum):
    league = "league"
    knockout = "knockout"
    hybrid = "hybrid"


class TournamentStatus(str, Enum):
    draft = "draft"
    active = "active"
    finished = "finished"
    cancelled = "cancelled"


class MatchStatus(str, Enum):
    scheduled = "scheduled"
    live = "live"
    finished = "finished"
    postponed = "postponed"


class PlayerPosition(str, Enum):
    forward = "Forward"
    midfielder = "Midfielder"
    defender = "Defender"
    goalkeeper = "Goalkeeper"


class MatchEventType(str, Enum):
    goal = "goal"
    own_goal = "own_goal"
    penalty_scored = "penalty_scored"
    penalty_missed = "penalty_missed"
    yellow_card = "yellow_card"
    second_yellow = "second_yellow"
    red_card = "red_card"
    substitution = "substitution"


# ── Rules ────────────────────────────────────────────────────────────────

class TournamentRules(BaseModel):
    points_per_win: int = 3
    points_per_draw: int = 1
    points_per_loss: int = 0
    total_matchweeks: int | None = None
    legs: int = 1
    yellow_cards_for_suspension: int = 5
    extra_time_allowed: bool = True
    penalties_allowed: bool = True
    max_substitutions: int = 5


# ── Tournament ───────────────────────────────────────────────────────────

class CreateTournament(BaseModel):
    name: str
    season: str | None = None
    type: TournamentType
    rules: TournamentRules | None = None
    is_public: bool = False


class PatchTournament(BaseModel):
    name: str | None = None
    status: TournamentStatus | None = None
    current_matchweek: int | None = None
    rules: TournamentRules | None = None
    is_public: bool | None = None


# ── Groups ───────────────────────────────────────────────────────────────

class CreateGroup(BaseModel):
    name: str
    advancement_slots: int = 2


class PatchGroup(BaseModel):
    name: str | None = None
    advancement_slots: int | None = None


class AssignTeamToGroup(BaseModel):
    team_id: str
    seed: int | None = None


# ── Teams ────────────────────────────────────────────────────────────────

class CreateTeam(BaseModel):
    name: str
    short_name: str = Field(..., max_length=3)
    logo_url: str | None = None
    seed: int | None = None
    group_id: str | None = None


class PatchTeam(BaseModel):
    name: str | None = None
    short_name: str | None = Field(None, max_length=3)
    logo_url: str | None = None
    seed: int | None = None


# ── Players ──────────────────────────────────────────────────────────────

class CreatePlayer(BaseModel):
    name: str
    position: PlayerPosition
    number: int
    avatar_url: str | None = None


class PatchPlayer(BaseModel):
    name: str | None = None
    position: PlayerPosition | None = None
    number: int | None = None
    avatar_url: str | None = None


# ── Matches ──────────────────────────────────────────────────────────────

class CreateMatch(BaseModel):
    home_team_id: str
    away_team_id: str
    date: str
    venue: str | None = None
    matchweek: int | None = None
    round: str | None = None
    group_id: str | None = None


class PatchMatch(BaseModel):
    date: str | None = None
    venue: str | None = None
    status: MatchStatus | None = None
    score_home: int | None = None
    score_away: int | None = None


# ── Match Events ─────────────────────────────────────────────────────────

class CreateMatchEvent(BaseModel):
    type: MatchEventType
    minute: int = Field(..., ge=0, le=120)
    stoppage_time: int | None = Field(None, ge=0, le=15)
    player_id: str
    assist_player_id: str | None = None
    team_id: str
    event_index: int | None = None


class PatchMatchEvent(BaseModel):
    type: MatchEventType | None = None
    minute: int | None = Field(None, ge=0, le=120)
    stoppage_time: int | None = Field(None, ge=0, le=15)
    player_id: str | None = None
    assist_player_id: str | None = None
    team_id: str | None = None
    event_index: int | None = None


# ── Fixture Generation ───────────────────────────────────────────────────

class GenerateScheduleRequest(BaseModel):
    start_date: str
    match_interval_days: int = 7
    default_venue: str | None = None
    group_id: str | None = None


class GenerateBracketRequest(BaseModel):
    source: str = Field(..., pattern="^(seeds|groups)$")
    teams: list[dict] | None = None
    from_group_standings: bool | None = None


class BulkMatchesRequest(BaseModel):
    matches: list[CreateMatch]


class BracketOverride(BaseModel):
    round: str
    match_index: int
    team1_id: str | None = None
    team2_id: str | None = None
    match_id: str | None = None
