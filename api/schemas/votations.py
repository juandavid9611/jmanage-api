"""Pydantic models for the Votations domain."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class CandidateIn(BaseModel):
    id: str
    name: str
    avatar_url: str | None = None
    training_pct: int
    match_pct: int = 0
    goals: int = 0
    assists: int = 0
    mvp: int = 0
    eligible: bool = True


class CreateVotation(BaseModel):
    workspace_id: str
    month: str                       # "YYYY-MM"
    min_pct: int = Field(ge=0, le=100)
    candidates: list[CandidateIn]    # already computed by preview endpoint


class CastVote(BaseModel):
    candidate_id: str


class CloseVotation(BaseModel):
    status: str = "closed"
