"""Pydantic models for the Votations domain."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field, model_validator


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
    period_type: Literal["month", "semester"] = "month"
    month: str | None = None         # "YYYY-MM", required when period_type == "month"
    start_date: str | None = None    # "YYYY-MM-DD", required when period_type == "semester"
    end_date: str | None = None      # "YYYY-MM-DD", required when period_type == "semester"
    min_pct: int = Field(ge=0, le=100)
    candidates: list[CandidateIn]    # already computed by preview endpoint

    @model_validator(mode="after")
    def _validate_period(self) -> "CreateVotation":
        if self.period_type == "month":
            if not self.month:
                raise ValueError("month is required when period_type is 'month'")
        elif not self.start_date or not self.end_date:
            raise ValueError("start_date and end_date are required when period_type is 'semester'")
        elif self.end_date <= self.start_date:
            raise ValueError("end_date must be after start_date")
        return self


class CastVote(BaseModel):
    candidate_id: str


class CloseVotation(BaseModel):
    status: str = "closed"
