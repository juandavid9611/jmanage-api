"""Pydantic models for the training sessions domain (camelCase on the wire)."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field

from core.casing import camel_alias


class CamelModel(BaseModel):
    model_config = {
        "alias_generator": camel_alias,
        "populate_by_name": True,
    }


class ExerciseIn(CamelModel):
    id: str | None = None
    name: str = Field(max_length=200)
    duration_minutes: int = Field(default=0, ge=0, le=600)
    instructions: str = Field(default="", max_length=5000)
    diagram: Any = None  # tactical-board JSON, opaque to the API


class SessionIn(CamelModel):
    title: str = Field(min_length=1, max_length=200)
    date: str = Field(min_length=1, max_length=40)
    exercises: list[ExerciseIn] = Field(default_factory=list, max_length=50)


class ReviewIn(CamelModel):
    approved: bool
    comment: str = Field(default="", max_length=2000)
