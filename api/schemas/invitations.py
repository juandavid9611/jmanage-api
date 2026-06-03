"""Pydantic models for the Tournament Invitation domain."""

from __future__ import annotations

from enum import Enum
from datetime import datetime
from typing import Optional

from pydantic import BaseModel
from core.casing import camel_alias


class CamelModel(BaseModel):
    model_config = {
        "alias_generator": camel_alias,
        "populate_by_name": True,
        "from_attributes": True,
    }


class InvitationStatus(str, Enum):
    pending = "pending"
    accepted = "accepted"
    expired = "expired"
    revoked = "revoked"


class TournamentInvitation(CamelModel):
    id: str
    account_id: str
    tournament_id: str
    tournament_team_id: str
    email: str
    token: str
    status: InvitationStatus
    expires_at: datetime
    accepted_at: Optional[datetime] = None
    accepted_by_user_id: Optional[str] = None
    created_at: datetime
    updated_at: datetime


class InvitationSummary(CamelModel):
    """Public projection returned by GET /public/invites/{token} — strips token,
    drops org-only fields, adds derived booleans for the frontend."""

    tournament_id: str
    tournament_name: str
    tournament_team_id: str
    team_name: str
    organizer_account_name: str
    email: str
    email_has_existing_user: bool
    status: InvitationStatus
    expires_at: datetime


class AcceptInvitationRequest(CamelModel):
    password: Optional[str] = None  # required only on the unauthenticated path
    name: Optional[str] = None  # captured on the unauthenticated path; ignored when JWT auth is used


class AcceptInvitationResponse(CamelModel):
    account_id: str
    tournament_id: str
    tournament_team_id: str
    access_token: Optional[str] = None   # only set on new-user path
    refresh_token: Optional[str] = None  # only set on new-user path
    id_token: Optional[str] = None       # only set on new-user path
