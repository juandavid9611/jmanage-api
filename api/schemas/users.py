import decimal
from enum import Enum
from pydantic import BaseModel


class PutUserMetrics(BaseModel):
    asistencia_entrenos: int
    asistencia_partidos: int
    puntualidad_pagos: int
    llegadas_tarde: int
    deuda_acumulada: int
    total: int
    puntaje_asistencia: decimal.Decimal
    puntaje_asistencia_description: str
    last_update: str | None = None

class PutUserAvatar(BaseModel):
    avatar_url: str

class UserStatus(str, Enum):
    ACTIVE = "active"
    DISABLED = "disabled"

class UserConfirmationStatus(str, Enum):
    CONFIRMED = "confirmed"
    PENDING = "pending"

class CreateUser(BaseModel):
    id: str
    name: str
    email: str
    accountId: str


class PutTourPreferences(BaseModel):
    tourKey: str


class PutUser(BaseModel):
    id: str
    name: str
    identityCardNumber: str
    email: str
    phoneNumber: str
    country: str
    city: str
    address: str
    rh: str
    eps: str
    emergencyContactName: str
    emergencyContactPhoneNumber: str
    emergencyContactRelationship: str
    status: str
    shirtNumber: str
    

from pydantic import ConfigDict, Field
from core.casing import camel_alias


class BulkUserStatusRequest(BaseModel):
    """Body for POST /users/bulk-status (camelCase on the wire)."""
    model_config = ConfigDict(alias_generator=camel_alias, populate_by_name=True)

    user_ids: list[str] = Field(min_length=1, max_length=200)
    disabled: bool
