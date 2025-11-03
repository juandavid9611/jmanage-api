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

class PutUser(BaseModel):
    id: str
    name: str
    identityCardNumber: str
    email: str
    phoneNumber: str
    country: str
    city: str
    address: str
    group: str
    rh: str
    eps: str
    emergencyContactName: str
    emergencyContactPhoneNumber: str
    emergencyContactRelationship: str
    status: str
    shirtNumber: str
    