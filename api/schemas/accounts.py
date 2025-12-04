from pydantic import BaseModel


class AccountSettings(BaseModel):
    default_workspace: str | None = "main"
    timezone: str | None = "America/Bogota"
    language: str | None = "es"
    currency: str | None = "COP"


class AccountSubscription(BaseModel):
    plan: str | None = "free"
    status: str | None = "active"
    trial_end: str | None = None


class AccountBranding(BaseModel):
    logo_url: str | None = None
    primary_color: str | None = None
    secondary_color: str | None = None


class CreateAccount(BaseModel):
    id: str
    name: str
    settings: AccountSettings | None = None
    subscription: AccountSubscription | None = None
    branding: AccountBranding | None = None


class UpdateAccount(BaseModel):
    name: str | None = None
    settings: AccountSettings | None = None
    subscription: AccountSubscription | None = None
    branding: AccountBranding | None = None
