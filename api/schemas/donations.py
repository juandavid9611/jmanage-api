from pydantic import Field, model_validator

from core.casing import camel_alias
from pydantic import BaseModel


class CamelModel(BaseModel):
    model_config = {
        "alias_generator": camel_alias,
        "populate_by_name": True,
        "from_attributes": True,
    }


class ContributionCreate(CamelModel):
    donor_name: str | None = None
    amount_cop: int = Field(gt=0)
    message: str | None = None
    anonymous: bool = False

    @model_validator(mode="after")
    def check_donor_name(self):
        if not self.anonymous and not self.donor_name:
            raise ValueError("donorName is required unless anonymous is true")
        return self


class ContributionOut(CamelModel):
    id: str
    donor_name: str | None
    amount_cop: int
    message: str | None
    created_at: int


class DonationSummary(CamelModel):
    total_amount_cop: int
    contribution_count: int
