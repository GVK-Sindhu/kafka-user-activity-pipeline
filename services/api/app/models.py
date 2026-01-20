from pydantic import BaseModel
from datetime import date
from uuid import UUID

class UserActivitySummary(BaseModel):
    item_id: UUID
    date: date
    view_count: int
    click_count: int
    purchase_count: int
