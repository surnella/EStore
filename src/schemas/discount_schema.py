from typing import List, Optional
from pydantic import BaseModel, Field

class DiscountResponse(BaseModel):
    DISCOUNT_ID: str
    DISCOUNT_CODE: str
    DISCOUNT_STATUS: int
    DISCOUNT_PERCENT: int
    ORDER_ID: int
    CUSTOMER_ID: int

# Schema for the whole cart update request
class DiscountUpdateRequest(BaseModel):
    DISCOUNT_STATUS: int
    DISCOUNT_ID: str
    DISCOUNT_PERCENT: int
    DISCOUNT_CODE: str

class DiscountUpdateResponse(BaseModel):
    DISCOUNT_ID: str
    MESSAGE: str

class DiscountRequest(BaseModel):
    DISCOUNT_ID: str