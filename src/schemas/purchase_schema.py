from typing import List, Optional
from pydantic import BaseModel, Field

class PurchaseResponse(BaseModel):
    ORDER_ID: int
    MESSAGE: str

# Schema for the whole cart update request
class PurchaseDRequest(BaseModel):
    CUSTOMER_ID: int
    DISCOUNT_ID: str

# Schema for the whole cart update request
class PurchaseRequest(BaseModel):
    CUSTOMER_ID: int