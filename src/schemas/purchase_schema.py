from typing import List, Optional
from pydantic import BaseModel, Field

class PurchaseResponse(BaseModel):
    order_id: int
    message: str

# Schema for the whole cart update request
class PurchaseDRequest(BaseModel):
    customer_id: int
    discount_id: int

# Schema for the whole cart update request
class PurchaseRequest(BaseModel):
    customer_id: int