from typing import List, Optional
from pydantic import BaseModel

class CartResponse(BaseModel):
    PRODUCT_ID: int
    CUSTOMER_ID: int
    PRODUCT_QUANTITY: int