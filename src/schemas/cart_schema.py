from typing import List, Optional
from pydantic import BaseModel, Field, model_validator

class CartResponse(BaseModel):
    CUSTOMER_ID: int
    MESSAGE: str

class CartItem(BaseModel):
    PRODUCT_ID: int
    PRODUCT_QUANTITY: int
    
class CartUpdateRequest(BaseModel):
    CUSTOMER_ID: int
    ITEMS: List[CartItem]

    @model_validator(mode='after') # <-- Use @model_validator with mode='after'
    def validate_quantities(self):
        if self.ITEMS:
            for item in self.ITEMS:
                if item.PRODUCT_QUANTITY <= 0:
                    raise ValueError(f"Quantity for product {item.PRODUCT_ID} must be greater than 0")
        return self # <-- must return self