from typing import List, Optional
from pydantic import BaseModel, Field, model_validator

class CartResponse(BaseModel):
    customer_id: int
    message: str

class CartItem(BaseModel):
    product_id: int
    product_quantity: int
    
class CartUpdateRequest(BaseModel):
    customer_id: int
    items: List[CartItem]

    @model_validator(mode='after') # <-- Use @model_validator with mode='after'
    def validate_quantities(self):
        if self.items:
            for item in self.items:
                if item.product_quantity <= 0:
                    raise ValueError(f"Quantity for product {item.product_id} must be greater than 0")
        return self # <-- must return self