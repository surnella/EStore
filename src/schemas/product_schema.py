from typing import List, Optional
from pydantic import BaseModel

class ProductResponse(BaseModel):
    PRODUCT_ID: int
    PRODUCT_DESC: str
    PRODUCT_CLASS_CODE: int
    PRODUCT_PRICE: float
    PRODUCT_QUANTITY_AVAIL: int
    LEN: float
    WIDTH: float
    HEIGHT: float
    WEIGHT: float

class PaginatedProductsResponse(BaseModel):
    TOTAL: int
    START: int
    END: int
    DATA: List[ProductResponse]

class ProductClassResponse(BaseModel):
    PRODUCT_CLASS_CODE: int
    PRODUCT_CLASS_DESC: Optional[str] = None
