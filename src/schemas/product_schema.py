from typing import List, Optional
from pydantic import BaseModel

class ProductResponse(BaseModel):
    product_id: int
    product_desc: str
    product_class_code: int
    product_price: float
    prouct_quantity_avail: int
    len: float
    width: float
    height: float
    weight: float

class PaginatedProductsResponse(BaseModel):
    total: int
    start: int
    end: int
    data: List[ProductResponse]

class ProductClassResponse(BaseModel):
    product_class_code: int
    product_class_desc: Optional[str] = None

