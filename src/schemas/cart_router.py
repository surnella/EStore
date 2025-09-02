from fastapi import APIRouter, Depends, HTTPException
from service.cart_transformer import CartTransformer
from schemas.cart_schema import CartResponse
from typing import List

router = APIRouter(prefix="/cart", tags=["Cart"])

@router.get("/", response_model=CartResponse)
def get_cart_items(cust_id: int):
    df = CartTransformer.list_cust(cust_id)
    return df.to_dict(orient="records")
