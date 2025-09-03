from service.purchase_transformer import PurchaseTransformer
from schemas.purchase_schema import PurchaseResponse, PurchaseRequest, PurchaseDRequest
from fastapi import APIRouter, HTTPException, status
from typing import Any, Dict, Optional

router = APIRouter(prefix="/Purchase", tags=["Purchase"])

# A helper function to reduce code duplication
def _handle_purchase_logic(customer_id: int, discount_id: Optional[int] = None) -> PurchaseResponse:
    """
    Encapsulates the core purchase logic and error handling.
    """
    try:
        if discount_id is not None:
            oid = PurchaseTransformer.purchase_discounted_cart_items(customer_id, discount_id)
        else:
            oid = PurchaseTransformer.purchase_cart_items(customer_id)
            
        return PurchaseResponse(
            order_id=oid,
            message=f"Purchase successful. Order ID: {oid}"
        )
    except Exception as e:
        # Log the detailed error for debugging
        print(f"Purchase transaction failed for customer {customer_id}: {e}")
        # Return a proper 500 Internal Server Error to the client
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An unexpected error occurred during the purchase."
        )


@router.post("/", response_model=PurchaseResponse)
def purchase_cart(purchase_req: PurchaseRequest):
    """
    Purchases the customer's cart items.
    """
    return _handle_purchase_logic(purchase_req.customer_id)


@router.post("/discount", response_model=PurchaseResponse)
def purchase_discounted_cart(purchase_req: PurchaseDRequest):
    """
    Purchases the customer's cart items with a discount applied.
    """
    return _handle_purchase_logic(purchase_req.customer_id, purchase_req.discount_id)