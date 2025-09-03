from service.cart_transformer import CartTransformer
from schemas.cart_schema import CartResponse, CartUpdateRequest, CartItem
from fastapi import APIRouter, Depends, HTTPException, status
from typing import List

router = APIRouter(prefix="/cart", tags=["Cart"])

@router.get("/", response_model=List[CartItem])
def get_cart_items(cust_id: int):
    """
    Retrieves all items in a customer's cart.
    """
    try:
        df = CartTransformer.list_cust(cust_id)
        # Assuming list_cust returns a list of dictionaries that match the CartItem schema
        return df.to_dict(orient="records")
    except Exception as e:
        # If the backend fails, return a proper 500 Internal Server Error
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An error occurred while retrieving cart: {e}"
        )


@router.post("/", response_model=CartResponse)
def add_to_cart(request: CartUpdateRequest):
    """
    Adds a list of items to a customer's cart.
    """
    try: 
        # Pydantic's root validator now handles the validation, so you can remove the for loop
        CartTransformer.addToCart(request.customer_id, request.items, debug=True)
        
        # Correctly return the CartResponse object
        return CartResponse(
            customer_id=request.customer_id,
            message=f"Add to cart successful for customer {request.customer_id}"
        )
    except Exception as e:
        # Catch any backend errors and return a 500 status code
        print(f"Cart addition not successful for customer {request.customer_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An unexpected error occurred: {e}"
        )


@router.post("/update", response_model=CartResponse)
def update_cart(request: CartUpdateRequest):
    """
    Updates a customer's entire cart by replacing existing items.
    """
    try: 
        # The Pydantic validator handles the validation
        CartTransformer.updateCart(request.customer_id, request.items, debug=True)

        return CartResponse(
            customer_id=request.customer_id,
            message=f"Cart update successful for customer {request.customer_id}"
        )
    except Exception as e:
        # Catch any backend errors and return a 500 status code
        print(f"Cart update not successful for customer {request.customer_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An unexpected error occurred: {e}"
        )