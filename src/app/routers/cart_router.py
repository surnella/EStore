from service.cart_service import CartService
from schemas.cart_schema import CartResponse, CartUpdateRequest, CartItem
from fastapi import APIRouter, Depends, HTTPException, status
from typing import List, Union

router = APIRouter(prefix="/cart", tags=["Cart"])

@router.get("/", response_model=Union[List[CartItem],CartResponse])
def get_cart_items(cust_id: int):
    """
    Retrieves all items in a customer's cart.
    """
    try:
        df = CartService.get_customer_cart(cust_id)
        if df.empty:
            return {"CUSTOMER_ID": cust_id, "MESSAGE": "Empty Cart. No items added. "}
        return df.to_dict(orient="records")
    except Exception as e:
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
        # print(request.CUSTOMER_ID)
        # print(request.ITEMS)
        items_as_dicts = [item.model_dump() for item in request.ITEMS]
        retval = CartService.addToCart(request.CUSTOMER_ID, items_as_dicts, debug=True)
        # print( "Outside CArt Service addToCart ", retval)
        if( retval < 0):
            msg=f"Product OR the product Request quantities are not avilable in the Store at this time. "
        else:
            msg=f"Add to cart successful for customer {request.CUSTOMER_ID}"
        return CartResponse(
            CUSTOMER_ID=request.CUSTOMER_ID,
            MESSAGE=msg
        )
    except Exception as e:
        print(f"Cart addition not successful for customer {request.CUSTOMER_ID}: {e}")
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
        items_as_dicts = [item.model_dump() for item in request.ITEMS]
        retval = CartService.updateCart(request.CUSTOMER_ID, items_as_dicts, debug=True)
        if( retval < 0):
            msg=f"Request quantities are not avilable in the Store. Check availbility and add again."
        else:
            msg=f"Cart update successful for customer {request.CUSTOMER_ID}"

        return CartResponse(
            CUSTOMER_ID=request.CUSTOMER_ID,
            MESSAGE=msg
        )
    except Exception as e:
        print(f"Cart update not successful for customer {request.CUSTOMER_ID}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An unexpected error occurred: {e}"
        )