from service.discount_transformer import DiscountTransformer
from schemas.discount_schema import DiscountResponse, DiscountRequest, DiscountUpdateRequest, DiscountUpdateResponse
from fastapi import APIRouter, HTTPException, status
from typing import Any, Dict, Optional, List

router = APIRouter(prefix="/Discount", tags=["Discount"])

@router.get("/all", response_model=List[DiscountResponse])
def get_all_codes():
    """
    Retrieves all items in a customer's cart.
    """
    try:
        df = DiscountTransformer.list_all_codes()
        # Assuming list_cust returns a list of dictionaries that match the CartItem schema
        return df.to_dict(orient="records")
    except Exception as e:
        # If the backend fails, return a proper 500 Internal Server Error
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An error occurred while getting discount codes: {e}"
        )

@router.get("/eligible/{cust_id}", response_model=List[DiscountResponse])
def eligible_codes(cust_id: int):
    """
    Retrieves all items in a customer's cart.
    """
    try:
        df = DiscountTransformer.list_eligible_codes(cust_id)
        # Assuming list_cust returns a list of dictionaries that match the CartItem schema
        return df.to_dict(orient="records")
    except Exception as e:
        # If the backend fails, return a proper 500 Internal Server Error
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An error occurred while getting discount codes: {e}"
        )

@router.get("/used/{cust_id}", response_model=List[DiscountResponse])
def used_codes(cust_id: int):
    """
    Retrieves all items in a customer's cart.
    """
    try:
        df = DiscountTransformer.list_used_codes(cust_id)
        # Assuming list_cust returns a list of dictionaries that match the CartItem schema
        return df.to_dict(orient="records")
    except Exception as e:
        # If the backend fails, return a proper 500 Internal Server Error
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An error occurred while getting discount codes: {e}"
        )

@router.get("/active", response_model=List[DiscountResponse])
def active_codes():
    """
    Retrieves all items in a customer's cart.
    """
    try:
        df = DiscountTransformer.list_active_codes()
        # Assuming list_cust returns a list of dictionaries that match the CartItem schema
        return df.to_dict(orient="records")
    except Exception as e:
        # If the backend fails, return a proper 500 Internal Server Error
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An error occurred while getting discount codes: {e}"
        )

@router.post("/update", response_model=DiscountUpdateResponse)
def enable_code(request: DiscountUpdateRequest):
    """
    Adds a list of items to a customer's cart.
    """
    try: 
        print(request)
        # Pydantic's root validator now handles the validation, so you can remove the for loop
        DiscountTransformer.enable_discount_codes(request.DISCOUNT_ID, 
                    request.DISCOUNT_PERCENT, request.DISCOUNT_CODE, 0, debug=True)
        
        print( " Returned from discount after update percent = ", request.DISCOUNT_PERCENT, request.DISCOUNT_CODE)
        # Correctly return the CartResponse object
        return DiscountUpdateResponse(
            DISCOUNT_ID=request.DISCOUNT_ID,
            MESSAGE=f"Discount ID update successful for customer {request.DISCOUNT_ID}"
        )
    except Exception as e:
        # Catch any backend errors and return a 500 status code
        print(f"Discount update not successful for customer {request.DISCOUNT_ID}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An unexpected error occurred: {e}"
        )