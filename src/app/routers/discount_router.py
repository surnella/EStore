from service.discount_service import DiscountService
from schemas.discount_schema import DiscountResponse, DiscountRequest, DiscountUpdateRequest, DiscountUpdateResponse, EmptyDiscountResponse
from fastapi import APIRouter, HTTPException, status
from typing import Any, Dict, Optional, List, Union

router = APIRouter(prefix="/discount", tags=["Discount"])

@router.get("/all", response_model=Union[List[DiscountResponse], EmptyDiscountResponse])
def get_all_codes():
    """
    Retrieves all discounts for all customer's. It gives used, eligible and active
    """
    try:
        df = DiscountService.list_all_codes()
        if df.empty:
            return {"DISCOUNT_ID": "N/A", "MESSAGE": "No discounts are available (used, active or eligible) in EStore."}
        return df.to_dict(orient="records")
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An error occurred while getting discount codes: {e}"
        )

@router.get("/eligible/{cust_id}", response_model=Union[List[DiscountResponse], EmptyDiscountResponse])
def eligible_codes(cust_id: int):
    """
    Retrieves all discounts which the customer is eligible but noy yet active.
    """
    try:
        df = DiscountService.list_eligible_codes(cust_id)
        if df.empty:
            return {"DISCOUNT_ID": "N/A", "MESSAGE": "Customer has no eligible codes which can be turned active."}
        return df.to_dict(orient="records")
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An error occurred while getting discount codes: {e}"
        )

@router.get("/used/{cust_id}", response_model=Union[List[DiscountResponse], EmptyDiscountResponse])
def used_codes(cust_id: int):
    """
    Retrieves all items used by the customer in purchases to avail discount.
    """
    try:
        df = DiscountService.list_used_codes(cust_id)
        if df.empty:
            return {"DISCOUNT_ID": "N/A", "MESSAGE": "Customer has not used any codes so far."}
        return df.to_dict(orient="records")

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An error occurred while getting discount codes: {e}"
        )

@router.get("/active", response_model=Union[List[DiscountResponse], EmptyDiscountResponse])
def active_codes():
    """
    Retrieves all discounts which are active but not yet used by the (all) customers. 
    """
    try:
        df = DiscountService.list_active_codes()
        if df.empty:
            return {"DISCOUNT_ID": "N/A", "MESSAGE": "No active discounts available for any customers"}
        return df.to_dict(orient="records")
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An error occurred while getting discount codes: {e}"
        )

@router.post("/update", response_model=DiscountUpdateResponse)
def enable_code(request: DiscountUpdateRequest):
    """
    Converts an Eligible code to active. 
    """
    try: 
        print(request)
        msg = "Discount ID update successful for customer"
        retval = DiscountService.enable_discount_codes(request.DISCOUNT_ID, 
                    request.DISCOUNT_PERCENT, request.DISCOUNT_CODE, 0, debug=True)
        if( retval is None ):
            msg = "Discount ID does not exist or is not eligible for activation"
        print( " Returned from discount after update percent = ", request.DISCOUNT_PERCENT, request.DISCOUNT_CODE, retval)
        return DiscountUpdateResponse(
            DISCOUNT_ID=request.DISCOUNT_ID,
            MESSAGE=f"{msg} - {request.DISCOUNT_ID}"
        )
    except Exception as e:
        print(f"Discount update not successful for customer {request.DISCOUNT_ID}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An unexpected error occurred: {e}"
        )