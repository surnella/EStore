from fastapi import APIRouter, Depends, HTTPException, status
from service.product_service import ProductService
from schemas.product_schema import ProductResponse, PaginatedProductsResponse, ProductClassResponse
from typing import List, Optional

router = APIRouter(prefix="/products", tags=["Products"])

@router.get("/", response_model=List[ProductResponse])
def get_all_products():
    """Retrieves all products."""
    try:
        print("Entered Products get entry point for get all products ")
        df = ProductService.list_all_products()
        print ( len(df), "\n", df.iloc[0,:])
        return df.to_dict(orient="records")
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An unexpected error occurred: {e}"
    )


@router.get("/classes", response_model=List[ProductClassResponse])
def list_product_classes():
    """Lists all available product classes."""
    try:
        df = ProductService.get_products_class()
        return df.to_dict(orient="records")
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An unexpected error occurred: {e}"
        )


@router.get("/{product_id}", response_model=ProductResponse)
def get_product(product_id: int):
    """Retrieves a single product by its ID."""
    try:
        print(" Recevied product id = ", product_id)
        # Better to have a dedicated transformer method for a single item lookup
        product = ProductService.list_all_products(product_id, True)
        dict = product.to_dict(orient="records")[0]

        # IMPROVEMENT: Handle 'not found' case properly
        if not dict:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Product with ID {product_id} not found."
            )
        print( " return value = ", dict )
        return product.to_dict(orient="records")[0] # Assumed to be a dictionary or Pydantic model
    except HTTPException as http_exc:
        # Re-raise the HTTPException
        raise http_exc
    except Exception as e:
        # Catch any other unexpected errors and return a 500 status code
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An unexpected error occurred: {e}"
        )


@router.get("/class/{class_id}", response_model=List[ProductResponse])
def get_products_in_class(class_id: int):
    """
    Retrieves a list of products within a specific class.
    """
    try:
        df = ProductService.list_products_in_class_df(class_id)
        return df.to_dict(orient="records")
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An unexpected error occurred: {e}"
        )