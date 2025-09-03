from fastapi import APIRouter, Depends, HTTPException, status
from service.product_transformer import ProductTransformer
from schemas.product_schema import ProductResponse, PaginatedProductsResponse, ProductClassResponse
from typing import List, Optional

router = APIRouter(prefix="/products", tags=["Products"])

@router.get("/", response_model=List[ProductResponse])
def get_all_products():
    """Retrieves all products."""
    try:
        df = ProductTransformer.list_all_products()
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
        df = ProductTransformer.get_products_class()
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
        # Better to have a dedicated transformer method for a single item lookup
        product = ProductTransformer.get_product_by_id(product_id)
        
        # IMPROVEMENT: Handle 'not found' case properly
        if not product:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Product with ID {product_id} not found."
            )
        
        return product # Assumed to be a dictionary or Pydantic model
    except HTTPException as http_exc:
        # Re-raise the HTTPException
        raise http_exc
    except Exception as e:
        # Catch any other unexpected errors and return a 500 status code
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An unexpected error occurred: {e}"
        )


@router.get("/class/{class_id}", response_model=PaginatedProductsResponse)
def get_products_in_class(
    class_id: int,
    skip: int = 0,
    limit: int = 100
):
    """
    Retrieves a paginated list of products within a specific class.
    """
    try:
        # Assumed your transformer now takes `skip` and `limit`
        products_df = ProductTransformer.list_products_in_class_paginated(
            class_id,
            skip=skip,
            limit=limit
        )
        
        # Get the total count for the entire class (without pagination)
        total_count = ProductTransformer.get_class_product_count(class_id)
        
        return {
            "total": total_count,
            "start": skip,
            "end": skip + len(products_df),
            "data": products_df.to_dict(orient="records")
        }
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An unexpected error occurred: {e}"
        )