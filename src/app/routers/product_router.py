from fastapi import APIRouter, Depends, HTTPException
from service.product_transformer import ProductTransformer
from schemas.product_schema import ProductResponse, PaginatedProductsResponse, ProductClassResponse
from typing import List


router = APIRouter(prefix="/products", tags=["Products"])

@router.get("/", response_model=List[ProductResponse])
def get_all_products():
    df = ProductTransformer.list_all_products()
    return df.to_dict(orient="records")

@router.get("/classes", response_model=List[ProductClassResponse])
def list_product_classes():
    df = ProductTransformer.get_products_class()
    mapped = df.to_dict(orient="records")
    return mapped


@router.get("/{product_id}", response_model=ProductResponse)
def get_product(product_id: int):
    df = ProductTransformer.list_all_products(product_id)
    return df.to_dict(orient="records")[0]


# @router.get("/{product_id}", response_model=ProductResponse)
# def get_product(product_id: int):
#     # returns a pandas DataFrame
#     df = ProductTransformer.list_all_products(product_id)
#     if df.empty:
#         raise HTTPException(status_code=404, detail="Product not found")
    
#     # Convert first row to dict and map DB column names -> Pydantic field names
#     product_dict = df.iloc[0].to_dict()
#     mapped = {
#         "product_id": product_dict["PRODUCT_ID"],
#         "product_desc": product_dict["PRODUCT_DESC"],
#         "product_class_code": product_dict["PRODUCT_CLASS_CODE"],
#         "product_price": float(product_dict["PRODUCT_PRICE"]) if product_dict["PRODUCT_PRICE"] is not None else None,
#         "product_quantity_avail": product_dict["PRODUCT_QUANTITY_AVAIL"],
#         "len": product_dict["LEN"],
#         "width": product_dict["WIDTH"],
#         "height": product_dict["HEIGHT"],
#         "weight": float(product_dict["WEIGHT"]) if product_dict["WEIGHT"] is not None else None
#     }
#     return mapped

@router.get("/class/{class_id}", response_model=PaginatedProductsResponse)
def get_products_in_class(class_id: int):
    products = ProductTransformer.list_products_in_class_df(class_id)
    total_count = len(products)

    mapped =  {
        "total": total_count,
        "start": 0,
        "end": total_count,
        "data": products.to_dict(orient="records")
    }
    return mapped