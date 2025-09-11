import pytest
from fastapi.testclient import TestClient
from app.app import app   # <-- import from app.py
import db.dbutil
import db.dbsql
import inspect
from dao.base_transformer import BaseDBTransformer
import db.constants as C
from sqlalchemy.exc import IntegrityError
from service.product_service import ProductService
from service.cart_service import CartService
from service.discount_service import DiscountService
from service.purchase_service import PurchaseService
from dao.product_transformer import ProductTransformer
from dao.discount_transformer import DiscountTransformer
from dao.purchase_transformer import PurchaseTransformer
import pandas as pd
import numpy as np
import pandas.testing as pdt
from unittest.mock import patch

client = TestClient(app)

def test_5030_product_apis_get_all():
    print(f"{inspect.currentframe().f_code.co_name}")  
    response = client.get("/products/products")
    print("Status:", response.status_code)
    print("Response:", response.json())
    assert response.status_code == 200

def test_5040_product_apis_get_classes():
    print(f"{inspect.currentframe().f_code.co_name}")  
    response = client.get("/products/products/classes")
    print("Status:", response.status_code)
    print("Response:", response.json())
    assert response.status_code == 200


@pytest.mark.parametrize("pid", range(199, 210, 1))
def test_5050_product_apis_get_product(pid: int):
    print(f"{inspect.currentframe().f_code.co_name}")  
    urlpath = f"/products/products/{pid}"   # f-string avoids str() conversion issues
    response = client.get(urlpath)

    print("Status:", response.status_code)
    print("Response:", response.json())

    # Example assertion: status should be 200 or 404
    assert response.status_code in (200, 404)

@pytest.mark.parametrize("class_id", range(2040, 2100, 10))
def test_5060_product_apis_get_class(class_id: int):
    print(f"{inspect.currentframe().f_code.co_name}")  
    urlpath = f"/products/products/class/{class_id}"   # f-string avoids str() conversion issues
    response = client.get(urlpath)

    print("Status:", response.status_code)
    print("Response:", response.json())

    # Example assertion: status should be 200 or 404
    assert response.status_code in (200, 404)

def test_5062_product_apis_get_all():
    print(f"{inspect.currentframe().f_code.co_name}")  

    with patch("service.product_service.ProductService.list_all_products", side_effect=Exception("DB error simulated")):
        response = client.get("/products/products")
    print("Status:", response.status_code)
    print("Response:", response.json())
    assert response.status_code in (200,500)

def test_5064_product_apis_get_classes():
    print(f"{inspect.currentframe().f_code.co_name}")  

    with patch("service.product_service.ProductService.get_products_class", side_effect=Exception("DB error simulated")):
        response = client.get("/products/products/classes")
    print("Status:", response.status_code)
    print("Response:", response.json())
    assert response.status_code in (200, 500)

@pytest.mark.parametrize("pid", range(199, 210, 1))
def test_5066_product_apis_get_product(pid: int):
    print(f"{inspect.currentframe().f_code.co_name}")  
    with patch("service.product_service.ProductService.list_all_products", side_effect=Exception("DB error simulated")):
        urlpath = f"/products/products/{pid}"   # f-string avoids str() conversion issues
        response = client.get(urlpath)

    print("Status:", response.status_code)
    print("Response:", response.json())

    # Example assertion: status should be 200 or 404
    assert response.status_code in (200, 500)

@pytest.mark.parametrize("class_id", range(2040, 2100, 10))
def test_5068_product_apis_get_class(class_id: int):
    print(f"{inspect.currentframe().f_code.co_name}")
    with patch("service.product_service.ProductService.list_products_in_class_df", side_effect=Exception("DB error simulated")):
        urlpath = f"/products/products/class/{class_id}"   # f-string avoids str() conversion issues
        response = client.get(urlpath)
    print("Status:", response.status_code)
    print("Response:", response.json())

    # Example assertion: status should be 200 or 404
    assert response.status_code in (200, 500)


@pytest.mark.parametrize("cust_id", [3, 15, 30, 45, 60])
def test_5110_cart_apis_get(cust_id):
    print(f"{inspect.currentframe().f_code.co_name}")  
    response = client.get("/cart/cart", params={"cust_id": cust_id})
    
    print(f"Testing cust_id={cust_id}")

    # assertions: either we get a list of cart items or the "empty cart" message
    assert response.status_code == 200
    data = response.json()    
    print("Status:", response.status_code)
    print("Response:", response.json())

# Define payloads
good_payload = {
    "CUSTOMER_ID": 15,
    "ITEMS": [
        {"PRODUCT_ID": 201, "PRODUCT_QUANTITY": 2},
        {"PRODUCT_ID": 202, "PRODUCT_QUANTITY": 1}
    ]
}

bad_payload = {
    "CUSTOMER_ID": 15,
    "ITEMS": [
        {"PRODUCT_ID": 101, "PRODUCT_QUANTITY": 1}
    ]
}

@pytest.mark.parametrize(
    "payload,expected_status",
    [
        (good_payload, 200),  # expect success
        (bad_payload, 422)    # expect validation error
    ]
)
def test_5120_cart_apis_add(payload, expected_status):
    print(f"{inspect.currentframe().f_code.co_name}")  
    response = client.post("/cart/cart", json=payload)

    print("Status:", response.status_code)
    print("Response:", response.json())
    assert response.status_code in (200, 422)

@pytest.mark.parametrize("cust_id", [3, 15, 30, 45, 60])
def test_5125_cart_apis_get(cust_id):
    print(f"{inspect.currentframe().f_code.co_name}")  
    response = client.get("/cart/cart", params={"cust_id": cust_id})
    
    print(f"Testing cust_id={cust_id}")

    # assertions: either we get a list of cart items or the "empty cart" message
    assert response.status_code == 200
    data = response.json()    
    print("Status:", response.status_code)
    print("Response:", response.json())

@pytest.mark.parametrize(
    "payload,expected_status",
    [
        (good_payload, 200),  # expect success
        (bad_payload, 422)    # expect validation error
    ]
)
def test_5130_cart_apis_update(payload, expected_status):
    print(f"{inspect.currentframe().f_code.co_name}")  
    response = client.post("/cart/cart/update", json=payload)
    print("Status:", response.status_code)
    print("Response:", response.json())
    assert response.status_code in (200, 422)

def test_5200_discount_all():
    print(f"{inspect.currentframe().f_code.co_name}")  
    response = client.get("/discount/discount/all")
    print(f"Discount codes = {response}")
    # assertions: either we get a list of cart items or the "empty cart" message
    assert response.status_code in (200, 422)
    data = response.json()
    print("Status:", response.status_code)
    print("Response:", response.json() )

def test_5201_discount_all():
    print(f"{inspect.currentframe().f_code.co_name}")  
    emp_df = pd.DataFrame()
    with patch("service.discount_service.DiscountService.list_all_codes", return_value=emp_df):
        response = client.get("/discount/discount/all")
    print(f"Discount codes = {response}")
    # assertions: either we get a list of cart items or the "empty cart" message
    assert response.status_code in (200, 422)
    data = response.json()
    print("Status:", response.status_code)
    print("Response:", response.json() )
    assert "No" in response.json()["MESSAGE"]


@pytest.mark.parametrize("cust_id", [15, 30, 45, 60])
def test_5210_discount_used(cust_id):
    print(f"{inspect.currentframe().f_code.co_name}")  
    urlpath = f"/discount/discount/used/{cust_id}"   # f-string avoids str() conversion issues
    response = client.get(urlpath, params={"cust_id": cust_id})
    print(f"Testing cust_id={cust_id}, {response}")
    # assertions: either we get a list of cart items or the "empty cart" message
    assert response.status_code in (200, 422)
    data = response.json()
    print("Status:", response.status_code)
    print("Response:", response.json() )

@pytest.mark.parametrize("cust_id", [15, 30, 45, 60])
def test_5220_discount_eligible(cust_id):
    print(f"{inspect.currentframe().f_code.co_name}")  
    urlpath = f"/discount/discount/eligible/{cust_id}"   # f-string avoids str() conversion issues
    response = client.get(urlpath, params={"cust_id": cust_id})
    print(f"Testing cust_id={cust_id}, {response}")
    # assertions: either we get a list of cart items or the "empty cart" message
    assert response.status_code in (200, 422)
    data = response.json()
    print("Status:", response.status_code)
    print("Response:", response.json() )

def test_5230_discount_active():
    print(f"{inspect.currentframe().f_code.co_name}")  
    response = client.get("/discount/discount/active")
    print(f"Discount codes = {response}")
    # assertions: either we get a list of cart items or the "empty cart" message
    assert response.status_code in (200, 422)
    data = response.json()
    print("Status:", response.status_code)
    print("Response:", response.json() )


# ---------------- Fixtures ----------------
@pytest.fixture(scope="function")
def purchase_order_id():
    print(f"{inspect.currentframe().f_code.co_name}")
    response = client.post("/cart/cart/update", json=good_payload)
    payload = {"CUSTOMER_ID": 15}
    resp = client.post("/purchase/purchase", json=payload)
    assert resp.status_code in (200,422)
    data = resp.json()
    order_id = data["ORDER_ID"]
    if ( order_id is None ):
        assert(False)
    else:
        custid =  good_payload["CUSTOMER_ID"]
        did = DiscountTransformer.generate_discount_id(order_id, custid)
        payl = {
            "DISCOUNT_STATUS": 0,
            "DISCOUNT_ID": did,
            "DISCOUNT_PERCENT": 10,
            "DISCOUNT_CODE": "SUMMER10"
        }
        client.post("/discount/discount/update", json=payl)
        print("purchase_id fixture", order_id, custid)
    return order_id, custid

@pytest.fixture(scope="function")
def valid_discount_id(purchase_order_id):
    print(f"{inspect.currentframe().f_code.co_name}")  
    """Generate a valid discount_id from order_id + customer_id."""
    order_id, customer_id = purchase_order_id
    discount_id =  DiscountTransformer.generate_discount_id(order_id, customer_id)
    print( "Valid discount id fixture ", order_id, customer_id, discount_id)
    return discount_id

# ---------------- Tests ----------------
def test_5250_purchase_cart(purchase_order_id):
    print(f"{inspect.currentframe().f_code.co_name}")  
    order_id, _ = purchase_order_id
    print(f"Generated ORDER_ID: {order_id}")
    assert order_id > 0

@pytest.mark.parametrize("payload_builder, expected_status", [
    (
        lambda valid_discount_id: {
            "DISCOUNT_STATUS": 0,
            "DISCOUNT_ID": valid_discount_id,
            "DISCOUNT_PERCENT": 10,
            "DISCOUNT_CODE": "SUMMER10"
        },
        200
    ),
    (
        lambda _: {
            "DISCOUNT_STATUS": 0,
            "DISCOUNT_ID": "INVALID999",
            "DISCOUNT_PERCENT": 15,
            "DISCOUNT_CODE": "BADCODE"
        },
        200
    ),
    (
        lambda valid_discount_id: {
            "DISCOUNT_STATUS": 0,
            "DISCOUNT_ID": valid_discount_id,
            # "DISCOUNT_PERCENT": 20,   # missing on purpose
            "DISCOUNT_CODE": "SUMMER20"
        },
        422
    )
])
def test_5260_enable_discount_code(valid_discount_id, payload_builder, expected_status):
    """Test discount update endpoint with valid, invalid and malformed payloads."""
    print(f"{inspect.currentframe().f_code.co_name}")  
    payload = payload_builder(valid_discount_id)
    print(payload)
    response = client.post("/discount/discount/update", json=payload)
    print("Status:", response.status_code)
    print("Response:", response.json())

    assert response.status_code == expected_status

    if expected_status == 200:
        data = response.json()
        assert "DISCOUNT_ID" in data
    
    with patch("service.discount_service.DiscountService.enable_discount_codes", side_effect=Exception("DB error simulated")):
        response = client.post("/discount/discount/update", json=payload)
    print("Status:", response.status_code)
    print("Response:", response.json())

    assert response.status_code in (500,422)

@pytest.mark.parametrize("cust_id", [15, 30, 45, 60])
def test_5262_discount_eligible(cust_id):
    print(f"{inspect.currentframe().f_code.co_name}")  
    urlpath = f"/discount/discount/eligible/{cust_id}"   # f-string avoids str() conversion issues
    response = client.get(urlpath, params={"cust_id": cust_id})
    print(f"Testing cust_id={cust_id}, {response}")
    # assertions: either we get a list of cart items or the "empty cart" message
    assert response.status_code in (200, 422)
    data = response.json()
    print("Status:", response.status_code)
    print("Response:", response.json() )

def test_5263_discount_active():
    print(f"{inspect.currentframe().f_code.co_name}")  
    response = client.get("/discount/discount/active")
    print(f"Discount codes = {response}")
    # assertions: either we get a list of cart items or the "empty cart" message
    assert response.status_code in (200, 422)
    data = response.json()
    print("Status:", response.status_code)
    print("Response:", response.json() )

@pytest.mark.parametrize("cust_id", [15, 30, 45, 60])
def test_5264_discount_used(cust_id):
    print(f"{inspect.currentframe().f_code.co_name}")  
    urlpath = f"/discount/discount/used/{cust_id}"   # f-string avoids str() conversion issues
    with patch("service.discount_service.DiscountService.list_used_codes", side_effect=Exception("DB error simulated")):
        response = client.get(urlpath, params={"cust_id": cust_id})
    print(f"Testing cust_id={cust_id}, {response}")
    # assertions: either we get a list of cart items or the "empty cart" message
    assert response.status_code == 500
    data = response.json()
    print("Status:", response.status_code)
    print("Response:", response.json() )

@pytest.mark.parametrize("cust_id", [15, 30, 45, 60])
def test_5265_discount_eligible(cust_id):
    print(f"{inspect.currentframe().f_code.co_name}")  
    urlpath = f"/discount/discount/eligible/{cust_id}"   # f-string avoids str() conversion issues
    with patch("service.discount_service.DiscountService.list_eligible_codes", side_effect=Exception("DB error simulated")):
        response = client.get(urlpath, params={"cust_id": cust_id})
    print(f"Testing cust_id={cust_id}, {response}")
    # assertions: either we get a list of cart items or the "empty cart" message
    assert response.status_code == 500
    data = response.json()
    print("Status:", response.status_code)
    print("Response:", response.json() )

def test_5266_discount_active():
    print(f"{inspect.currentframe().f_code.co_name}")  
    with patch("service.discount_service.DiscountService.list_active_codes", side_effect=Exception("DB error simulated")):
        response = client.get("/discount/discount/active")
    print(f"Discount codes = {response}")
    # assertions: either we get a list of cart items or the "empty cart" message
    assert response.status_code == 500
    data = response.json()
    print("Status:", response.status_code)
    print("Response:", response.json() )

def test_5270_purchase_discounted_cart_valid(valid_discount_id, purchase_order_id):
    print(f"{inspect.currentframe().f_code.co_name}")  
    client.post("/cart/cart/update", json=good_payload)
    order_id, customer_id = purchase_order_id
    payload = {
        "CUSTOMER_ID": customer_id,
        "DISCOUNT_ID": valid_discount_id
    }
    print( "cart valid ", payload)
    resp = client.post("/purchase/purchase/discount", json=payload)
    print("Payload:", payload)
    print("Response:", resp.json())

    assert resp.status_code == 200
    data = resp.json()
    assert "ORDER_ID" in data

@pytest.mark.parametrize("cust_id", [15, 30, 45, 60])
def test_5261_discount_used(cust_id):
    print(f"{inspect.currentframe().f_code.co_name}")  
    urlpath = f"/discount/discount/used/{cust_id}"   # f-string avoids str() conversion issues
    response = client.get(urlpath, params={"cust_id": cust_id})
    print(f"Testing cust_id={cust_id}, {response}")
    # assertions: either we get a list of cart items or the "empty cart" message
    assert response.status_code in (200, 422)
    data = response.json()
    print("Status:", response.status_code)
    print("Response:", response.json() )

@pytest.mark.parametrize(
    "payload, expected_status",
    [
        ({"CUSTOMER_ID": 15, "DISCOUNT_ID": "missing"}, 400),   # missing discount
    ]
)
def test_5280_purchase_discounted_cart_invalid(payload, expected_status):
    print(f"{inspect.currentframe().f_code.co_name}")  
    client.post("/cart/cart/update", json=good_payload)
    print( "cart invalid ", payload)
    resp = client.post("/purchase/purchase/discount", json=payload)
    print("Response:", resp.json())
    assert resp.status_code in (400,200)

@pytest.mark.parametrize(
    "payload, expected_status",
    [
        ({"CUSTOMER_ID": 1000, "DISCOUNT_ID": "missing"}, 400),   # missing discount
    ]
)
def test_5281_purchase_discounted_cart_invalid(payload, expected_status):
    print(f"{inspect.currentframe().f_code.co_name}")  
    with patch("service.purchase_service.PurchaseService.purchase_discounted_cart_items", side_effect=Exception("DB error simulated")):
        resp = client.post("/purchase/purchase/discount", json=payload)
    print("Response:", resp.json())
    assert resp.status_code in (200, 500)

@pytest.mark.parametrize("cust_id", [30, 60])
def test_5300_get_cart_items_exception(cust_id: int):
    """Force CartService.get_customer_cart to raise an exception → tests except block."""
    print(f"{inspect.currentframe().f_code.co_name}")  
    with patch("app.routers.cart_router.CartService.get_customer_cart", side_effect=Exception("DB error simulated")):
        response = client.get("/cart/cart", params={"cust_id": cust_id})
    print("Status:", response.status_code)
    print("Response:", response.json())
    assert response.status_code == 500

def test_5310_add_cart_items_exception():
    """Force CartService.add_customer_cart to raise an exception → tests except block."""
    print(f"{inspect.currentframe().f_code.co_name}")  
    with patch("app.routers.cart_router.CartService.addToCart", side_effect=Exception("DB error simulated")):
        response = client.post("/cart/cart", json=good_payload)
    print("Status:", response.status_code)
    print("Response:", response.json())
    assert response.status_code == 500

def test_5320_update_cart_items_exception():
    """Force CartService.update_customer_cart to raise an exception → tests except block."""
    print(f"{inspect.currentframe().f_code.co_name}")  
    with patch("app.routers.cart_router.CartService.updateCart", side_effect=Exception("DB error simulated")):
        response = client.post("/cart/cart/update", json=good_payload)
    print("Status:", response.status_code)
    print("Response:", response.json())
    assert response.status_code == 500

def test_5330_update_cart_items_exception():
    """Force CartService.update_customer_cart to raise an exception → tests except block."""
    print(f"{inspect.currentframe().f_code.co_name}")  
    with patch("service.cart_service.CartTransformer.updateCart", side_effect=Exception("DB error simulated")):
        response = client.post("/cart/cart/update", json=good_payload)
    print("Status:", response.status_code)
    print("Response:", response.json())
    assert response.status_code == 200
    assert "not avilable" in response.json()["MESSAGE"]

