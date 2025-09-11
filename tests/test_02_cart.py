import pytest
import db.dbutil
import db.dbsql
import inspect
from dao.base_transformer import BaseDBTransformer
import db.constants as C
from sqlalchemy.exc import IntegrityError
from service.product_service import ProductService
from service.cart_service import CartService
from dao.product_transformer import ProductTransformer
import pandas as pd
import numpy as np
import pandas.testing as pdt

@pytest.mark.parametrize("cust_id", range(5, 50, 15))
def test_2030_empty_cart( cust_id: int, debug=False):
    print(f"{inspect.currentframe().f_code.co_name}")
    initial_cart = CartService.empty_cart(cust_id)
    prds_5 = ProductService.getRandomProducts(5)
    selected_items = prds_5.loc[:, [C.pid]]
    selected_items[C.qnt] = np.random.randint(1, 3, size=len(selected_items))
    items = selected_items.to_dict(orient='records')
    CartService.addToCart(cust_id, items, True)
    cart_cid =  CartService.empty_cart(cust_id)
    if( (initial_cart is None) & (cart_cid is None) ):
        assert(initial_cart == cart_cid)
    else:
        assert(False)
    if(debug):
        print(f"customer id = {cust_id} - Success empty cart assertion is True.")

@pytest.mark.parametrize("cust_id", range(5, 50, 15))
def test_2000_addto_cart(cust_id: int, debug=False ):
    print(f"{inspect.currentframe().f_code.co_name}")
    # if( db_mode == "fake"):
    #     assert(True)
    #     return
    prds_10 = ProductService.getRandomProducts(10)

    prds_5 = prds_10.iloc[0:5]
    selected_items_1 = prds_5.loc[:, [C.pid]]
    selected_items_1[C.qnt] = np.random.randint(1, 3, size=len(selected_items_1))
    items = selected_items_1.to_dict(orient='records')
    CartService.addToCart(cust_id, items, True)

    prds_5 = prds_10.iloc[5:11]
    selected_items_2 = prds_5.loc[:, [C.pid]]
    selected_items_2[C.qnt] = np.random.randint(1, 3, size=len(selected_items_2))
    items = selected_items_2.to_dict(orient='records')
    CartService.addToCart(cust_id, items, True)

    selected_items = pd.concat([selected_items_1, selected_items_2], ignore_index=True)
    if(debug):
        print(selected_items)
    cart_cid = CartService.get_customer_cart(cust_id)
    if(debug):
        print(cart_cid)
    pdt.assert_frame_equal(
        selected_items.sort_values(by=list(selected_items.columns)).reset_index(drop=True),
        cart_cid.loc[:, list(selected_items.columns)].sort_values(by=list(selected_items.columns)).reset_index(drop=True),
        check_dtype=False
    )
    if(debug):
        print("Success add To cart and verification of cart elements.")

@pytest.mark.parametrize("cust_id", range(5, 50, 15))
def test_2020_overwrite_cart( cust_id: int, debug=True):
    print(f"{inspect.currentframe().f_code.co_name}")
    prds_5 = ProductService.getRandomProducts(5)
    selected_items = prds_5.loc[:, [C.pid]]
    selected_items[C.qnt] = np.random.randint(1, 3, size=len(selected_items))
    items = selected_items.to_dict(orient='records')
    CartService.updateCart(cust_id, items, True)
    cart_cid = CartService.get_customer_cart(cust_id)
    pdt.assert_frame_equal(
        selected_items.sort_values(by=list(selected_items.columns)).reset_index(drop=True),
        cart_cid.loc[:, list(selected_items.columns) ].sort_values(by=list(selected_items.columns)).reset_index(drop=True),
        check_dtype=False
    )
    if(debug):
        print("Success Update cart (after adding to cart) and verification of cart elements getting overwritten.")


@pytest.mark.parametrize("cust_id", range(5, 50, 15))
def test_2010_get_cart_items(cust_id: int, debug=False):
    
    print(f"{inspect.currentframe().f_code.co_name}")
    cart_aggs = []
    customers = BaseDBTransformer.read(C.custs)
    for _, (_, row) in enumerate(customers.iterrows()):
        cust_id = row[C.custid]
        cart_cid = CartService.get_customer_cart(cust_id)
        cart_aggs.append(cart_cid)
    cart_agg_df = pd.concat(cart_aggs, ignore_index=True)
    cart_all = CartService.list_all()
    if (debug):
        print( " Lengths of cart dfs to compare [agg:ori] = ", len(cart_agg_df), ":", len(cart_all))
    assert (len(cart_agg_df) == len(cart_all))
    pdt.assert_frame_equal(
        cart_agg_df.sort_values(by=list(cart_agg_df.columns)).reset_index(drop=True),
        cart_all.sort_values(by=list(cart_all.columns)).reset_index(drop=True),
        check_dtype=False
    )
    if(debug):
        print("Success - aggregation of customer carts to overall cart\n")