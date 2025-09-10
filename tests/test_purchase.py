import pytest
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

def test_4030_list_orders(debug=False):

    print(f"{inspect.currentframe().f_code.co_name}")  

    custs = BaseDBTransformer.read(C.orders)
    cords = custs[C.custid].unique().tolist()
    o_agg = []
    for c in cords:
        print(c)
        cord = PurchaseService.list_orders_by_customer(c)
        print(cord)
        o_agg.append(cord)
    o_all = PurchaseService.list_all_orders()
    do_agg = pd.concat(o_agg, ignore_index=True)
    pdt.assert_frame_equal (
        do_agg.sort_values(by=list(do_agg.columns)).reset_index(drop=True),
        o_all.loc[:, list(o_all.columns)].sort_values(by=list(o_all.columns)).reset_index(drop=True),
        check_dtype=False
    )
    print("Success Verified orders listing before Purchase")


@pytest.mark.parametrize("cust_id", range(5, 50, 15))
def test_4050_purchase( cust_id: int, debug=False):

    print(f"{inspect.currentframe().f_code.co_name}")  

    # Get Products to buy -- Step 1 
    products_to_buy = ProductService.getRandomProducts()

    avail_dict_ori = products_to_buy.set_index(C.pid)[C.pavl].to_dict()
    print( avail_dict_ori )

    # First ask to buy more than avilable - It must fail 
    selected_items = products_to_buy.loc[:, [C.pid]]
    selected_items[C.qnt] = np.random.randint( products_to_buy.loc[:, [C.pavl]].max(), products_to_buy.loc[:, [C.pavl]].max() + 1000, size=len(selected_items))
    products = selected_items.to_dict(orient='records')

    # Add Them to Cart - Step 2 - Must fail as quantity is higher than available. 
    retval = CartService.addToCart(cust_id, products, True)
    if (retval < 0):
        print("Success Check avilability check verified. Higher quanitties check passed. ")
        assert(True)
    
    # Add the proper amounts to the cart. - Step 3
    selected_items = products_to_buy.loc[:, [C.pid]]
    selected_items[C.qnt] = 1
    products = selected_items.to_dict(orient='records')
    print(products_to_buy)
    retval = CartService.addToCart(cust_id, products, True)
    if (retval < 0):
        print("Something wrong with quantities. Even Minimum is failing. ", products)
        assert(False)
    else:
        read_back = CartService.get_customer_cart(cust_id).loc[:, [C.pid, C.qnt]]
        read_back_products = read_back.to_dict(orient='records')
        print(products)
        print(read_back_products)
        assert products == read_back_products, "Product Dictionaries dont match in the cart. Aborting purchase."
        print(" Success Verfied AddToCart ")

    # Initiate a Purchase - Step 4
    order_id = PurchaseService.purchase_cart_items(cust_id)
    print(order_id, "Generated from the purchase ")

    # Verifications - Step 5
    # First verify the items and order 
    read_back_items = BaseDBTransformer.readf(C.items, **{C.ordid: order_id}).loc[:, [C.pid, C.qnt]]
    print(read_back)
    assert products == read_back_items.to_dict(orient='records'), "Product Dictionaries dont match from order items. Cart items to Order items conversion failed - Purchase failed. ."

    # Next verify the quanitites of availability updatation.  - Step 6
    avail_dict_new = BaseDBTransformer.readdf(C.prd, C.pid, list(selected_items[C.pid])).set_index(C.pid)[C.pavl].to_dict()

    cart_quanties = selected_items.set_index(C.pid)[C.qnt].to_dict()
    
    print ( avail_dict_new, "\n", avail_dict_ori, "\n", cart_quanties)
    assert all(avail_dict_ori[k] - cart_quanties[k] == avail_dict_new[k] for k in avail_dict_ori)
    print(" Success Purchase cart is now verfied. For orders, order items and product quantities. ")

     # Verify if cart is now empty and Addition of discount coupons is successfull. 
    read_back = CartService.get_customer_cart(cust_id)
    if (len(read_back) > 0):
        print( "Cart NOT empty after purchase. Test failed. ")
        assert(False)
    
    disc_id = DiscountTransformer.generate_discount_id(order_id, cust_id)
    disc = BaseDBTransformer.readf(C.discounts, **{C.did: disc_id})
    if (len(disc) <= 0):
        print( "Discount code not found. Test failed. ")
        assert(False)
    
    scalars = disc.iloc[0].to_dict()
    status = scalars[C.dst]
    percent = scalars[C.dpct]
    oid = scalars[C.ordid]
    dcode = scalars[C.dcode]
    dcus = scalars[C.custid]

    print( scalars )
    print( status, percent, oid, dcode, dcus)

    assert all( [(status == 0), (percent == 0), ( dcus == cust_id), ( oid == order_id)]) 
    print( "Success - Purchase from cart without discount is completely verfied.")

    # Now make a purchase with discount using this dicount id created. 
    products_to_buy = ProductService.getRandomProducts()
    # avail_dict_ori = products_to_buy.set_index(C.pid)[C.pavl].to_dict()

    selected_items = products_to_buy.loc[:, [C.pid]]
    selected_items[C.qnt] = np.random.randint(1, products_to_buy.loc[:, [C.pavl]].min(), size=len(selected_items))
    products = selected_items.to_dict(orient='records')
    retval = CartService.addToCart(cust_id, products, True)
    if (retval < 0):
        print("Something wrong with quantities. Even Minimum is failing. ", products)
        assert(False)
    # Enable the discount coupon 
    DiscountService.enable_discount_codes(disc_id, 10, "FESTIVAL OFFER", True)
    order_id_disc = PurchaseService.purchase_discounted_cart_items(cust_id, disc_id)
    print(order_id_disc, "Generated from the purchase with discount ")

    # Verify if discount is set to used. 
    disc = BaseDBTransformer.readf(C.discounts, **{C.did: disc_id})
    if (len(disc) <= 0):
        print( "Discount code not found. Test failed. ")
        assert(False)
    scalars = disc.iloc[0].to_dict()
    status = scalars[C.dst]
    print( status, scalars)
    assert (status == 1)
    print( "Verified Discount coupon after use in purchase is now flagfed as used. (1) ")
    
    # lastly cleanup this purchase and verify if delete purcahse is working. 
    PurchaseTransformer.deletePurchase(order_id_disc, cust_id, None)
    PurchaseService.deletePurchase(order_id, cust_id)
    itm = BaseDBTransformer.readf(C.items, **{C.ordid: order_id})
    itm_d = BaseDBTransformer.readf(C.items, **{C.ordid: order_id_disc})
    crt = BaseDBTransformer.readf(C.cart, **{C.custid: cust_id})
    ord = BaseDBTransformer.readf(C.orders, **{C.ordid: order_id})
    ord_d = BaseDBTransformer.readf(C.orders, **{C.ordid: order_id_disc})
    dis = BaseDBTransformer.readf(C.discounts, **{C.did: disc_id})
    print( len(dis), len(ord), len(crt), len(itm) )
    assert all( [(len(dis) == 0), (len(itm) == 0), ( len(ord) == 0), ( len(crt) == 0)]) 

    print("Success Cleanup successfull after purchase ")