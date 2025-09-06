import pandas as pd
from dao.base_transformer import BaseDBTransformer
from dao.purchase_transformer import PurchaseTransformer
from dao.discount_transformer import DiscountTransformer
import db.constants as C
from sqlalchemy.orm import Session
from db.dbutil import transaction

class PurchaseService():
    

    @staticmethod
    def list_orders_by_customer(cust_id: int):
        rows = PurchaseTransformer.list_orders_by_customer(cust_id)
        return rows
    
    @staticmethod
    def list_all_orders():
        rows = PurchaseTransformer.list_all_orders()
        return rows

    @staticmethod
    def empty_cart(cust_id, debug=False):
        # purchase is completed - Empty cart and Return Order ID. 
        PurchaseTransformer.empty_cart(cust_id)
        return None
    
    # Purcahse Methods ---------------------------------------------------Will commit the data or rollback on failure.
    
    # Purchase cart Items without discount
    @staticmethod
    def purchase_cart_items(cust_id, shipper_id=50001, pay_mode='Credit Card', debug=False):
        # This is the case where no discount is applied. Discount_id and code will be generated with Percent 0.
        # Admin APIs will update the percentage. 
        # First create order_header (order_id)
        ordh_dict = {}
        res = 0
        # ordh_dict[C.ordid] = 0
        ordh_dict[C.custid] = cust_id
        ordh_dict[C.pmode] = pay_mode
        ordh_dict[C.shipid] = shipper_id
        ordh_dict[C.did] = '1000' # This is dummy code used for orders done without discount. 1000 is -1 percent discount and status 1;

        try:
            with transaction() as session:
                # Step 1: Check if there are items in the cart.
                cust_cart = BaseDBTransformer.read(C.cart, **{C.custid: cust_id})
                if( len(cust_cart) <= 0):
                    print("Cart is Empty, Nothing to Purcahase, Aborting ")
                    return None
                
                # Step 2: Items are there in the cart. Create an Order id. 
                res = BaseDBTransformer.insert_(session, C.orders, ordh_dict)
                if(debug):
                    print("Order id created = ", res)

                # iterate all the items
                for i, dict in enumerate(cust_cart.to_dict(orient='records')):
                    ordi_dict = {}
                    ordi_dict[C.ordid] = res
                    ordi_dict[C.pid] = dict[C.pid]
                    ordi_dict[C.qnt] = dict[C.qnt]
                    resi = BaseDBTransformer.insert_(session, C.items,ordi_dict)
                    if(debug):
                        print("Inserted Order item retval = ", resi, " Content = ", ordi_dict)

                # Step 3: Now that purchase is completed. Empty the Cart. 
                PurchaseTransformer.empty_cart_after_purchase(session, cust_id, debug)

                # Step 4: This purchase is done without apply discount. So generate a coupon for the next purcahse. 
                DiscountTransformer.generate_discount_coupon(session, cust_id, res, debug)

        except Exception as e:
            print("Error: (Automatic rollback applied) while purchasing items for. Order Id = ", res, e)
            raise
        return res

    @staticmethod
    def deletePurchase(order_id, cust_id, debug=True):
       ret =  PurchaseTransformer.deletePurchase(order_id, cust_id, DiscountTransformer.generate_discount_id(cust_id, order_id))
       return ret
    
    @staticmethod
    def purchase_discounted_cart_items(cust_id, discount_id, shipper_id=50001, pay_mode='Credit Card', debug=False):
        # This is the case when discount is applied. Discount_id is passed The percent is retrieved and applied.
        # Admin APIs will update the percentage. 

        # Step 0: Check if discount is valid. If not then use normal purchase APIs.
        row = BaseDBTransformer.readf(C.discounts, **{C.dpct + "__gt":0, C.dst + "__eq":0, C.did + "__eq":discount_id})

        if (len(row) <=0 ):
            print("Discount_id is not valid, default to purcahse without discount")
            return PurchaseTransformer.purchase_cart_items(cust_id, shipper_id, pay_mode, debug)
        
        disc_dict = row.to_dict(orient='records')[0]
        print( "Valid discount id = ", disc_dict)

        # First create order_header (order_id)
        ordh_dict = {}
        res = 0
        # ordh_dict[C.ordid] = 0
        ordh_dict[C.custid] = cust_id
        ordh_dict[C.pmode] = pay_mode
        ordh_dict[C.shipid] = shipper_id
        ordh_dict[C.did] = disc_dict[C.did]

        # Step - 1 check if there are items in cart to purchase
        try:
            with transaction() as session:  
                cust_cart = BaseDBTransformer.read(C.cart, **{C.custid: cust_id})
                if( len(cust_cart) <= 0):
                    print("Cart is Empty, Nothing to Purcahase, Aborting ")
                    return None
                
                # Step - 2 - There are items so create an order_id. 
                res = BaseDBTransformer.insert_(session, C.orders,ordh_dict)
                if(debug):
                    print("Order id created = ", res)

                # Step 3 - Now add the cart items into order items to complete purchase. 
                for i, dict in enumerate(cust_cart.to_dict(orient='records')):
                    ordi_dict = {}
                    ordi_dict[C.ordid] = res
                    ordi_dict[C.pid] = dict[C.pid]
                    ordi_dict[C.qnt] = dict[C.qnt]
                    resi = BaseDBTransformer.insert_(session, C.items,ordi_dict)
                    if(debug):
                        print("Inserted Order item retval = ", resi, " Content = ", ordi_dict)

                # Step 4: Empty the cart now as the purchase is done. 
                PurchaseTransformer.empty_cart_after_purchase(session, cust_id, debug)

                # Step 5: Mark the discount coupon as used. 
                DiscountTransformer.discount_coupon_used(session, discount_id, debug)

        except Exception as e:
            print("Error: (Automatic rollback applied) while purchasing with discount items for. Order Id = ", res, e)
            raise
        return res
    
    # Transaction Methods ---------------------------------------------------Will commit the data or rollback. 
