import pandas as pd
from dao.base_transformer import BaseDBTransformer
import db.constants as C
from sqlalchemy.orm import Session
from db.dbutil import transaction

class PurchaseTransformer():
    @staticmethod
    def list_orders_by_customer(cust_id: int):
        rows = BaseDBTransformer.readf(C.orders, **{C.custid: cust_id})
        # Transform into dictionaries for JSON/UI
        return rows
    
    @staticmethod
    def list_all_orders():
        rows = BaseDBTransformer.read(C.orders)
        # Transform into dictionaries for JSON/UI
        return rows

    @staticmethod
    def empty_cart(cust_id, debug=False):
        # purchase is completed - Empty cart and Return Order ID. 
        try:
            BaseDBTransformer.delete(C.cart, cust_id, C.custid)
        except Exception as e:
            raise
        return None
    
    @staticmethod
    def reverse_product_availbility(session: Session, order_id, debug=False):
        # purchase is completed - First update the Availibility values in the product. Empty cart and Return Order ID. 
        try:
            item_cart = BaseDBTransformer.readf(C.items, **{C.ordid+"__eq":order_id})
            if( len(item_cart) <= 0):
                print("items cart is already Empty, Aborting ")
                return None
            for i, dict in enumerate(item_cart.to_dict(orient='records')):
                prd_df = BaseDBTransformer.read(C.prd, dict[C.pid])
                prd_dict = prd_df.iloc[0].to_dict()
                prd_dict[C.pavl] = int(prd_dict[C.pavl]) + int( dict[C.qnt])
                res = BaseDBTransformer.update_(session, C.prd, dict[C.pid], prd_dict)
                if(debug):
                    print( i, ": Update product availibility for ", dict[C.pid], " New:Change = ", 
                          prd_dict[C.pavl] , ":",dict[C.qnt], " Return value = ", res)
        except Exception as e:
            raise
        return None

    @staticmethod
    def update_product_availbility(session: Session, cust_id, debug=True):
        # purchase is completed - First update the Availibility values in the product. Empty cart and Return Order ID. 
        try:
            cust_cart = BaseDBTransformer.read(C.cart, **{C.custid: cust_id})
            if( len(cust_cart) <= 0):
                print("Cart is already Empty, Aborting ")
                return None
            for i, dict in enumerate(cust_cart.to_dict(orient='records')):
                prd_df = BaseDBTransformer.read(C.prd, dict[C.pid])
                prd_dict = prd_df.iloc[0].to_dict()
                # if( add ):
                #     prd_dict[C.pavl] = int(prd_dict[C.pavl]) + int( dict[C.qnt])
                # else:
                prd_dict[C.pavl] = int(prd_dict[C.pavl]) - int( dict[C.qnt])
                res = BaseDBTransformer.update_(session, C.prd, dict[C.pid], prd_dict)
                if(debug):
                    print( i, ": Update product availibility for ", dict[C.pid], " New:Change = ", 
                          prd_dict[C.pavl] , ":",dict[C.qnt], " Return value = ", res)
        except Exception as e:
            raise
        return None
    
    @staticmethod
    def empty_cart_after_purchase(session: Session, cust_id, debug=False):
        # purchase is completed - First update the Availibility values in the product. Empty cart and Return Order ID. 
        try:
            PurchaseTransformer.update_product_availbility(session, cust_id, debug)
            # Now empty the cart. 
            BaseDBTransformer.delete_(session, C.cart, cust_id, C.custid)
        except Exception as e:
            raise
        return None
    
    @staticmethod
    def deletePurchase(order_id: int, cust_id, discount_id: str, debug=False):
        try:
            with transaction() as session:
                PurchaseTransformer.reverse_product_availbility(session, order_id, debug)
                BaseDBTransformer.delete_(session, C.items, order_id, C.ordid)
                BaseDBTransformer.delete_(session, C.orders, order_id, C.ordid)
                BaseDBTransformer.delete_(session, C.discounts, discount_id)
                if(debug):
                    print("Undo a purchase success for order id = ", order_id)
        except Exception as e:
            print("Undo a purchase failed Order Id = ", order_id, e) 
            raise
        return order_id
    
    # Transaction Methods ---------------------------------------------------Will commit the data or rollback. 
