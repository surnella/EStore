import pandas as pd
from dao.cart_transformer import CartTransformer
from dao.base_transformer import BaseDBTransformer
import db.constants as C

# Not used now but may need if business logic increases. 
# import db.constants as C
# from sqlalchemy.orm import Session
# from db.dbutil import transaction

class CartService():
    @staticmethod
    def list_all():
        rows = CartTransformer.list_all()
        return rows
    
    @staticmethod
    def get_customer_cart(cust_id: str):
        rows = CartTransformer.list_cust(cust_id)
        return rows

    @staticmethod
    def checkAvailbility(products, debug=False):
        try:
            prd_list = {}
            for i,dict in enumerate(products):
                prd_list[dict[C.pid]] = dict[C.qnt]
            # TO DO: Existing cart count needs to be added here. 
            prds = BaseDBTransformer.readdf(C.prd, C.pid, prd_list)
            if ( (len(prds) <= 0) | len(prds) != len(prd_list)):
                return False
            # print( prds)
            # print( prd_list)
            for i, (idx, prd) in enumerate(prds.iterrows()):
                if( debug ):
                    print(i, "Comparing: ", prd[C.pavl], " with ",  prd_list[prd[C.pid]] )
                if( prd[C.pavl] < prd_list[prd[C.pid]]):
                    print(f" Request quantity not avilable, have {prd[C.pavl]} requested {prd_list[dict[C.pid]]} for product {prd}")
                    return False
            return True
        except Exception as e:
            print("Check avilability failed to check quantities.",products, e)
            raise
    
    @staticmethod
    def addToCart(cust_id, products, debug=False):
        try:
            if( CartService.checkAvailbility(products, debug) ):
                print(" Adding to Cart")
                return CartTransformer.addToCart(cust_id, products, debug)
            else:
                print("Some Items not avilabile")
            return -1
        except Exception as e:
            return -1        
    
    @staticmethod
    def updateCart(cust_id, products, debug=False):
        try:
            if( CartService.checkAvailbility(products) ):
                CartTransformer.updateCart(cust_id, products, debug)
                return 0
            else:
                print("Some Items not avilable")
                return -1
        except Exception as e:
            return -1
    
    @staticmethod
    def empty_cart(cust_id, debug=False):
        # purchase is completed - Empty cart and Return Order ID. 
        try:
            CartTransformer.empty_cart(cust_id, debug)
            return None
        except Exception as e:
            raise