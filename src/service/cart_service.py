import pandas as pd
from dao.cart_transformer import CartTransformer
from service.product_service import ProductService
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
            prds = BaseDBTransformer.readdf(C.prd, C.pid, prd_list)
            print( prds)
            for i, (idx, prd) in enumerate(prds.iterrows()):
                if( prd[C.pavl] < prd_list[dict[C.pid]]):
                    print(f" Request quantity not avilable, have {prd[C.pavl]} requested {prd_list[dict[C.pid]]} for product {prd}")
                    return False
            return True
        except Exception as e:
            print("Check avilability failed to check quantities.",products, e)
            raise
    
    @staticmethod
    def addToCart(cust_id, products, debug=False):
        if( CartService.checkAvailbility(products) ):
            CartTransformer.addToCart(cust_id, products, debug)
            return 0
        else:
            print("Some Items not avilabile")
            return -1
        
    
    @staticmethod
    def updateCart(cust_id, products, debug=False):
        if( CartService.checkAvailbility(products) ):
            CartTransformer.updateCart(cust_id, products, debug)
            return 0
        else:
            print("Some Items not avilable")
            return -1
    
    @staticmethod
    def empty_cart(cust_id, debug=False):
        # purchase is completed - Empty cart and Return Order ID. 
        try:
            CartTransformer.empty_cart(cust_id, debug)
        except Exception as e:
            raise
        return None