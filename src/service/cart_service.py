import pandas as pd
from dao.cart_transformer import CartTransformer

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
    def addToCart(cust_id, products, debug=False):
        CartTransformer.addToCart(cust_id, products, debug)
        return
    
    @staticmethod
    def updateCart(cust_id, products, debug=False):
        CartTransformer.updateCart(cust_id, products, debug)
        return