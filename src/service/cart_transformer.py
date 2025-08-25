import pandas as pd
from dao.base_transformer import BaseDBTransformer
import db.constants as C

class CartTransformer():

    @staticmethod
    def list_all():
        rows = BaseDBTransformer.read(C.cart)
        # Transform into dictionaries for JSON/UI
        return rows
    
    @staticmethod
    def list_cust(cust_id: str):
        rows = BaseDBTransformer.read(C.cart, **{C.custid: cust_id})
        # Transform into dictionaries for JSON/UI
        return rows

    @staticmethod
    def addToCart(cust_id, products, debug=False):
        for i,pid in enumerate(products[C.pid]):
            cart_dict = {}
            cart_dict[C.custid] = cust_id
            cart_dict[C.pid] = pid
            cart_dict[C.qnt] = 1
            if( debug ):
                print( "row: ", i, "=", cart_dict )
            BaseDBTransformer.upsert(C.cart, cart_dict, C.qnt)
        return