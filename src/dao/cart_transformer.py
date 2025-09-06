import pandas as pd
from dao.base_transformer import BaseDBTransformer
import db.constants as C
from sqlalchemy.orm import Session
from db.dbutil import transaction

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
    def addToCart_(session: Session, cust_id, products, debug=False):
        try:
            for i,dict in enumerate(products):
                cart_dict = {}
                cart_dict[C.custid] = cust_id
                cart_dict[C.pid] = dict[C.pid]
                cart_dict[C.qnt] = dict[C.qnt]
                if(debug):
                    print( "row: ", i, "=", cart_dict )
                BaseDBTransformer.upsert_(session, C.cart, cart_dict, C.qnt)
        except Exception as e:
            print(" BaseDBTransformer: upsert failed", e)
            raise

    @staticmethod
    def addToCart(cust_id, products, debug=False):
        try:
            with transaction() as session:
                CartTransformer.addToCart_(session, cust_id, products, debug)
        except Exception as e:
            print("Adding to Cart Failed. Auto Rollback.", e)
        return
    
    @staticmethod
    def updateCart(cust_id, products, debug=False):
        try:
            with transaction() as session:
                BaseDBTransformer.delete_(session, C.cart, cust_id, C.custid)
                CartTransformer.addToCart_(session, cust_id, products, debug)
        except Exception as e:
            print("Updating Cart Failed. Auto Rollback.", e)
        return