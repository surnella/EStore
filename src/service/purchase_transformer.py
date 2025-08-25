import pandas as pd
from dao.base_transformer import BaseDBTransformer
import db.constants as C

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