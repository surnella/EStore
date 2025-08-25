import pandas as pd
from dao.base_transformer import BaseDBTransformer
import db.constants as C

class DiscountTransformer():

    @staticmethod
    def list_eligible_codes(cust_id: int):
        rows = BaseDBTransformer.read(C.discounts, **{C.custid: cust_id})
        # Transform into dictionaries for JSON/UI
        return rows
    
    @staticmethod
    def list_all_codes():
        rows = BaseDBTransformer.read(C.discounts)
        # Transform into dictionaries for JSON/UI
        return rows