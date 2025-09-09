import pandas as pd
from dao.base_transformer import BaseDBTransformer
from dao.discount_transformer import DiscountTransformer
# import db.constants as C
# from db.dbutil import transaction

class DiscountService():

    @staticmethod
    def list_eligible_codes(cust_id: int):
        rows = DiscountTransformer.list_eligible_codes(cust_id)
        return rows
    
    @staticmethod
    def list_used_codes(cust_id: int):
        rows = DiscountTransformer.list_used_codes(cust_id)
        return rows
    
    @staticmethod
    def list_all_codes(cust_id: int = None):
        rows = DiscountTransformer.list_all_codes(cust_id)
        return rows

    @staticmethod
    def list_active_codes(cust_id=None):
        rows = DiscountTransformer.list_active_codes(cust_id)
        return rows
    
    @staticmethod
    def enable_discount_codes(discount_id, percent, discount_code, discount_status=0, debug=False):
        retval = DiscountTransformer.enable_discount_codes(discount_id, percent, discount_code, discount_status, debug)
        print("retval in Service class ", retval)
        return retval
