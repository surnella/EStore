import pandas as pd
from dao.base_transformer import BaseDBTransformer
import db.constants as C

class DiscountTransformer():

    @staticmethod
    def list_eligible_codes(cust_id: int):
        rows = BaseDBTransformer.read(C.discounts, **{C.custid: cust_id})
        return rows
    
    @staticmethod
    def list_all_codes():
        rows = BaseDBTransformer.read(C.discounts)
        return rows

    @staticmethod
    def list_active_codes():
        rows = BaseDBTransformer.readf(C.discounts, **{C.dpct + "__gt":0, C.dst + "__eq":0})
        return rows
    
    @staticmethod
    def enable_discount_codes(discount_id, percent, discount_code, debug=False):
        row = BaseDBTransformer.readf(C.discounts, **{C.dpct + "__gte":0, C.dst + "__eq":0, C.did + "__eq":discount_id})  
        if(debug):
            print(row) 
        if( (len(row) < 0) | (percent <= 0)):
            return None
        disc_dict = row.to_dict(orient='records')[0]
        if(debug):
            print( type( disc_dict), "\n", disc_dict)
        disc_dict[C.dpct] = percent
        disc_dict[C.dcode] = discount_code
        if( debug):
            print(disc_dict)
        try:
            BaseDBTransformer.update(C.discounts, discount_id, disc_dict)
        except Exception as e:
            print("Unable to update Discount percentage ", e)
        return
