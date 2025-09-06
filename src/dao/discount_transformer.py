import pandas as pd
from dao.base_transformer import BaseDBTransformer
import db.constants as C
from sqlalchemy.orm import Session
from db.dbutil import transaction

class DiscountTransformer():

    @staticmethod
    def list_eligible_codes(cust_id: int):
        rows = BaseDBTransformer.readf(C.discounts, **{C.custid+"__eq": cust_id, C.dst+"__eq":0})
        return rows
    
    @staticmethod
    def list_used_codes(cust_id: int):
        rows = BaseDBTransformer.readf(C.discounts, **{C.custid+"__eq": cust_id, C.dst+"__eq":1})
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
    def enable_discount_codes(discount_id, percent, discount_code, discount_status=0, debug=False):
        row = BaseDBTransformer.readf(C.discounts, **{C.dpct + "__gte":0, C.dst + "__eq":0, C.did + "__eq":discount_id})  
        if(debug):
            print("enable_discount_codes called with: ", row) 

        if( (len(row) <= 0) | (percent <= 0)):
            return None
        
        disc_dict = row.to_dict(orient='records')[0]

        disc_dict[C.dpct] = percent
        disc_dict[C.dcode] = discount_code
        disc_dict[C.dst] = discount_status

        if(debug):
            print("enable_discount_codes called disocunt dict = : ", disc_dict, type(disc_dict))
        try:
            retval = BaseDBTransformer.update(C.discounts, discount_id, disc_dict)
            if(debug):
                print("In ennable discount codes after update in BaseDBTransformer.", retval, ":", discount_id)
            return discount_id
        except Exception as e:
            print("Unable to update Discount percentage ", e)
            raise

    @staticmethod
    def discount_coupon_used(session: Session, discount_id, debug=False):
        try:
            disc_dict = {}
            disc_dict[C.dst] = 1
            resd = BaseDBTransformer.update_(session, C.discounts, discount_id, disc_dict)
            if(debug):
                print("Updated discount code status to 1 retval = ", resd, " Content = ", disc_dict)
        except Exception as e:
            raise
    
    @staticmethod 
    def generate_discount_id(order_id: int, cust_id: int):
        return str(cust_id) + "__" + str(order_id)

    @staticmethod
    def generate_discount_coupon(session: Session, cust_id, order_id, debug=False):
        #Before returning Order ID also store an inactive row in Discounts table. It will be Percent 0. 
        #Admin can update it a peercent value then it can be applied to an order. 
        disc_dict = {}
        disc_dict[C.did] = DiscountTransformer.generate_discount_id(order_id, cust_id)
        disc_dict[C.ordid] = order_id
        disc_dict[C.custid] = cust_id
        disc_dict[C.dcode] = "ADMIN_TO_RENAME"
        disc_dict[C.dpct] = 0
        disc_dict[C.dst] = 0
        try:
            resd = BaseDBTransformer.insert_(session, C.discounts,disc_dict)
            if(debug):
                print("Inserted discount code retval = ", resd, " Content = ", disc_dict)
        except Exception as e:
            raise
        return order_id
