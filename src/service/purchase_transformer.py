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

    # Purchase cart Items without discount
    @staticmethod
    def purchase_cart_items(cust_id, shipper_id=50001, pay_mode='Credit Card', debug=False):
        # This is the case where no discount is applied. Discount_id and code will be generated with Percent 0.
        # Admin APIs will update the percentage. 
        # First create order_header (order_id)
        ordh_dict = {}
        res = 0
        # ordh_dict[C.ordid] = 0
        ordh_dict[C.custid] = cust_id
        ordh_dict[C.pmode] = pay_mode
        ordh_dict[C.shipid] = shipper_id
        ordh_dict[C.did] = '1000' # This is dummy code used for orders done without discount. 1000 is -1 percent discount and status 1;

        try:
            cust_cart = BaseDBTransformer.read(C.cart, **{C.custid: cust_id})
            if( len(cust_cart) <= 0):
                print("Cart is Empty, Nothing to Purcahase, Aborting ")
                return None
            res = BaseDBTransformer.insert(C.orders,ordh_dict)
            if(debug):
                print("Order id created = ", res)

            for i, dict in enumerate(cust_cart.to_dict(orient='records')):
                ordi_dict = {}
                ordi_dict[C.ordid] = res
                ordi_dict[C.pid] = dict[C.pid]
                ordi_dict[C.qnt] = dict[C.qnt]
                resi = BaseDBTransformer.insert(C.items,ordi_dict)
                if(debug):
                    print("Inserted Order item retval = ", resi, " Content = ", ordi_dict)
        except Exception as e:
            print("Error  while creating order id and order items. Undoing inserts:", e)
            if( res > 0):
                BaseDBTransformer.delete(C.orders, res, C.custid)
                BaseDBTransformer.delete(C.items, res, C.custid)
            raise

        try: 
            PurchaseTransformer.empty_cart(cust_id, debug)
        except Exception as e:
            print("Error  while Emptying cart. purchase completed Order Id = ", res, e)
            raise

        try: 
            PurchaseTransformer.generate_discount_coupon(cust_id, res, debug)
        except Exception as e:
            print("Error  while generating dscount coupon. purchase completed Order Id = ", res, e)
            raise

        return res

    @staticmethod
    def empty_cart(cust_id, debug=False):
        # purchase is completed - Empty cart and Return Order ID. 
        try:
            BaseDBTransformer.delete(C.cart, cust_id, C.custid)
        except Exception as e:
            raise
        return None
    
    @staticmethod
    def discount_coupon_used(discount_id, debug=False):
        try:
            disc_dict = {}
            disc_dict[C.dst] = 1
            resd = BaseDBTransformer.update(C.discounts, discount_id, disc_dict)
            if(debug):
                print("Updated discount code status to 1 retval = ", resd, " Content = ", disc_dict)
        except Exception as e:
            raise

    @staticmethod
    def generate_discount_coupon(cust_id, res, debug=False):
        #Before returning Order ID also store an inactive row in Discounts table. It will be Percent 0. 
        #Admin can update it a peercent value then it can be applied to an order. 
        disc_dict = {}
        disc_dict[C.did] = str(cust_id) + "__" + str(res)
        disc_dict[C.ordid] = res
        disc_dict[C.custid] = cust_id
        disc_dict[C.dcode] = "ADMIN_TO_RENAME"
        disc_dict[C.dpct] = 0
        disc_dict[C.dst] = 0
        try:
            resd = BaseDBTransformer.insert(C.discounts,disc_dict)
            if(debug):
                print("Inserted discount code retval = ", resd, " Content = ", disc_dict)
        except Exception as e:
            raise
        return res
    
    @staticmethod
    def purchase_discounted_cart_items(cust_id, discount_id, shipper_id=50001, pay_mode='Credit Card', debug=False):
        # This is the case when discount is applied. Discount_id is passed The percent is retrieved and applied.
        # Admin APIs will update the percentage. 

        #Check if discount is valid. 
        row = BaseDBTransformer.readf(C.discounts, **{C.dpct + "__gt":0, C.dst + "__eq":0, C.did + "__eq":discount_id})

        if (len(row) <=0 ):
            print("Discount_id is not valid, default to purcahse without discount")
            PurchaseTransformer.purchase_cart_items(cust_id, shipper_id, pay_mode, debug)
        
        disc_dict = row.to_dict(orient='records')[0]
        print( "Valid discount id = ", disc_dict)

        # First create order_header (order_id)
        ordh_dict = {}
        res = 0
        # ordh_dict[C.ordid] = 0
        ordh_dict[C.custid] = cust_id
        ordh_dict[C.pmode] = pay_mode
        ordh_dict[C.shipid] = shipper_id
        ordh_dict[C.did] = disc_dict[C.did]

        try:
            cust_cart = BaseDBTransformer.read(C.cart, **{C.custid: cust_id})
            if( len(cust_cart) <= 0):
                print("Cart is Empty, Nothing to Purcahase, Aborting ")
                return None
            res = BaseDBTransformer.insert(C.orders,ordh_dict)
            if(debug):
                print("Order id created = ", res)

            for i, dict in enumerate(cust_cart.to_dict(orient='records')):
                ordi_dict = {}
                ordi_dict[C.ordid] = res
                ordi_dict[C.pid] = dict[C.pid]
                ordi_dict[C.qnt] = dict[C.qnt]
                resi = BaseDBTransformer.insert(C.items,ordi_dict)
                if(debug):
                    print("Inserted Order item retval = ", resi, " Content = ", ordi_dict)
        except Exception as e:
            print("Error  while creating order id and order items. Undoing inserts:", e)
            if( res > 0):
                BaseDBTransformer.delete(C.orders, res, C.custid)
                BaseDBTransformer.delete(C.items, res, C.custid)
            raise
        try: 
            PurchaseTransformer.empty_cart(cust_id, debug)
        except Exception as e:
            print("Error  while Emptying cart. purchase completed Order Id = ", res, e)
            raise
        try: 
            PurchaseTransformer.discount_coupon_used(discount_id, debug)
        except Exception as e:
            print("Error  while generating dscount coupon. purchase completed Order Id = ", res, e)
            raise
        return res