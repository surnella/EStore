import pandas as pd
from dao.base_transformer import BaseDBTransformer
import db.constants as C
from sqlalchemy.orm import Session
from db.dbutil import transaction

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

    # Helper methods ---------------------------------------------------Will NOT commit the data or rollback. 

    @staticmethod
    def empty_cart(cust_id, debug=False):
        # purchase is completed - Empty cart and Return Order ID. 
        try:
            BaseDBTransformer.delete(C.cart, cust_id, C.custid)
        except Exception as e:
            raise
        return None
    
    @staticmethod
    def reverse_product_availbility(session: Session, order_id, debug=False):
        # purchase is completed - First update the Availibility values in the product. Empty cart and Return Order ID. 
        try:
            item_cart = BaseDBTransformer.readf(C.items, **{C.ordid+"__eq":order_id})
            if( len(item_cart) <= 0):
                print("items cart is already Empty, Aborting ")
                return None
            for i, dict in enumerate(item_cart.to_dict(orient='records')):
                prd_df = BaseDBTransformer.read(C.prd, dict[C.pid])
                prd_dict = prd_df.iloc[0].to_dict()
                prd_dict[C.pavl] = int(prd_dict[C.pavl]) + int( dict[C.qnt])
                res = BaseDBTransformer.update_(session, C.prd, dict[C.pid], prd_dict)
                if(debug):
                    print( i, ": Update product availibility for ", dict[C.pid], " New:Change = ", 
                          prd_dict[C.pavl] , ":",dict[C.qnt], " Return value = ", res)
        except Exception as e:
            raise
        return None

    @staticmethod
    def update_product_availbility(session: Session, cust_id, debug=True):
        # purchase is completed - First update the Availibility values in the product. Empty cart and Return Order ID. 
        try:
            cust_cart = BaseDBTransformer.read(C.cart, **{C.custid: cust_id})
            if( len(cust_cart) <= 0):
                print("Cart is already Empty, Aborting ")
                return None
            for i, dict in enumerate(cust_cart.to_dict(orient='records')):
                prd_df = BaseDBTransformer.read(C.prd, dict[C.pid])
                prd_dict = prd_df.iloc[0].to_dict()
                # if( add ):
                #     prd_dict[C.pavl] = int(prd_dict[C.pavl]) + int( dict[C.qnt])
                # else:
                prd_dict[C.pavl] = int(prd_dict[C.pavl]) - int( dict[C.qnt])
                res = BaseDBTransformer.update_(session, C.prd, dict[C.pid], prd_dict)
                if(debug):
                    print( i, ": Update product availibility for ", dict[C.pid], " New:Change = ", 
                          prd_dict[C.pavl] , ":",dict[C.qnt], " Return value = ", res)
        except Exception as e:
            raise
        return None
    
    @staticmethod
    def empty_cart_after_purchase(session: Session, cust_id, debug=False):
        # purchase is completed - First update the Availibility values in the product. Empty cart and Return Order ID. 
        try:
            PurchaseTransformer.update_product_availbility(session, cust_id, debug)
            # Now empty the cart. 
            BaseDBTransformer.delete_(session, C.cart, cust_id, C.custid)
        except Exception as e:
            raise
        return None
    
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
        disc_dict[C.did] = PurchaseTransformer.generate_discount_id(order_id, cust_id)
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
    
        # END Helper methods ---------------------------------------------------Will NOT commit the data or rollback. 


    # Transaction Methods ---------------------------------------------------Will commit the data or rollback. 

    
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
            with transaction() as session:
                # Step 1: Check if there are items in the cart.
                cust_cart = BaseDBTransformer.read(C.cart, **{C.custid: cust_id})
                if( len(cust_cart) <= 0):
                    print("Cart is Empty, Nothing to Purcahase, Aborting ")
                    return None
                
                # Step 2: Items are there in the cart. Create an Order id. 
                res = BaseDBTransformer.insert_(session, C.orders, ordh_dict)
                if(debug):
                    print("Order id created = ", res)

                # iterate all the items
                for i, dict in enumerate(cust_cart.to_dict(orient='records')):
                    ordi_dict = {}
                    ordi_dict[C.ordid] = res
                    ordi_dict[C.pid] = dict[C.pid]
                    ordi_dict[C.qnt] = dict[C.qnt]
                    resi = BaseDBTransformer.insert_(session, C.items,ordi_dict)
                    if(debug):
                        print("Inserted Order item retval = ", resi, " Content = ", ordi_dict)

                # Step 3: Now that purchase is completed. Empty the Cart. 
                PurchaseTransformer.empty_cart_after_purchase(session, cust_id, debug)

                # Step 4: This purchase is done without apply discount. So generate a coupon for the next purcahse. 
                PurchaseTransformer.generate_discount_coupon(session, cust_id, res, debug)

        except Exception as e:
            print("Error: (Automatic rollback applied) while purchasing items for. Order Id = ", res, e)
            raise
        return res
    
    @staticmethod
    def purchase_discounted_cart_items(cust_id, discount_id, shipper_id=50001, pay_mode='Credit Card', debug=False):
        # This is the case when discount is applied. Discount_id is passed The percent is retrieved and applied.
        # Admin APIs will update the percentage. 

        # Step 0: Check if discount is valid. If not then use normal purchase APIs.
        row = BaseDBTransformer.readf(C.discounts, **{C.dpct + "__gt":0, C.dst + "__eq":0, C.did + "__eq":discount_id})

        if (len(row) <=0 ):
            print("Discount_id is not valid, default to purcahse without discount")
            return PurchaseTransformer.purchase_cart_items(cust_id, shipper_id, pay_mode, debug)
        
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

        # Step - 1 check if there are items in cart to purchase
        try:
            with transaction() as session:  
                cust_cart = BaseDBTransformer.read(C.cart, **{C.custid: cust_id})
                if( len(cust_cart) <= 0):
                    print("Cart is Empty, Nothing to Purcahase, Aborting ")
                    return None
                
                # Step - 2 - There are items so create an order_id. 
                res = BaseDBTransformer.insert_(session, C.orders,ordh_dict)
                if(debug):
                    print("Order id created = ", res)

                # Step 3 - Now add the cart items into order items to complete purchase. 
                for i, dict in enumerate(cust_cart.to_dict(orient='records')):
                    ordi_dict = {}
                    ordi_dict[C.ordid] = res
                    ordi_dict[C.pid] = dict[C.pid]
                    ordi_dict[C.qnt] = dict[C.qnt]
                    resi = BaseDBTransformer.insert_(session, C.items,ordi_dict)
                    if(debug):
                        print("Inserted Order item retval = ", resi, " Content = ", ordi_dict)

                # Step 4: Empty the cart now as the purchase is done. 
                PurchaseTransformer.empty_cart_after_purchase(session, cust_id, debug)

                # Step 5: Mark the discount coupon as used. 
                PurchaseTransformer.discount_coupon_used(session, discount_id, debug)
        except Exception as e:
            print("Error: (Automatic rollback applied) while purchasing with discount items for. Order Id = ", res, e)
            raise
        return res

    @staticmethod
    def deletePurchase(order_id: int, cust_id, debug=False):
        try:
            with transaction() as session:
                PurchaseTransformer.reverse_product_availbility(session, order_id, debug)
                BaseDBTransformer.delete_(session, C.items, order_id, C.ordid)
                BaseDBTransformer.delete_(session, C.orders, order_id, C.ordid)

                discount_id = PurchaseTransformer.generate_discount_id(order_id, cust_id)
                BaseDBTransformer.delete_(session, C.discounts, discount_id)
                if(debug):
                    print("Undo a purchase success for order id = ", order_id)
        except Exception as e:
            print("Undo a purchase failed Order Id = ", order_id, e) 
            raise
        return order_id
    
    # Transaction Methods ---------------------------------------------------Will commit the data or rollback. 
