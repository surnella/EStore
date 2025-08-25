import pandas as pd
from dao.base_transformer import BaseDBTransformer
import db.constants as C

class ProductTransformer():

    @staticmethod
    def list_products_in_class(class_id: int):
        rows = BaseDBTransformer.get_products_by_class(class_id)
        # Transform into dictionaries for JSON/UI
        return [
            {C.pid: row.PRODUCT_CLASS_CODE, C.pname: row.PRODUCT_DESC, C.mrp: row.PRODUCT_PRICE}
            for row in rows
        ]
    
    @staticmethod
    def list_products_in_class_df(class_id: int):
        rows = BaseDBTransformer.get_products_by_class_df(class_id)
        # Transform into dictionaries for JSON/UI
        return rows

    @staticmethod
    def list_all_products():
        rows = BaseDBTransformer.read(C.prd)
        # Transform into dictionaries for JSON/UI
        return rows
    
    @staticmethod    
    def getRandomProducts(num=3, debug=False):
        products = ProductTransformer.list_all_products()
        if (debug):
            print(products.shape)
            print(products.head())
        return products.sample(n=num)
