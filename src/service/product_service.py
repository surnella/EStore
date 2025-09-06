import pandas as pd
from dao.product_transformer import ProductTransformer
import db.constants as C

class ProductService():
    
    @staticmethod
    def list_products_in_class_df(class_id: int):
        rows = ProductTransformer.get_products_by_class_df(class_id)
        # Transform into dictionaries for JSON/UI
        return rows

    @staticmethod
    def list_all_products(prd_id=None, debug=False):
        rows = ProductTransformer.list_all_products(prd_id, debug)
        return rows

    @staticmethod
    def get_products_class():
        rows = ProductTransformer.get_products_class()
        return rows
    
    @staticmethod    
    def getRandomProducts(num=3, debug=False):
        return ProductTransformer.getRandomProducts(num, debug)
