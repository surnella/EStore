import pandas as pd
from dao.base_transformer import BaseDBTransformer
import db.constants as C

class CartTransformer():

    @staticmethod
    def list_cart_items():
        rows = BaseDBTransformer.read(C.cart)
        # Transform into dictionaries for JSON/UI
        return rows