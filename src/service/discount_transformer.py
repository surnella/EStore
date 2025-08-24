import pandas as pd
from dao.base_transformer import BaseDBTransformer
import db.constants as C

class DiscountTransformer():

    @staticmethod
    def list_products_in_class(class_id: int):
        rows = BaseDBTransformer.get_products_by_class(class_id)
        # Transform into dictionaries for JSON/UI
        return [
            {"id": row.Product.id, "name": row.Product.name, "price": row.Product.price}
            for row in rows
        ]