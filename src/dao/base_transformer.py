import pandas as pd
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import Table, MetaData, delete, select, update
from db import SessionLocal, Product, ProductClass
import db.constants as C

class BaseDBTransformer:
    @staticmethod
    def get_products_by_class(class_id: int):
        """Return all products for a given product_class_id."""
        stmt = (
            select(Product)
            .join(ProductClass, Product.c.product_class_id == ProductClass.c.id)
            .where(ProductClass.c.id == class_id)
        )

        with SessionLocal() as session:
            result = session.execute(stmt).fetchall()
            return result  # list of Row objects
