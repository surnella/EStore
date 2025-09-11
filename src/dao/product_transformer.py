import pandas as pd
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import Table, MetaData, delete, select, update, func
from sqlalchemy.dialects.mysql import insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session
from db.dbsql import SessionLocal, Tables
from db.dbutil import transaction

# Import all the tables as classes. Putting here make it available for all the upper layers. 
from db.dbsql import Product, ProductClass
import db.constants as C
from dao.base_transformer import BaseDBTransformer

class ProductTransformer:

    @staticmethod
    def list_products_in_class(class_id: int):# pragma: no cover
        rows = ProductTransformer.get_products_by_class(class_id)
        # Transform into dictionaries for JSON/UI
        return [
            {C.pid: row.PRODUCT_ID, C.pname: row.PRODUCT_DESC, C.pavl: row.PRODUCT_QUANTITY_AVAIL, C.prdc: row.PRODUCT_CLASS_CODE, C.pname: row.PRODUCT_DESC, C.mrp: row.PRODUCT_PRICE}
            for row in rows
        ]
    
    @staticmethod
    def list_products_in_class_df(class_id: int):
        rows = ProductTransformer.get_products_by_class_df(class_id)
        # Transform into dictionaries for JSON/UI
        return rows

    @staticmethod
    def list_all_products(prd_id=None, debug=False):
        if(debug):
            print( " Recevied the product id ", prd_id)
        if (prd_id is None):
            rows = BaseDBTransformer.read(C.prd)
        else:
            rows = BaseDBTransformer.read(C.prd, prd_id)
        if(debug):
            print( " retrieved the data rows = ", len(rows))
        return rows

    @staticmethod
    def get_products_class():
        rows = BaseDBTransformer.read(C.prdc)
        # Transform into dictionaries for JSON/UI
        return rows
    
    @staticmethod    
    def getRandomProducts(num=3, debug=False):
        products = ProductTransformer.list_all_products()
        if (debug):
            print(products.shape)
            print(products.head())
        return products.sample(n=num)
    
    @staticmethod
    def get_products_by_class(class_id: int):# pragma: no cover
        """Return all products for a given product_class_id."""
        stmt = (
            select(Product)
            .join(ProductClass, Product.c.PRODUCT_CLASS_CODE == ProductClass.c.PRODUCT_CLASS_CODE)
            .where(ProductClass.c.PRODUCT_CLASS_CODE == class_id)
        )
        try:
            with SessionLocal() as session:
                result = session.execute(stmt).fetchall()
                return result  # list of Row objects
        except Exception as e:
            print("Error getting products by class as list:", e)
            raise

    @staticmethod
    def insert_product(product_data: dict):# pragma: no cover
        stmt = Product.insert().values(**product_data)
        try:
            with SessionLocal() as session:
                result = session.execute(stmt)
                #session.commit()
                return result.inserted_primary_key[0]
        except Exception as e:
            print("Error inserting product:", e)
            raise

    @staticmethod
    def update_product(pk_value: int, update_dict: dict, pk_column=Product.c.PRODUCT_ID):# pragma: no cover
        stmt = Product.update().where(pk_column == pk_value).values(**update_dict)
        try:
            with SessionLocal() as session:
                session.execute(stmt)
                #session.commit()
        except Exception as e:
            print("Error updating product:", e)
            raise

    @staticmethod
    def delete_product(pk_value: int, pk_column=Product.c.PRODUCT_ID):# pragma: no cover
        stmt = Product.delete().where(pk_column == pk_value)
        try:
            with SessionLocal() as session:
                session.execute(stmt)
                #session.commit()
        except Exception as e:
            print("Error deleting product:", e)
            raise

    @staticmethod
    def get_products_by_class_df(class_id: int, debug=True):
        # Build SQLAlchemy Core select statement
        stmt = (
            select(Product)
            .join(ProductClass, Product.c.PRODUCT_CLASS_CODE == ProductClass.c.PRODUCT_CLASS_CODE)
            .where(ProductClass.c.PRODUCT_CLASS_CODE == class_id)
        )
        try:
            # Use a session to get a connection
            with SessionLocal() as session:
                conn = session.connection()
                df = pd.read_sql_query(stmt, conn)
                if( debug):
                    print(df)
                return df
        except Exception as e:
            print("Error getting products by class as df:", e)
            raise

