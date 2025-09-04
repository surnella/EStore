import pandas as pd
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import Table, MetaData, delete, select, update
from sqlalchemy.orm import Session
from sqlalchemy.dialects.mysql import insert
from sqlalchemy.exc import IntegrityError
from db.dbsql import SessionLocal, Tables
from db.dbutil import transaction
# Import all the tables as classes. Putting here make it available for all the upper layers. 
from db.dbsql import Product, ProductClass, Address, Customer, Items, Orders, Shipper, Cart, Discount
import db.constants as C

class BaseDBTransformer:
    debug=True

    @staticmethod
    def get_products_by_class(class_id: int):
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
    def get_products_class():
        """Return all products for a given product_class_id."""
        stmt = (
            select(ProductClass)
        )
        try:
            with SessionLocal() as session:
                result = session.execute(stmt).fetchall()
                return result  # list of Row objects
        except Exception as e:
            print("Error getting products by class as list:", e)
            raise

    @staticmethod
    def insert_product(product_data: dict):
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
    def update_product(pk_value: int, update_dict: dict, pk_column=Product.c.PRODUCT_ID):
        stmt = Product.update().where(pk_column == pk_value).values(**update_dict)
        try:
            with SessionLocal() as session:
                session.execute(stmt)
                #session.commit()
        except Exception as e:
            print("Error updating product:", e)
            raise

    @staticmethod
    def delete_product(pk_value: int, pk_column=Product.c.PRODUCT_ID):
        stmt = Product.delete().where(pk_column == pk_value)
        try:
            with SessionLocal() as session:
                session.execute(stmt)
                #session.commit()
        except Exception as e:
            print("Error deleting product:", e)
            raise

    @staticmethod
    def get_products_by_class_df(class_id: int):
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
                return df
        except Exception as e:
            print("Error getting products by calss as df:", e)
            raise

# -------------------------GENERIC TABLE METHODS -----------------------------------------------------
    @staticmethod
    def read(tname: str, *args, **kwargs):
        """
        Read rows from a table as a DataFrame.

        Usage:
        - read("products") → all rows
        - read("products", 123) → filter by PK = 123
        - read("products", PRODUCT_CLASS_CODE=45) → filter by column
        - read("products", PRODUCT_CLASS_CODE=45, STATUS="ACTIVE") → multiple filters
        """
        tableC = Tables[tname]
        if tableC is None:
            return None

        stmt = select(tableC)

        # Case 1: single positional arg -> assume primary key
        if len(args) == 1 and not kwargs:
            keylist = list(tableC.primary_key)
            if (len(keylist) > 0):
                pk_col = list(tableC.primary_key)[0]
            else:
                pk_col = tableC.c[next(iter(tableC.columns.keys()))]
            stmt = stmt.where(pk_col == args[0])
            if(BaseDBTransformer.debug):
                print( " Colum extracted = ", stmt)

        # Case 2: keyword args -> filters on given columns
        elif kwargs:
            for col_name, val in kwargs.items():
                if col_name in tableC.c:
                    stmt = stmt.where(tableC.c[col_name] == val)
                else:
                    raise ValueError(f"Column {col_name} not found in table {tname}")
        try:
            if(BaseDBTransformer.debug):
                print("Read : Fetching in table ", str, " Criteria = ", stmt)
            with SessionLocal() as session:
                conn = session.connection()
                df = pd.read_sql_query(stmt, conn)
                return df
        except Exception as e:
            print(f"Error reading {tname}:", e)
            raise
    
    @staticmethod
    def readf(tname: str, **filters):
        """Read rows with Django-style filters (supports gte, lte, contains, etc)."""
        tableC = Tables[tname]
        if tableC is None:
            return None
        stmt = select(tableC)

        for col_expr, val in filters.items():
            # Split column and operator
            if "__" in col_expr:
                col_name, op = col_expr.split("__", 1)
            else:
                col_name, op = col_expr, "eq"

            if op not in C.OPERATORS:
                raise ValueError(f"Unsupported filter operator: {op}")

            # Apply operator
            stmt = stmt.where(C.OPERATORS[op](tableC.c[col_name], val))
        try:
            if(BaseDBTransformer.debug):
                print("ReadF : Fetching in table ", str, " Criteria = ", stmt)
            with SessionLocal() as session:
                conn = session.connection()
                df = pd.read_sql_query(stmt, conn)
                return df
        except Exception as e:
            print("Error getting filtered rows as df:", e)
            raise

    @staticmethod
    def insert_(session: Session, tname: str, data: dict):
        """Insert a row into the given table."""
        tableC = Tables[tname]
        if tableC is None:
            raise ValueError(f"Table {tname} not found")

        stmt = tableC.insert().values(**data)
        try:
            # with SessionLocal() as session:
                result = session.execute(stmt)
                #session.commit()
                return result.inserted_primary_key[0] if result.inserted_primary_key else None
        except Exception as e:
            print(f"Error insert_ into {tname}: with {dict}", e)
            raise

    @staticmethod
    def insert(tname: str, data: dict):
        try:
            with transaction() as session:
                return BaseDBTransformer.insert_(session, tname, data)
        except Exception as e:
            print(f"Error insert into {tname}: with {dict}", e)
            raise

    @staticmethod
    def update_(session: Session, tname: str, pk_value, update_dict: dict, pk_column: str = None):
        """Update a row in the given table using primary key (or provided column)."""
        tableC = Tables[tname]
        if tableC is None:
            raise ValueError(f"Table {tname} not found")

        # Default: assume first PK column if not explicitly passed
        if pk_column is None:
            pk_column = list(tableC.primary_key.columns)[0].name

        stmt = tableC.update().where(tableC.c[pk_column] == pk_value).values(**update_dict)
        try:
           # with SessionLocal() as session:
                session.execute(stmt)
                #session.commit()
        except Exception as e:
            print(f"Error update_ {tname}:{pk_value} with {update_dict} on {pk_column} ", e)
            raise
        return pk_value
    
    @staticmethod
    def update(tname: str, pk_value, update_dict: dict, pk_column: str = None):
        try:
            with transaction() as session:
                return BaseDBTransformer.update_(session, tname, pk_value, update_dict, pk_column)
        except Exception as e:
            print(f"Error update {tname}:{pk_value} with {update_dict} on {pk_column} ", e)
            raise         

    @staticmethod
    def delete_(session: Session, tname: str, pk_value, pk_column: str = None):
        """Delete a row from the given table using primary key (or provided column)."""
        tableC = Tables[tname]
        if tableC is None:
            raise ValueError(f"Table {tname} not found")

        if pk_column is None:
            pk_column = list(tableC.primary_key.columns)[0].name

        stmt = tableC.delete().where(tableC.c[pk_column] == pk_value)
        try:
            # with SessionLocal() as session:
                session.execute(stmt)
                #session.commit()
        except Exception as e:
            print(f"Error delete_ {tname}:{pk_column} with {pk_value}", e)
            raise
        return pk_value

    @staticmethod
    def delete(tname: str, pk_value, pk_column: str = None):
        try:
            with transaction() as session:
                return BaseDBTransformer.delete_(session, tname, pk_value, pk_column)
        except Exception as e:
            print(f"Error delete {tname}:{pk_column} with {pk_value}", e)
            raise         

    def upsert_(session: Session, tname: str, update_dict: dict, inc_key: str):
        """
        Generic MySQL update that inserts values into table.
        If a duplicate key occurs, increments the given key column.
        
        :param tname: SQLAlchemy Table object
        :param update_dict: dict of column_name -> value for insert
        :param inc_key: str name of the column to increment on duplicate
        """

        tableC = Tables[tname]
        if tableC is None:
            raise ValueError(f"Table {tname} not found")
        if inc_key not in tableC.c:
            raise ValueError(f"Column {inc_key} not found in {tableC.name}")

        # stmt = tableC.insert().values(**update_dict)
        stmt = insert(tableC).values(**update_dict)
        # increment column on duplicate key
        stmt = stmt.on_duplicate_key_update(
            **{
                inc_key: tableC.c[inc_key] + stmt.inserted[inc_key]
            }
        )
        try:
            # with SessionLocal() as session:
                result = session.execute(stmt)
                #session.commit()
                return result.inserted_primary_key[0] if result.inserted_primary_key else None
        except Exception as e:
            print(f"Upsert_ is failing for {tname}:{inc_key} with {update_dict} {e}")
            raise
        return None

    def upsert(tname: str, update_dict: dict, inc_key: str):
        try:
            with transaction() as session:
                BaseDBTransformer.upsert_(session, tname, update_dict, inc_key)
        except Exception as e:
            print(f"Upsert is failing for {tname}:{inc_key} with {update_dict} {e}")
            raise
        return None