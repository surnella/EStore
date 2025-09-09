import pandas as pd
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import Table, MetaData, delete, select, update, func, text
from sqlalchemy.orm import Session
from sqlalchemy.dialects.mysql import insert
from sqlalchemy.exc import IntegrityError
from db.dbsql import SessionLocal, Tables
from db.dbutil import transaction
from pathlib import Path

# Import all the tables as classes. Putting here make it available for all the upper layers. 
from db.dbsql import Product, ProductClass, Address, Customer, Items, Orders, Shipper, Cart, Discount
import db.constants as C

class BaseDBTransformer:
    debug=False
    prj_root = None

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

    def readdf(tname: str, pk_column: str, values: list):
        tableC = Tables[tname]
        if tableC is None:
            return None
        with SessionLocal() as session:
            column = tableC.c[pk_column]
            stmt = tableC.select().where(column.in_(values))
            result = session.execute(stmt)
            data = [dict(row._mapping) for row in result]  # row._mapping is dict-like
            df = pd.DataFrame(data)
            if(BaseDBTransformer.debug):
                print(df)
            return df

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
    def tlen_(session: Session, tname: str):
        tableC = Tables[tname]
        if tableC is None:
            raise ValueError(f"Table {tname} not found")
        stmt = select(func.count()).select_from(tableC)
        return session.execute(stmt).scalar_one()

    @staticmethod
    def tlen(tname: str):
        try:
            with transaction() as session:
                return BaseDBTransformer.tlen_(session, tname)
        except Exception as e:
            print(f"Error getting number of records {tname}", e)
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
            result = session.execute(stmt)
            if( result.rowcount == 0):
                return None
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

    @staticmethod
    def delete_all_(session: Session, tname: str):
        """Delete a row from the given table using primary key (or provided column)."""
        tableC = Tables[tname]
        if tableC is None:
            raise ValueError(f"Table {tname} not found")

        stmt = tableC.delete()
        try:
            result = session.execute(stmt)
            if( result.rowcount == 0):
                return 0
        except Exception as e:
            print(f"Error delete_all_ {tname} ", e)
            raise
        return result.rowcount
    
    @staticmethod
    def delete_all(tname: str):
        try:
            with transaction() as session:
                return BaseDBTransformer.delete_all_(session, tname)
        except Exception as e:
            print(f"Error delete ALL {tname} ", e)
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

    @staticmethod
    def run_sql_script_(session: Session, script_path: str):
        """Run a SQL script file using the given session."""
        try:
            with open(script_path, "r") as f:
                sql_script = f.read()

            # Some DB drivers don’t allow multiple statements at once
            for stmt in sql_script.split(";"):
                stmt = stmt.strip()
                if stmt:
                    session.execute(text(stmt))
                    # print(stmt)

        except Exception as e:
            print(f"Error running SQL script {script_path}: ", e)
            raise

    @staticmethod
    def run_sql_script(script_path: str):
        try:
            with transaction() as session:
                BaseDBTransformer.run_sql_script_(session, script_path)
        except Exception as e:
            print(f"Error running SQL script {script_path}: ", e)
            raise
    
    @staticmethod
    def purge():
        from pathlib import Path
        print("Starting to purge all data and reinintialize it with the default and test data\n")
        # Remove all the data
        retval = BaseDBTransformer.delete_all(C.items)
        retval = BaseDBTransformer.delete_all(C.cart)
        retval = BaseDBTransformer.delete_all(C.orders)
        retval = BaseDBTransformer.delete_all(C.discounts)
        retval = BaseDBTransformer.delete_all(C.prd)
        retval = BaseDBTransformer.delete_all(C.prdc)
        
        #Populate the defaults data - Run the SQL script file 
        ROOT = BaseDBTransformer.get_project_root()  # go up from tests/ to project root
        sql_path = ROOT / "data" / "ordersdb.sql"
        print( sql_path, "\n", ROOT)
        BaseDBTransformer.run_sql_script(sql_path)

    @staticmethod
    def get_project_root():
        fullpath = Path(__file__).resolve()
        # print(fullpath)
        # print(ROOT.parent, ROOT.parents[0])
        # print(ROOT.parent.parent, ROOT.parents[1])
        # print(ROOT.parent.parent.parent, ROOT.parents[2])
        # import importlib.resources as resources
        # print( resources.files("EStore").joinpath("data/ordersdb.sql") )
        for p in fullpath.parents:
            if( p / "pyproject.toml").exists():
                BaseDBTransformer.prj_root = Path(p)
                # print(f"Project root folder found {p}")
                break
        return BaseDBTransformer.prj_root