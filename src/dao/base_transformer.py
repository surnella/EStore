import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import Table, MetaData, delete, select, update

class BaseDBTransformer:
    def __init__(self, engine, table_name: str, pk: str = None):
        self.engine = engine
        self.table_name = table_name
        self.data = None
        self.read()
        metadata = MetaData()
        table = Table(self.table_name, metadata, autoload_with=self.engine)
        # Auto-detect primary key
        if pk is None:
            self.pk_columns = list(table.primary_key.columns)
            self.pk_name = self.pk_columns[0].name
        print( "Initialized(Base) for table: ", self.table_name , " size = ", self.data.shape, " primary keys = ", 
                self.pk_columns, "\n TBM if multiple primary keys. Handling only one PK = ", self.pk_name )

    def read(self, refresh=False):
        """Load table into memory"""
        if self.data is not None and not refresh:
            return self.data
        try:
            self.data = pd.read_sql(f"SELECT * FROM {self.table_name}", self.engine)
        except Exception:
            # If table does not exist, start with empty dataframe
            self.data = pd.DataFrame()
            self.pk_column = None
        return self

    def update(self, pk_value, params):
        """
        Update a row in the table.
        
        Args:
            pk_value: value of the primary key to match
            params: dict OR single-row DataFrame of column-value pairs
            pk_column (str, optional): primary key column name.
                                    If None, auto-detect from table.
        """
        if self.engine is None:
            self.read()

        metadata = MetaData()
        table = Table(self.table_name, metadata, autoload_with=self.engine)

        pk_column = self.pk_columns
        # auto-detect PK if not passed
        if pk_column is None:
            pk_column = list(table.primary_key.columns)[0].name

        # normalize params → dict
        if isinstance(params, pd.DataFrame):
            if len(params) != 1:
                raise ValueError("DataFrame for update must have exactly one row")
            params_dict = params.iloc[0].to_dict()

        elif isinstance(params, dict):
            params_dict = params
        else:
            raise TypeError("params must be dict or single-row DataFrame")

        try:
            stmt = (
                update(table)
                .where(getattr(table.c, pk_column) == pk_value)
                .values(**params_dict)
            )
            with self.engine.begin() as conn:
                conn.execute(stmt)

            if self.data is not None and pk_column in self.data.columns:
                mask = self.data[pk_column] == pk_value
                for col, val in params_dict.items():
                    if col in self.data.columns:
                        self.data.loc[mask, col] = val
            print(f"Updated {self.table_name} where {pk_column}={pk_value} with {params_dict}")
        except SQLAlchemyError as e:
            print("Error updating DB:", e)
            raise
        return self

    def create(self, params):
        """
        Update a row in the table.
        
        Args:
            pk_value: value of the primary key to match
            params: dict OR single-row DataFrame of column-value pairs
            pk_column (str, optional): primary key column name.
                                    If None, auto-detect from table.
        """
        if self.engine is None:
            self.read()

        metadata = MetaData()
        table = Table(self.table_name, metadata, autoload_with=self.engine)

        pk_column = self.pk_name
        # auto-detect PK if not passed
        if pk_column is None:
            pk_column = list(table.primary_key.columns)[0].name

        # normalize params → dict
        if isinstance(params, pd.DataFrame):
            if len(params) != 1:
                raise ValueError("DataFrame for update must have exactly one row")
            params_dict = params.iloc[0].to_dict()

        elif isinstance(params, dict):
            params_dict = params
        else:
            raise TypeError("params must be dict or single-row DataFrame")
        
        row = {}
        try:
            print(f"Trying to add row in {self.table_name} where {params_dict[pk_column]} with {params_dict}")
            params_dict[pk_column] = 0
            with self.engine.begin() as conn:
                result = conn.execute(table.insert().values(**params_dict))
                print ("insert: \n", params_dict, "result = \n", result)
                new_id = result.lastrowid # DB-generated PK
                # Fetch the inserted row only
                row = conn.execute(
                    select(table).where(getattr(table.c, pk_column) == new_id)
                ).fetchone()
                print (new_id, ": Row = ", row, result.lastrowid )
                # if self.data is not None and pk_column in self.data.columns:
                #     self.data = pd.concat([self.data, row], ignore_index=True)
                print(f"Added {self.table_name} where {pk_column}")
                if row is not None:
                    print (f"row: \n{row.to_dict()} = \n{self.data}")
                
        except SQLAlchemyError as e:
            print("Error updating DB:", e)
            raise 
        return row


    def get(self, pk):
        if self.engine is None:
            self.read()  # ensure engine/metadata loaded
        if pk:
            df = self.data
            return df[df[self.pk_column].isin(pk)]
        return None

    def delete(self, primary_key_value):
        """Delete a row by primary key"""
        if self.engine is None:
            self.read()  # ensure engine/metadata loaded

        metadata = MetaData()
        table = Table(self.table_name, metadata, autoload_with=self.engine)

        # Build delete statement
        stmt = ( delete(table).where(table.c[self.pk_name] == primary_key_value) )

        with self.engine.begin() as conn:
            conn.execute(stmt)

        df = self.data
        df = df[df[self.pk_name] != primary_key_value]
        self.data = df

        return self  # optional, for method chaining
