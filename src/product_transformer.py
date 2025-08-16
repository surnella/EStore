import pandas as pd
from src.base_transformer import BaseDBTransformer
import src.constants as C

class ProductTransformer(BaseDBTransformer):
    def create(self, *args, **kwargs):
        if len(args) == 1 and isinstance(args[0], pd.DataFrame):
            return super().create(args[0])
        elif len(args) > 1:
            product_id, product_type, sugar, weight, area, mrp = args
            new_row = pd.DataFrame([{
                C.pid: product_id,
                C.ptyp: product_type,
                C.sc: sugar,
                C.wt: weight,
                C.area: area,
                C.mrp: mrp
            }])
            return super().create(new_row)
        else:
            raise ValueError("Invalid arguments to create()")

    def update_df(self, df, mode="append"):
        super().update(df, mode)

    def read(self, product_id=None):
        print(f"Product Transformer read invoked product id = {product_id}")
        df = self.transform()
        if product_id:
            return df[df[C.pid] == product_id]
        return df

    def update(self, product_id, **kwargs):
        print(f"Product Transformer update invoked product id = {product_id}, kwargs = {kwargs}")
        df = self.transform()
        for key, val in kwargs.items():
            df.loc[df[C.pid] == product_id, key] = val
        self.data = df
        self.save()

    def delete(self, product_id):
        print(f"Product Transformer delete invoked product id = {product_id}")
        df = self.transform()
        df = df[df[C.pid] != product_id]
        self.data = df
        self.save()