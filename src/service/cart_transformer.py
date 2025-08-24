import pandas as pd
from src.base_transformer import BaseDBTransformer
import src.constants as C

class CartTransformer(BaseDBTransformer):
    def __init__(self, engine, table_name="cart"):
        super().__init__(engine, table_name)

    def create(self, *args, **kwargs):
        # Case 1: DataFrame provided
        if len(args) == 1 and isinstance(args[0], pd.DataFrame):
            return super().create(args[0])

        # Case 2: individual fields provided
        elif len(args) > 1:
            cart_id, product_id, quantity, usrid = args
            new_row = pd.DataFrame([{
                C.crtid: cart_id,
                C.pid: product_id,
                C.qnt: quantity,
                C.usrid: usrid
            }])
            return super().create(new_row)

        else:
            raise ValueError("Invalid arguments to create()")

    def update_df(self, df, mode="append"):
        super().update(df, mode)

    def read(self, cart_id=None):
        df = self.transform()
        if cart_id:
            return df[df[C.crtid].isin(cart_id)]
        return df

    def update(self, cart_id, product_id, quantity):
        df = self.transform()
        df.loc[(df[C.crtid] == cart_id) & (df[C.pid] == product_id), C.qnt] = quantity
        self.data = df
        self.save()

    def delete(self, cart_id, product_id=None):
        df = self.transform()
        if product_id:
            df = df[~((df[C.crtid] == cart_id) & (df[C.pid] == product_id))]
        else:
            df = df[df[C.crtid] != cart_id]
        self.data = df
        self.save()
