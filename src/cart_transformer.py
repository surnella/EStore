import pandas as pd
from src.base_transformer import BaseDBTransformer
import src.constants as C

class CartTransformer(BaseDBTransformer):

    def create(self, *args, **kwargs):
        # Case 1: DataFrame provided
        print("Cart Transformer create invoked with args:", args, len(args))
        if len(args) == 1 and isinstance(args[0], pd.DataFrame):
            return super().create(args[0])

        # Case 2: individual fields provided
        elif len(args) > 1:
            cart_id, product_id, quantity, usrid = args
            new_row = pd.DataFrame([{
                C.crtid: cart_id,
                C.pid: product_id,
                C.qnt: quantity,
                C.usrid:usrid
            }])
            return super().create(new_row)

        else:
            raise ValueError("Invalid arguments to create()")

    def update_df(self, df, mode="append"):
        super().update(df, mode)

    def read(self, cart_id=None):
        print(f"Cart Transformer read invoked cart id = {cart_id}")
        df = self.transform()
        if cart_id:
            return df[df[C.crtid] == cart_id]
        return df

    def update(self, cart_id, product_id, quantity):
        print(f"Cart Transformer update invoked cart id = {cart_id}, product id = {product_id} quantity = {quantity}")
        df = self.transform()
        df.loc[(df[C.crtid] == cart_id) & (df[C.pid] == product_id), C.qnt] = quantity
        self.data = df
        self.save()

    def delete(self, cart_id, product_id=None):
        df = self.transform()
        print(f"Cart Transformer delete invoked cart id = {cart_id}, product id = {product_id}")
        if product_id:
            df = df[~((df[C.crtid] == cart_id) & (df[C.pid] == product_id))]
        else:
            df = df[df[C.crtid] != cart_id]
        self.data = df
        self.save()