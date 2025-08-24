import pandas as pd
from src.base_transformer import BaseDBTransformer
import src.constants as C

class DiscountTransformer(BaseDBTransformer):
    def __init__(self, connection_string):
        super().__init__(table_name="discounts", connection_string=connection_string)

    def create(self, *args, **kwargs):
        if len(args) == 1 and isinstance(args[0], pd.DataFrame):
            return super().create(args[0])
        elif len(args) > 1:
            discount_id, usr_id, discount_percent, discount_code, discount_status = args
            new_row = pd.DataFrame([{
                C.did: discount_id,
                C.usrid: usr_id,
                C.dpct: discount_percent,
                C.dcode: discount_code,
                C.dst: discount_status
            }])
            return super().create(new_row)
        else:
            raise ValueError("Invalid arguments to create()")

    def update_df(self, df, mode="append"):
        super().update(df, mode)

    def read(self, discount_id=None):
        df = self.transform()
        if discount_id:
            return df[df[C.did].isin(discount_id)]
        return df

    def update(self, discount_id, **kwargs):
        df = self.transform()
        for key, val in kwargs.items():
            df.loc[df[C.did] == discount_id, key] = val
        self.data = df
        self.save()

    def delete(self, discount_id):
        df = self.transform()
        df = df[df[C.did] != discount_id]
        self.data = df
        self.save()
