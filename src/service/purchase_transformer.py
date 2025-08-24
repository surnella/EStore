import pandas as pd
from src.base_transformer import BaseDBTransformer
import src.constants as C

class PurchaseTransformer(BaseDBTransformer):
    """Transformer for handling purchase data."""
    def create(self, *args, **kwargs):
        if len(args) == 1 and isinstance(args[0], pd.DataFrame):
            return super().create(args[0])
        elif len(args) > 1:
            purchase_id, cart_id, discount_id, total_amount = args
            new_row = pd.DataFrame([{
                C.prcid: purchase_id,
                C.crtid: cart_id,
                C.did: discount_id,
                C.tamt: total_amount
                }])
            return super().create(new_row)
        else:
            raise ValueError("Invalid arguments to create()")

    def update_df(self, df, mode="append"):
        super().update(df, mode)

    def read(self, purchase_id=None):
        # print(f"Purchase Transformer read invoked purchase id = {purchase_id}")
        df = self.transform()
        if purchase_id:import pandas as pd
from src.base_transformer import BaseDBTransformer
import src.constants as C

class PurchaseTransformer(BaseDBTransformer):
    """Transformer for handling purchase data with MySQL persistence."""

    def __init__(self, engine, table_name="order_header"):
        super().__init__(engine, table_name)

    def create(self, *args):
        if len(args) == 1 and isinstance(args[0], pd.DataFrame):
            return super().create(args[0])
        elif len(args) > 1:
            purchase_id, cart_id, discount_id, total_amount = args
            new_row = pd.DataFrame([{
                C.prcid: purchase_id,
                C.crtid: cart_id,
                C.did: discount_id,
                C.tamt: total_amount
            }])
            return super().create(new_row)
        else:
            raise ValueError("Invalid arguments to create()")
        return self
