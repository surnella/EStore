import pandas as pd
from src.base_transformer import BaseDBTransformer
import src.constants as C

class ProductTransformer(BaseDBTransformer):
    def __init__(self, engine):
        # Tell BaseDBTransformer to use MySQL engine + products table
        super().__init__(engine, table_name="product")

    def create(self, *args):
        if len(args) == 1 and isinstance(args[0], pd.DataFrame):
            return super().create(args[0])
        elif len(args) > 1:
            pid, pname, ptyp, mrp, pavl, plen, wd, ht, wt = args
            new_row = pd.DataFrame([{
                C.pid: pid,
                C.pname: pname,
                C.ptyp: ptyp,
                C.mrp: mrp,
                C.pavl: pavl,
                C.wt: wt,
                C.plen: plen,
                C.wd: wd,
                C.ht: ht
            }])
            return super().create(new_row)
        else:
            raise ValueError("Invalid arguments to create()")
