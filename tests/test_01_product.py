import pytest
import db.dbutil
import db.dbsql
import inspect
from dao.base_transformer import BaseDBTransformer
import db.constants as C
from sqlalchemy.exc import IntegrityError
from service.product_service import ProductService
import pandas as pd
import pandas.testing as pdt
from pathlib import Path

nprdcs = 50
products = []
product_class = []
debug=True
nprds = nprdcs+1

@staticmethod
def test_0010_data_clean_start():
    BaseDBTransformer.purge()

def test_0100_data_creation_product_class():
    print(f"{inspect.currentframe().f_code.co_name}")
    product_class.clear()
    for i in range(0,nprdcs):
        pc_dict = {}
        pc_dict[C.ptyp] = 4001 + i
        pc_dict[C.pdesc] = "Testing product class " + str(i)
        product_class.append(pc_dict)
    assert((i+1)==len(product_class))
    if(debug):
        print( "Created products class = ", len(product_class))

def test_0101_data_creation_products():
    print(f"{inspect.currentframe().f_code.co_name}")
    products.clear()
    for i in range(0,nprds):
        p_dict = {}
        p_dict[C.pid] = 1000 + i
        p_dict[C.pname] = "Testing Product " + str(i)
        p_dict[C.ptyp] = 4001 + i
        p_dict[C.pavl] = 300
        p_dict[C.plen] = 75
        p_dict[C.wt] = 71 + i
        p_dict[C.ht] = 42 + i
        p_dict[C.wd] = 13 + i
        p_dict[C.mrp] = 790 + i
        products.append(p_dict)
    assert ((i+1) == len(products))
    if(debug):
        print( "Created products = ", len(products))
    return

def test_1000_delete_product():
    print(f"{inspect.currentframe().f_code.co_name}")
    p_cnt_i = BaseDBTransformer.tlen(C.prd)
    rows_deleted = 0
    for i, dict in enumerate(products):
        pkey_i = dict[C.pid]
        try:
            pkey_o = BaseDBTransformer.delete(C.prd, pkey_i)
            if(pkey_o is not None):
                rows_deleted += 1
                assert(pkey_o == pkey_i)
            else:
                assert(pkey_o is None)
        except Exception as e:
            print(f"{inspect.currentframe().f_code.co_name}: Exception occured {e}. Aborting the function")
            assert(False)
    p_cnt_o = BaseDBTransformer.tlen(C.prd)
    if(debug):
        print(f"inserted product class Records: \nInitial count {p_cnt_i}. Final count{p_cnt_o}. Rows deleted {rows_deleted}")
    assert( p_cnt_i == p_cnt_o + rows_deleted)

def test_1010_delete_product_class():
    print(f"{inspect.currentframe().f_code.co_name}")
    pc_cnt_i = BaseDBTransformer.tlen(C.prdc)
    rows_deleted = 0
    for i, dict in enumerate(product_class):
        pkey_i = dict[C.ptyp]
        try:
            pkey_o = BaseDBTransformer.delete(C.prdc, pkey_i, C.ptyp)
            if(pkey_o is not None):
                rows_deleted += 1
                assert(pkey_o == pkey_i)
            else:
                assert(pkey_o is None)
        except Exception as e:
            print(f"{inspect.currentframe().f_code.co_name}: Exception occured {e}. Aborting the function")
            assert(False)
    pc_cnt_o = BaseDBTransformer.tlen(C.prdc)
    if(debug):
        print(f"deleted product class Records:\nInitial count {pc_cnt_i}, Final count{pc_cnt_o}. Rows deleted {rows_deleted}")
    assert( pc_cnt_i == pc_cnt_o + rows_deleted)

def test_1100_insert_product_class():
    print(f"{inspect.currentframe().f_code.co_name}")
    # if( db_mode ==  "fake"):
    #     assert(True)
    #     return
    pc_cnt_i = BaseDBTransformer.tlen(C.prdc)
    rows_inserted=0
    for i, dict in enumerate(product_class):
        pkey_i = dict[C.ptyp]
        try:
            pkey_o = BaseDBTransformer.insert(C.prdc, dict)
            if(pkey_o is not None):
                rows_inserted += 1
                assert(pkey_o == pkey_i)
            else:
                assert(pkey_o is None)
            if( (i>=nprdcs) & (i< nprds) ):
                pytest.fail("Excepted Integrity Error not raised.")
        except IntegrityError as e:
            if( (i>=nprdcs) & (i< nprds) ):
                msg = str(e).lower()
                assert (
                    "foreign key" in msg
                    or "constraint fails" in msg
                    or "violates foreign key constraint" in msg
                )
            else:
                if(debug):
                    print("Integrity error where it should not come, asserting false to fail it")
                assert(False)
        except Exception as e:
            print(f"{inspect.currentframe().f_code.co_name}: Exception occured {e}. Aborting the function")
            assert(False)
    pc_cnt_o = BaseDBTransformer.tlen(C.prdc)
    if(debug):
        print(f"inserted product class Records:\nInitial count {pc_cnt_i}, Final count{pc_cnt_o}. Rows inserted {rows_inserted}")
    assert(pc_cnt_i + rows_inserted == pc_cnt_o)

def test_1110_insert_product():
    print(f"{inspect.currentframe().f_code.co_name}")
    # if( db_mode ==  "fake"): 
    #     assert(True)
    #     return
    p_cnt_i = BaseDBTransformer.tlen(C.prd)
    rows_inserted=0
    for i, dict in enumerate(products):
        pkey_i = dict[C.pid]
        try:
            pkey_o = BaseDBTransformer.insert(C.prd, dict)
            if(pkey_o is not None):
                rows_inserted += 1
                assert(pkey_o == pkey_i)
            else:
                assert(pkey_o is None)
            if( (i>=nprdcs) & (i< nprds) ):
                pytest.fail("Excepted Integrity Error not raised.")
        except IntegrityError as e:
            if( (i>=nprdcs) & (i< nprds) ):
                msg = str(e).lower()
                assert (
                    "foreign key" in msg
                    or "constraint fails" in msg
                    or "violates foreign key constraint" in msg
                )
            else:
                if(debug):
                    print("Integrity error where it should not come, asserting false to fail it")
                assert(False)
        except Exception as e:
            print(f"{inspect.currentframe().f_code.co_name}: Exception occured {e}. Aborting the function")
            assert(False)
    p_cnt_o = BaseDBTransformer.tlen(C.prd)
    if(debug):
        print(f"inserted product class Records:\nInitial count {p_cnt_i}, Final count{p_cnt_o}. Rows inserted {rows_inserted}")
    assert(p_cnt_i + rows_inserted == p_cnt_o)

def test_1120_product_service():
    print(f"{inspect.currentframe().f_code.co_name}")
    prds_aggs = []
    prd_classes = ProductService.get_products_class()
    for _, (_, row) in enumerate(prd_classes.iterrows()):
        prd_class = row[C.ptyp]
        prds_c = ProductService.list_products_in_class_df(prd_class)
        prds_aggs.append(prds_c)
    prds_agg_df = pd.concat(prds_aggs, ignore_index=True)
    prds_all =ProductService.list_all_products()
    assert (len(prds_agg_df) == len(prds_all))
    pdt.assert_frame_equal(
        prds_agg_df.sort_values(by=list(prds_agg_df.columns)).reset_index(drop=True),
        prds_all.sort_values(by=list(prds_all.columns)).reset_index(drop=True),
        check_dtype=False
    )
    if(debug):
        print("Successfull test aggregation of class products with totals products\n")