import pytest
import db.dbutil
import db.dbsql
import inspect
from dao.base_transformer import BaseDBTransformer
import db.constants as C
from sqlalchemy.exc import IntegrityError
# from service.product_service import ProductService
from service.discount_service import DiscountService
from dao.product_transformer import ProductTransformer
import pandas as pd
import numpy as np
import pandas.testing as pdt

@pytest.mark.parametrize("cust_id", range(5, 50, 15))
def test_3030_get_discounts( cust_id: int, debug=False):
    print(f"{inspect.currentframe().f_code.co_name}")  
    d_used = DiscountService.list_used_codes(cust_id)
    d_eligible =  DiscountService.list_eligible_codes(cust_id)
    d_active =  DiscountService.list_active_codes(cust_id)
    d_all = DiscountService.list_all_codes(cust_id)
    if( (d_used is None) & (d_eligible is None) & (d_active is None) ):
        # Nothing to check it is True
        assert(True)
    else:
        d_agg = pd.concat([d_used, d_eligible, d_active], ignore_index=True)
        pdt.assert_frame_equal (
            d_all.sort_values(by=list(d_all.columns)).reset_index(drop=True),
            d_agg.loc[:, list(d_agg.columns)].sort_values(by=list(d_agg.columns)).reset_index(drop=True),
            check_dtype=False
        )
    if(debug):
        print("Success Discount codes all and aggregates lists are same for a a given customer.")

@pytest.mark.parametrize("cust_id", range(5, 50, 15))
def test_3050_update_discounts( cust_id: int, debug=False):

    print(f"{inspect.currentframe().f_code.co_name}")  
    d_ori = DiscountService.list_eligible_codes(cust_id)
    print(d_ori)
    for i, (idx, row) in enumerate(d_ori.iterrows()):
        print( row[C.did], row[C.dpct], row[C.dst], row[C.ordid], row[C.custid], row[C.dcode])
        dstatus = 0
        dpercent = (i+1) % 25
        dcode = row[C.dcode] + "_t"
        d_updated = DiscountService.enable_discount_codes(row[C.did], dpercent, dcode, dstatus, True)
        d_updated_read = BaseDBTransformer.readdf(C.discounts, C.did, [row[C.did]])
        if( (d_updated is None) | (d_updated_read is None)):
            print( "Discount code updation/read back failed for Discount ID = ", row[C.did], d_updated, d_updated_read)
            assert(False)
        else:
            cmp = d_updated_read.loc[ (d_updated_read[C.did] == row[C.did]) & (d_updated_read[C.dst] == dstatus ) 
                                     & (d_updated_read[C.dcode] == dcode) & (d_updated_read[C.dpct] == dpercent )  ]
            if ( len(cmp) <= 0):
                print( "Discount code updation failed for Discount ID = ", row[C.did])
                assert(False)
            d_updated = DiscountService.enable_discount_codes(row[C.did], row[C.dpct], row[C.dcode], row[C.dst], True)
    d_end = DiscountService.list_eligible_codes(cust_id)    
    print( d_end )
    pdt.assert_frame_equal (
        d_end.sort_values(by=list(d_end.columns)).reset_index(drop=True),
        d_ori.loc[:, list(d_ori.columns)].sort_values(by=list(d_ori.columns)).reset_index(drop=True),
        check_dtype=False
    )
    if(debug):
        print("Success add updating of discount codes, percentage and status. verified")