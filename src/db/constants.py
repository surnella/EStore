
# Product-related constants
pid = "PRODUCT_ID"
pname = "PRODUCT_DESC"
ptyp = "PRODUCT_CLASS_CODE"
pavl = "PRODUCT_QUANTITY_AVAIL"
plen = "LEN"
wt = "WEIGHT"
wd = "WIDTH"
ht = "HEIGHT"
mrp = "PRODUCT_PRICE"

# Cart-related constants
custid = "CUSTOMER_ID"
custfn = "CUSTOMER_FNAME"
custln = "CUSTOMER_LNAME"

# Purchase-related constants
ordid = "ORDER_ID"

# Discount-related constants
dcode = "DISCOUNT_CODE"

qnt = "PRODUCT_QUANTITY"
dpct = "DISCOUNT_PERCENT"
did = "DISCOUNT_ID"
tamt = "TotalAmount"
dst = "DISCOUNT_STATUS"

prd =  "product"
prdc = "product_class"
address = "address"
custs = "online_customer"
orders = "order_header"
items = "order_items"
ship = "shipper"
cart = "cart_items"
discounts = "discounts"
TABLES_STRS = [
   prd, prdc, address, custs, orders, items, ship, cart, discounts
]

OPERATORS = {
    "eq": lambda col, val: col == val,
    "gte": lambda col, val: col >= val,
    "lte": lambda col, val: col <= val,
    "gt": lambda col, val: col > val,
    "lt": lambda col, val: col < val,
    "ne": lambda col, val: col != val,
    "contains": lambda col, val: col.contains(val),  # for LIKE '%val%'
    "startswith": lambda col, val: col.startswith(val),
    "endswith": lambda col, val: col.endswith(val),
}
