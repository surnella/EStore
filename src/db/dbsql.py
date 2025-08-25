from sqlalchemy import create_engine, MetaData, URL
from sqlalchemy.orm import sessionmaker
import db.constants as C

# print("Executing dbsql and creating engine.")
# Connection string (only thing you change if DB changes!)
url_object = URL.create(
    drivername="mysql+mysqlconnector",
    username="root",
    password="root@321",  # raw password, no need to encode
    host="localhost",
    database="ORDERS"
)

engine = create_engine(url_object, echo=False)

SessionLocal = sessionmaker(bind=engine)

# Reflect the schema
metadata = MetaData()
metadata.reflect(bind=engine)

Tables = { x: metadata.tables[x] for x in C.TABLES_STRS }
# Access reflected tables
Product = metadata.tables["product"]
ProductClass = metadata.tables["product_class"]
Address = metadata.tables["address"] 
Customer = metadata.tables["online_customer"]
Orders = metadata.tables["order_header"]
Items = metadata.tables["order_items"]
Shipper = metadata.tables["shipper"]
Cart = metadata.tables["cart_items"]
Discount = metadata.tables["discounts"]

