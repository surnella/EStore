from sqlalchemy import create_engine, MetaData, URL
from sqlalchemy.orm import sessionmaker

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

# Access reflected tables
Product = metadata.tables["product"]
ProductClass = metadata.tables["product_class"]
