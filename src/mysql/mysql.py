from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import sessionmaker

# Connection string (only thing you change if DB changes!)
DATABASE_URL = "mysql+mysqlconnector://root:root@localhost/ORDERS"

engine = create_engine(DATABASE_URL, echo=False)
SessionLocal = sessionmaker(bind=engine)

# Reflect the schema
metadata = MetaData()
metadata.reflect(bind=engine)

# Access reflected tables
Product = metadata.tables["product"]
ProductClass = metadata.tables["product_class"]
