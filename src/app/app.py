from fastapi import FastAPI
from app.routers import product_router, cart_router
#, discount_router, purchase_router

app = FastAPI(title="EStore API")

# Include routers
app.include_router(product_router.router, prefix="/products", tags=["Products"])
app.include_router(cart_router.router, prefix="/cart", tags=["Cart"])
# app.include_router(discount_router.router, prefix="/discounts", tags=["Discounts"])
# app.include_router(purchase_router.router, prefix="/purchases", tags=["Purchases"])uvicorn
