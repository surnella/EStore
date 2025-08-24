# from fastapi import FastAPI
# from service import ProductService

# app = FastAPI()

# @app.get("/products/class/{class_id}")
# def get_products(class_id: int):
#     products = ProductService.list_products_in_class(class_id)
#     return {"products": products}


from flask import Flask, request, jsonify
from product_transformer import ProductDBTransformer
from cart_transformer import CartDBTransformer
from purchase_transformer import PurchaseDBTransformer
from discount_transformer import DiscountDBTransformer

app = Flask(__name__)

# Instantiate transformers (each works on its own DB)
product_transformer = ProductDBTransformer("data/products.DB")
cart_transformer = CartDBTransformer("data/cart.DB")
purchase_transformer = PurchaseDBTransformer("data/purchases.DB")
discount_transformer = DiscountDBTransformer("data/discounts.DB")

# -------------------------------
# Product API
# -------------------------------
@app.route("/products", methods=["GET"])
def get_products():
    return jsonify(product_transformer.read_DB().to_dict(orient="records"))

@app.route("/products", methods=["POST"])
def add_product():
    data = request.json
    product_transformer.create(data)
    return jsonify({"message": "Product added successfully!"}), 201

@app.route("/products/<int:row_id>", methods=["PUT"])
def update_product(row_id):
    data = request.json
    product_transformer.update(row_id, data)
    return jsonify({"message": "Product updated successfully!"})

@app.route("/products/<int:row_id>", methods=["DELETE"])
def delete_product(row_id):
    product_transformer.delete(row_id)
    return jsonify({"message": "Product deleted successfully!"})


# -------------------------------
# Cart API
# -------------------------------
@app.route("/cart", methods=["GET"])
def get_cart():
    return jsonify(cart_transformer.read_DB().to_dict(orient="records"))

@app.route("/cart", methods=["POST"])
def add_to_cart():
    data = request.json
    cart_transformer.create(data)
    return jsonify({"message": "Item added to cart!"}), 201

@app.route("/cart/<int:row_id>", methods=["DELETE"])
def delete_from_cart(row_id):
    cart_transformer.delete(row_id)
    return jsonify({"message": "Item removed from cart!"})


# -------------------------------
# Purchase API
# -------------------------------
@app.route("/purchases", methods=["GET"])
def get_purchases():
    return jsonify(purchase_transformer.read_DB().to_dict(orient="records"))

@app.route("/purchases", methods=["POST"])
def add_purchase():
    data = request.json
    purchase_transformer.create(data)
    return jsonify({"message": "Purchase recorded!"}), 201


# -------------------------------
# Discount API
# -------------------------------
@app.route("/discounts", methods=["GET"])
def get_discounts():
    return jsonify(discount_transformer.read_DB().to_dict(orient="records"))

@app.route("/discounts", methods=["POST"])
def add_discount():
    data = request.json
    discount_transformer.create(data)
    return jsonify({"message": "Discount added!"}), 201

@app.route("/discounts/<int:row_id>", methods=["PUT"])
def apply_discount(row_id):
    data = request.json
    discount_transformer.update(row_id, data)
    return jsonify({"message": "Discount updated!"})


if __name__ == "__main__":
    app.run(debug=True)
