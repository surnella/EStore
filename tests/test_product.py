from flask import Blueprint, request, jsonify

product_bp = Blueprint("product", __name__)
products = {}

@product_bp.route("/products", methods=["POST"])
def create_product():
    data = request.json
    pid = data["product_id"]
    products[pid] = data
    return jsonify(products[pid]), 200

@product_bp.route("/products/<int:pid>", methods=["GET"])
def get_product(pid):
    if pid in products:
        return jsonify(products[pid]), 200
    return jsonify({"error": "Not found"}), 404

@product_bp.route("/products/<int:pid>", methods=["PUT"])
def update_product(pid):
    if pid in products:
        products[pid] = request.json
        return jsonify(products[pid]), 200
    return jsonify({"error": "Not found"}), 404

@product_bp.route("/products/<int:pid>", methods=["DELETE"])
def delete_product(pid):
    if pid in products:
        deleted = products.pop(pid)
        return jsonify(deleted), 200
    return jsonify({"error": "Not found"}), 404




#----------------------------Keep this------------------------------
import unittest
import pandas as pd
from product_transformer import ProductTransformer

class TestProductTransformer(unittest.TestCase):

    def setUp(self):
        # Setup sample DataFrame
        self.df = pd.DataFrame({
            "product_id": [1, 2],
            "name": ["apple", "banana"],
            "price": [10.0, 20.0]
        })
        self.transformer = ProductTransformer()

    def test_fit(self):
        self.transformer.fit(self.df)
        self.assertTrue(True)  # just checking fit runs

    def test_transform(self):
        result = self.transformer.transform(self.df)
        self.assertIsInstance(result, pd.DataFrame)

if __name__ == "__main__":
    unittest.main()



