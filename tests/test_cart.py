import unittest
from app import app

class TestCartAPI(unittest.TestCase):

    def setUp(self):
        self.client = app.test_client()

    def test_create_cart(self):
        resp = self.client.post("/carts", json={"cart_id": 1, "items": []})
        self.assertEqual(resp.status_code, 200)

    def test_get_cart(self):
        self.client.post("/carts", json={"cart_id": 2, "items": ["apple"]})
        resp = self.client.get("/carts/2")
        self.assertEqual(resp.status_code, 200)

    def test_update_cart(self):
        self.client.post("/carts", json={"cart_id": 3, "items": ["banana"]})
        resp = self.client.put("/carts/3", json={"cart_id": 3, "items": ["banana", "mango"]})
        self.assertEqual(resp.status_code, 200)

    def test_delete_cart(self):
        self.client.post("/carts", json={"cart_id": 4, "items": ["orange"]})
        resp = self.client.delete("/carts/4")
        self.assertEqual(resp.status_code, 200)

#----------------------------Keep this------------------------------

import unittest
import pandas as pd
from cart_transformer import CartTransformer

class TestCartTransformer(unittest.TestCase):

    def setUp(self):
        self.df = pd.DataFrame({
            "cart_id": [101, 102],
            "user_id": [1, 2],
            "total": [100.0, 200.0]
        })
        self.transformer = CartTransformer()

    def test_fit(self):
        self.transformer.fit(self.df)
        self.assertTrue(True)

    def test_transform(self):
        result = self.transformer.transform(self.df)
        self.assertIsInstance(result, pd.DataFrame)

if __name__ == "__main__":
    unittest.main()
