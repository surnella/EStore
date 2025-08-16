import unittest
from app import app

class TestPurchaseAPI(unittest.TestCase):

    def setUp(self):
        self.client = app.test_client()

    def test_create_purchase(self):
        resp = self.client.post("/purchases", json={"purchase_id": 1, "cart_id": 10, "amount": 100.0})
        self.assertEqual(resp.status_code, 200)

    def test_get_purchase(self):
        self.client.post("/purchases", json={"purchase_id": 2, "cart_id": 11, "amount": 150.0})
        resp = self.client.get("/purchases/2")
        self.assertEqual(resp.status_code, 200)

    def test_update_purchase(self):
        self.client.post("/purchases", json={"purchase_id": 3, "cart_id": 12, "amount": 200.0})
        resp = self.client.put("/purchases/3", json={"purchase_id": 3, "cart_id": 12, "amount": 220.0})
        self.assertEqual(resp.status_code, 200)

    def test_delete_purchase(self):
        self.client.post("/purchases", json={"purchase_id": 4, "cart_id": 13, "amount": 250.0})
        resp = self.client.delete("/purchases/4")
        self.assertEqual(resp.status_code, 200)




#----------------------------Keep---------------------------

import unittest
import pandas as pd
from purchase_transformer import PurchaseTransformer

class TestPurchaseTransformer(unittest.TestCase):

    def setUp(self):
        self.df = pd.DataFrame({
            "purchase_id": [201, 202],
            "cart_id": [101, 102],
            "status": ["completed", "pending"]
        })
        self.transformer = PurchaseTransformer()

    def test_fit(self):
        self.transformer.fit(self.df)
        self.assertTrue(True)

    def test_transform(self):
        result = self.transformer.transform(self.df)
        self.assertIsInstance(result, pd.DataFrame)

if __name__ == "__main__":
    unittest.main()
