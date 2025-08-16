import unittest
from app import app

class TestDiscountAPI(unittest.TestCase):

    def setUp(self):
        self.client = app.test_client()

    def test_create_discount(self):
        resp = self.client.post("/discounts", json={"discount_id": 1, "percentage": 10})
        self.assertEqual(resp.status_code, 200)

    def test_get_discount(self):
        self.client.post("/discounts", json={"discount_id": 2, "percentage": 20})
        resp = self.client.get("/discounts/2")
        self.assertEqual(resp.status_code, 200)

    def test_update_discount(self):
        self.client.post("/discounts", json={"discount_id": 3, "percentage": 15})
        resp = self.client.put("/discounts/3", json={"discount_id": 3, "percentage": 25})
        self.assertEqual(resp.status_code, 200)

    def test_delete_discount(self):
        self.client.post("/discounts", json={"discount_id": 4, "percentage": 30})
        resp = self.client.delete("/discounts/4")
        self.assertEqual(resp.status_code, 200)

#----------------------------Keep this------------------------------

import unittest
import pandas as pd
from discount_transformer import DiscountTransformer

class TestDiscountTransformer(unittest.TestCase):

    def setUp(self):
        self.df = pd.DataFrame({
            "discount_id": [301, 302],
            "product_id": [1, 2],
            "discount_pct": [10, 20]
        })
        self.transformer = DiscountTransformer()

    def test_fit(self):
        self.transformer.fit(self.df)
        self.assertTrue(True)

    def test_transform(self):
        result = self.transformer.transform(self.df)
        self.assertIsInstance(result, pd.DataFrame)

if __name__ == "__main__":
    unittest.main()
