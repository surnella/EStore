class ProductListTransformer(BaseDBTransformer):
    def create(self, product_id, product_type, sugar, weight, area, mrp):
        """Add a new product."""
        new_row = pd.DataFrame([{
            "ProductID": product_id,
            "ProductType": product_type,
            "SugarContent": sugar,
            "Weight": weight,
            "Area": area,
            "MRP": mrp
        }])
        self.data = pd.concat([self.transform(), new_row], ignore_index=True)
        self.save()

    def read(self, product_id=None):
        """Fetch all products or one by ID."""
        df = self.transform()
        if product_id:
            return df[df["ProductID"] == product_id]
        return df

    def update(self, product_id, **kwargs):
        """Update product attributes."""
        df = self.transform()
        for key, val in kwargs.items():
            df.loc[df["ProductID"] == product_id, key] = val
        self.data = df
        self.save()

    def delete(self, product_id):
        """Delete a product."""
        df = self.transform()
        df = df[df["ProductID"] != product_id]
        self.data = df
        self.save()
