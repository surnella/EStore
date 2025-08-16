class DiscountTransformer(BaseDBTransformer):
    def create(self, discount_id, product_id, discount_percent):
        new_row = pd.DataFrame([{
            "DiscountID": discount_id,
            "ProductID": product_id,
            "DiscountPercent": discount_percent
        }])
        self.data = pd.concat([self.transform(), new_row], ignore_index=True)
        self.save()

    def read(self, discount_id=None):
        df = self.transform()
        if discount_id:
            return df[df["DiscountID"] == discount_id]
        return df

    def update(self, discount_id, **kwargs):
        df = self.transform()
        for key, val in kwargs.items():
            df.loc[df["DiscountID"] == discount_id, key] = val
        self.data = df
        self.save()

    def delete(self, discount_id):
        df = self.transform()
        df = df[df["DiscountID"] != discount_id]
        self.data = df
        self.save()
