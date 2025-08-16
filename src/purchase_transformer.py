class PurchaseTransformer(BaseDBTransformer):
    def create(self, purchase_id, cart_id, total_amount):
        new_row = pd.DataFrame([{
            "PurchaseID": purchase_id,
            "CartID": cart_id,
            "TotalAmount": total_amount
        }])
        self.data = pd.concat([self.transform(), new_row], ignore_index=True)
        self.save()

    def read(self, purchase_id=None):
        df = self.transform()
        if purchase_id:
            return df[df["PurchaseID"] == purchase_id]
        return df

    def update(self, purchase_id, **kwargs):
        df = self.transform()
        for key, val in kwargs.items():
            df.loc[df["PurchaseID"] == purchase_id, key] = val
        self.data = df
        self.save()

    def delete(self, purchase_id):
        df = self.transform()
        df = df[df["PurchaseID"] != purchase_id]
        self.data = df
        self.save()
