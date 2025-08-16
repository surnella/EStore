class CartTransformer(BaseDBTransformer):
    def create(self, cart_id, product_id, quantity):
        new_row = pd.DataFrame([{
            "CartID": cart_id,
            "ProductID": product_id,
            "Quantity": quantity
        }])
        self.data = pd.concat([self.transform(), new_row], ignore_index=True)
        self.save()

    def read(self, cart_id=None):
        df = self.transform()
        if cart_id:
            return df[df["CartID"] == cart_id]
        return df

    def update(self, cart_id, product_id, quantity):
        df = self.transform()
        df.loc[(df["CartID"] == cart_id) & (df["ProductID"] == product_id), "Quantity"] = quantity
        self.data = df
        self.save()

    def delete(self, cart_id, product_id=None):
        df = self.transform()
        if product_id:
            df = df[~((df["CartID"] == cart_id) & (df["ProductID"] == product_id))]
        else:
            df = df[df["CartID"] != cart_id]
        self.data = df
        self.save()
