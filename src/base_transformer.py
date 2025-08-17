import pandas as pd
from pathlib import Path
# from sklearn.base import BaseEstimator, TransformerMixin

# class BaseDBTransformer(BaseEstimator, TransformerMixin):
class BaseDBTransformer(object):
    def __init__(self, file_path: str):
        self.file_path = Path(file_path)
        self.data = None
        # print("Base DB Object Transformer invoked with file path:", self.file_path)

        if not self.file_path.exists():
            pd.DataFrame().to_csv(self.file_path, index=False)

    def create(self, df: pd.DataFrame):
        """Default create using dataframe"""
        # print("Base Transformer create invoked for dataframe")
        self.data = pd.concat([self.transform(), df], ignore_index=True)
        self.save()
    
    def save(self):
        """Persist current dataframe to DB."""
        # print("Base Transformer save invoked")
        if self.data is not None:
            self.data.to_csv(self.file_path, index=False)

        if not self.file_path.exists():
            pd.DataFrame().to_csv(self.file_path, index=False)

    def fit(self, X=None, y=None):
        """Load DB into memory (X,y ignored for sklearn compatibility)."""
        # print("Base Transformer fit invoked")
        self.data = pd.read_csv(self.file_path)
        return self

    def transform(self, X=None):
        """Return current dataframe for pipeline usage."""
        # print("Base Transformer transform invoked")
        if self.data is None:
            self.fit()
        return self.data.copy()

    def update(self, new_data: pd.DataFrame, mode="append"):
        """Custom method: append or overwrite data in DB."""
        # print(f"Base Transformer update invoked with mode = {mode}")
        if self.data is None:
            self.fit()

        if mode == "append":
            self.data = pd.concat([self.data, new_data], ignore_index=True)
        elif mode == "overwrite":
            self.data = new_data
        else:
            raise ValueError("mode must be 'append' or 'overwrite'")

        self.data.to_csv(self.file_path, index=False)
        return self
