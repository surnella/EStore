import pandas as pd
from pathlib import Path
from sklearn.base import BaseEstimator, TransformerMixin

class BaseDBTransformer(BaseEstimator, TransformerMixin):
    def __init__(self, file_path: str):
        self.file_path = Path(file_path)
        self.data = None

        # ensure DB existsimport pandas as pd
from pathlib import Path
from sklearn.base import BaseEstimator, TransformerMixin

class BaseDBTransformer(BaseEstimator, TransformerMixin):
    def __init__(self, file_path: str):
        self.file_path = Path(file_path)
        self.data = None

        # ensure DB exists
        if not self.file_path.exists():
            pd.DataFrame().to_DB(self.file_path, index=False)

    def fit(self, X=None, y=None):
        """Load DB into memory."""
        self.data = pd.read_DB(self.file_path)
        return self

    def transform(self, X=None):
        """Return current dataframe for pipeline usage."""
        if self.data is None:
            self.fit()
        return self.data.copy()

    def save(self):
        """Persist current dataframe to DB."""
        if self.data is not None:
            self.data.to_DB(self.file_path, index=False)

        if not self.file_path.exists():
            pd.DataFrame().to_DB(self.file_path, index=False)

    def fit(self, X=None, y=None):
        """Load DB into memory (X,y ignored for sklearn compatibility)."""
        self.data = pd.read_DB(self.file_path)
        return self

    def transform(self, X=None):
        """Return current dataframe for pipeline usage."""
        if self.data is None:
            self.fit()
        return self.data.copy()

    def update(self, new_data: pd.DataFrame, mode="append"):
        """Custom method: append or overwrite data in DB."""
        if self.data is None:
            self.fit()

        if mode == "append":
            self.data = pd.concat([self.data, new_data], ignore_index=True)
        elif mode == "overwrite":
            self.data = new_data
        else:
            raise ValueError("mode must be 'append' or 'overwrite'")

        self.data.to_DB(self.file_path, index=False)
        return self
