import os
import pandas as pd

class DeathCasesTransformer:
    """ Class to extract relevant data from demographics data """

    tag = "death_cases"

    def __init__(self, file_name):

        self.file_name = file_name

    def load_data(self) -> pd.DataFrame:
        """ load and parse xml file """

        if not os.path.exists(self.file_name):
            raise FileNotFoundError(f"File does not exist: {self.file_name}")

        _, ext = os.path.splitext(self.file_name)
        assert ext == ".csv", "Invalid filetype attempted to load"

        return pd.read_csv(self.file_name)

    @staticmethod
    def convert_data(df: pd.DataFrame) -> pd.DataFrame:
        
        column_types = {}
        for i in range(1990,2019):
            column_types[str(i)] = 'int32'
        
        return df.astype(column_types)

    def transform_document(self) -> pd.DataFrame:
        """ Parse entire file """
        df = self.load_data()
        df = self.convert_data(df)
        return df

    @classmethod
    def pipe(cls, filename: str) -> pd.DataFrame:
        """ run entire transform pipeline """

        transformer = cls(filename)
        data = transformer.transform_document()

        return data