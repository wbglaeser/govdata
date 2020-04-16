import os
import pandas as pd

class TotalEmployedTransformer:
    """ Class to extract relevant data from demographics data """

    tag = "total_employed"

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
    def calculate_data(df: pd.DataFrame) -> pd.DataFrame:
        """ Data are presented as *1000, here they will be calculated and rounded """
        
        new_df = df.copy()

        row_index = 0
        for _, values in new_df.iterrows():

            for i in range(len(values)):
                if i == 0:
                    continue
                content = round(values.iloc[i] * 1000)
                new_df.iloc[row_index, i] = content

            row_index = row_index + 1

        return new_df

    @staticmethod
    def convert_data(df: pd.DataFrame) -> pd.DataFrame:
        
        column_types = {}
        for i in range(1991,2019):
            column_types[str(i)] = 'int32'
        
        return df.astype(column_types)

    def transform_document(self) -> pd.DataFrame:
        """ Parse entire file """
        df = self.load_data()
        df = self.calculate_data(df)
        df = self.convert_data(df)
        return df

    @classmethod
    def pipe(cls, filename: str) -> pd.DataFrame:
        """ run entire transform pipeline """

        transformer = cls(filename)
        data = transformer.transform_document()

        return data