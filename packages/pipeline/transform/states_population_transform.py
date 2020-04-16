import os
import pandas as pd

class StatesPopulationTransformer:
    """ Class to extract relevant data from demographics data """

    tag = "states_population"

    def __init__(self, file_name):

        self.file_name = file_name

    def load_data(self) -> pd.DataFrame:
        """ load and parse xml file """

        if not os.path.exists(self.file_name):
            raise FileNotFoundError(f"File does not exist: {self.file_name}")

        _, ext = os.path.splitext(self.file_name)
        assert ext == ".csv", "Invalid filetype attempted to load"

        return pd.read_csv(self.file_name)

    def transform_document(self) -> pd.DataFrame:
        """ Parse entire file """

        return self.load_data()

    @classmethod
    def pipe(cls, filename: str) -> pd.DataFrame:
        """ run entire transform pipeline """

        transformer = cls(filename)
        data = transformer.transform_document()

        return data