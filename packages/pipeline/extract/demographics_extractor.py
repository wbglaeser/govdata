import pandas as pd
import os

class DemographicsExtractor:
    """ Class to parse XML node """

    tag = "demographics"

    def __init__(self, file_name):
        self.file_name = file_name

    def load_data(self) -> pd.DataFrame:
        """ Load data into dataframe """

        if not os.path.exists(self.file_name):
            raise FileNotFoundError(f"File does not exist: {self.file_name}")

        return pd.read_csv(self.file_name, encoding="ISO-8859-1", skiprows=6, sep=";")

    @staticmethod
    def extract_demographics(df: pd.DataFrame) -> pd.DataFrame:
        """ Cut relevant rows """

        # cut header and footer rows
        df = df[:36]

        # rename NaN Columns
        rename_columns = {
            "Unnamed: 0":"date",
            "Unnamed: 1":"gender",
            "Unnamed: 2":"age_group",
        }
        df.rename(columns=rename_columns, inplace=True)

        return df

    @classmethod
    def pipe(cls, file_name: str) -> list:
        """ classmethod to provide more succinct access to parser """

        # initialise parser
        extractor = cls(file_name)

        # parse records
        extracted_data = extractor.load_data()

        # extract relevant data
        extracted_data = extractor.extract_demographics(extracted_data)

        return extracted_data