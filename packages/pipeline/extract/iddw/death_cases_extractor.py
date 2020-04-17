import pandas as pd
import os

class DeathCasesExtractor:
    """ Class to extract and clean data from sbamt project data """

    tag = "death_cases"

    def __init__(self, file_name):

        self.file_name = file_name

    def load_data(self) -> pd.DataFrame:
        """ load raw data file from """

        if not os.path.exists(self.file_name):
            raise FileNotFoundError(f"File does not exist: {self.file_name}")

        _, ext = os.path.splitext(self.file_name)
        assert ext == ".xls", "Invalid filetype attempted to load"

        return pd.read_excel(self.file_name)

    @staticmethod
    def extract_data(df: pd.DataFrame) -> pd.DataFrame:
        
        new_header = df.loc[9].copy()
        new_header[1:] = pd.to_numeric(new_header[1:], downcast='integer')
        new_df = df.drop(range(10))
        new_df = new_df.rename(columns=new_header)
        
        return new_df

    @classmethod
    def pipe(cls, filename: str) -> pd.DataFrame:
        """ run entire transform pipeline """

        # initialise parser
        extractor = cls(filename)
        
        # get records
        raw_data = extractor.load_data()
        
        # extract relevant data
        extracted_data = extractor.extract_data(raw_data)

        return extracted_data
