from collections import Counter
import pandas as pd
import json
import os

""" Class contains parser for xml data files. Input is xml data file and json file describing parsing scheme """

class GoogleParser:
    """ Class to parse XML node """

    def __init__(self, file_name):
        self.file_name = file_name

    def load_data(self) -> pd.DataFrame:
        """ Load data into dataframe """

        if not os.path.exists(self.file_name):
            raise FileNotFoundError(f"File does not exist: {self.file_name}")

        return pd.read_csv(self.file_name)

    @classmethod
    def pipe(cls, file_name: str) -> list:
        """ classmethod to provide more succinct access to parser """

        # initialise parser
        parser = cls(file_name)

        # parse records
        parsed_data = parser.load_data()

        return parsed_data
