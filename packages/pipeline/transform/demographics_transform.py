import os

import pandas as pd

class DemographicsTransformer:
    """ Class to extract relevant data from demographics data """

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
    def filter_data(df: pd.DataFrame) -> pd.DataFrame:
        """ Filter roughly """

        return df

    @staticmethod
    def stack_states(df: pd.DataFrame) -> pd.DataFrame:
        """ stack variables """

        states = [
            'Schleswig-Holstein',
            'Hamburg',
            'Niedersachsen',
            'Bremen',
            'Nordrhein-Westfalen',
            'Hessen',
            'Rheinland-Pfalz',
            'Baden-Württemberg',
            'Bayern',
            'Saarland',
            'Berlin',
            'Brandenburg',
            'Mecklenburg-Vorpommern',
            'Sachsen',
            'Sachsen-Anhalt',
            'Thüringen'
        ]

        # loop through states
        df_list = []
        for state in states:
            excerpt = ["date", "gender", "age_group", state]
            _df = df[excerpt]
            _df["state"] = state
            _df.rename(columns={state:"value"}, inplace=True)
            df_list.append(_df)

        # concate states
        df_stacked = pd.concat(df_list)
        df_stacked.reset_index(inplace=True, drop=True)

        return df_stacked

    @staticmethod
    def format_df(df: pd.DataFrame) -> pd.DataFrame:
        """ Format dataframe """

        # reorder columns
        new_order = [
            "date",
            "state",
            "gender",
            "age_group",
            "value"
        ]
        df = df[new_order]

        return df

    def transform_document(self) -> pd.DataFrame:
        """ Parse entire file """

        df = self.load_data()
        df = self.filter_data(df)
        df = self.stack_states(df)
        df = self.format_df(df)

        return df

    @classmethod
    def pipe(cls, filename: str) -> pd.DataFrame:
        """ run entire transform pipeline """

        transformer = cls(filename)
        data = transformer.transform_document()

        return data