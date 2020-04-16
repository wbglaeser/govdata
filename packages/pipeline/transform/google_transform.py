from collections import namedtuple
import os
import json

import pandas as pd

class GoogleTransformer:
    """ Class to extract relevant data from bmz project data """

    tag = "google"

    def __init__(self, file_name):

        self.file_name = file_name

    def load_data(self) -> dict:
        """ load and parse xml file """

        if not os.path.exists(self.file_name):
            raise FileNotFoundError(f"File does not exist: {self.file_name}")

        _, ext = os.path.splitext(self.file_name)
        assert ext == ".csv", "Invalid filetype attempted to load"

        return pd.read_csv(self.file_name)

    @staticmethod
    def filter_data(df: pd.DataFrame) -> pd.DataFrame:
        """ Filter roughly """

        # filter by value
        filter_dict = {
            "Country":"Germany"
        }

        for (key, val) in filter_dict.items():
            df = df[df[key] == val]

        # drop columns
        drop_cols = ["headline_percent", "codes", "Country"]
        df.drop(drop_cols, axis=1, inplace=True)

        return df

    @staticmethod
    def wide_to_long(df: pd.DataFrame) -> pd.DataFrame:
        """ Reshape data from wide to long format """

        # prepare date columns
        old_cols = [col for col in list(df) if "2020" in col]
        new_cols = ["date_" + col.replace("-", "") for col in list(df) if "2020" in col]
        rename_dict = dict(zip(old_cols, new_cols))
        df.rename(columns=rename_dict, inplace=True)

        # reshape data
        df = pd.wide_to_long(df, stubnames="date_", i=["location", "category"], j="date")
        df.reset_index(inplace=True)

        # rename column
        df.rename(columns={"date_":"mob_score"}, inplace=True)

        return df

    @staticmethod
    def parse_to_datetime(df: pd.DataFrame) -> pd.DataFrame:
        """ Parse date column to date datatype """
        df["date"] = pd.to_datetime(df["date"], format="%Y%m%d", errors='coerce')
        return df

    @staticmethod
    def format_df(df: pd.DataFrame) -> pd.DataFrame:
        """ Format dataframe """
        rename_dict = {
            "location": "state",
            "category": "key",
            "mob_score": "value"
        }
        df.rename(columns=rename_dict, inplace=True)

        return df

    def transform_document(self) -> pd.DataFrame:
        """ Parse entire file """

        df = self.load_data()
        df = self.filter_data(df)
        df = self.wide_to_long(df)
        df = self.parse_to_datetime(df)
        df = self.format_df(df)

        return df

    @classmethod
    def pipe(cls, filename: str) -> pd.DataFrame:
        """ run entire transform pipeline """

        transformer = cls(filename)
        data = transformer.transform_document()

        return data




