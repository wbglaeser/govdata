import os
import re

import pandas as pd

from packages.toolkit.data_handler_base.data_handler_base import BaseDataHandler

class BmzAnalyser(BaseDataHandler):

    def __init__(self, directory: str, filename: str):
        super().__init__(
            input_dir="production_input",
            output_dir="analysis_output",
            input_filesheet="file_sheet.json",
            output_filesheet="file_sheet.json",
            project_directory=directory
        )
        self.filename = filename

        # verify file structure
        self._verify_file_structure()

    def _load_data(self) -> pd.DataFrame:
        """ load file into dataframe """

        file_path = os.path.join(self.input_directory_path, self.filename)
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File for analysis does not exist: {file_path}")

        _, ext = os.path.splitext(file_path)
        assert ext == ".csv", "Input file has data type which is currently not supported"

        return pd.read_csv(file_path)

    def filter_relevant_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        """ select relevant data fields """

        # select relevant fields
        fields = ["identifier", "location_city", "location_coordinates", "budget"]
        df = df[fields]

        # setup regex
        clutter = r"[()\']"

        # split coordinates
        df["latitude"] = df["location_coordinates"].apply(lambda x: re.sub(clutter, "", x.split(",")[0]))
        df["longitude"] = df["location_coordinates"].apply(lambda x: re.sub(clutter, "", x.split(",")[1]))

        # drop unnecessary field
        df.drop(["location_coordinates"], axis=1, inplace=True)

        # aggregate
        df = self.aggregate_data(df)

        return df

    @staticmethod
    def aggregate_data(df: pd.DataFrame) -> pd.DataFrame:
        """ aggregate budget data on coordinates """
        return df.groupby(["latitude", "longitude"])["budget"].sum().reset_index()

    def run_analysis(self):
        """ run analysis pipeline """

        df = self._load_data()
        df = self.filter_relevant_fields(df)
        df_json = df.to_dict(orient="records")
        self.store_data(df_json, self.filename)

        return df
if __name__ == "__main__":
    loc = BmzAnalyser(
        directory="data/bmz",
        filename="projects_25032020.csv"
    )
    df = loc.run_analysis()
