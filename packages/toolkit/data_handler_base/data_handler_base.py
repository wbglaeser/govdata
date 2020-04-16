import os
import json

import pandas as pd

class BaseDataHandler:

    DATASHEET_PATH = "data/CovidData/data_inventory.json"

    def __init__(self,
                 stage: str,
                 data_tag: str):

        assert stage in ["extraction", "transformation"]
        self.stage = stage

        self.data_tag = data_tag
        self.__obtain_data_structure()

    def __obtain_data_structure(self):
        """ retrieve data structure from datasheet """

        if not os.path.exists(self.DATASHEET_PATH):
            raise FileNotFoundError("Data inventory does not exist.")

        with open(self.DATASHEET_PATH, "r") as fp:
            datasheet = json.load(fp)

        data_dir = datasheet[self.data_tag]["directory"]
        self.project_directory = os.path.join("data/CovidData", data_dir)

        if self.stage == "extraction":

            self.input_files = datasheet[self.data_tag]["raw_files"]
            self.next_stage = "extracted_files"
            self.input_dir = "raw_input"
            self.output_dir = "extracted_input"

        elif self.stage == "transformation":

            self.input_files = datasheet[self.data_tag]["extracted_files"]
            self.next_stage = "production_files"
            self.input_dir = "extracted_input"
            self.output_dir = "production_input"

    def _verify_file_structure(self):
        """ make sure necessary directories exist """

        if not os.path.exists(self.project_directory):
            print(self.project_directory)
            raise FileNotFoundError(f"Provided data directory does not exist: {self.project_directory}")

        self.input_directory_path = os.path.join(self.project_directory, self.input_dir)
        if not os.path.exists(self.input_directory_path):
            raise FileNotFoundError(f"Provided data directory does not exist: {self.input_directory_path}")

        self.out_directory_path = os.path.join(self.project_directory, self.output_dir)
        if not os.path.exists(self.out_directory_path):
            print("Target directory does not exist. Trying to create ...")
            os.makedirs(self.out_directory_path)

    def _load_file_list(self) -> list:
        """ return list of files for data pipe """

        if isinstance(self.input_files, list):
            file_list = [os.path.join(self.input_directory_path, fname) for fname in self.input_files]
        elif isinstance(self.input_files, str):
            file_list = [os.path.join(self.input_directory_path, self.input_files)]

        return file_list

    def _update_data_inventory(self, filename: str):
        """ update data inventory to reflect extracted files """

        if not os.path.exists(self.DATASHEET_PATH):
            raise FileNotFoundError("Data inventory does not exist.")

        with open(self.DATASHEET_PATH, "r") as fp:
            datasheet = json.load(fp)

        datasheet[self.data_tag][self.next_stage] = filename.split("/")[-1]

        with open(self.DATASHEET_PATH, "w") as fp:
            json.dump(datasheet, fp, indent=4)

    def store_data(self, records, filename: str):
        """ store extracted data in output directory """

        assert type(records) in [list, pd.DataFrame], "Data type currently not supported for saving."

        # prepare file name
        file_name_short = filename.split("/")[-1]
        file_name_trunk, _ = os.path.splitext(file_name_short)
        file_path = os.path.join(self.out_directory_path, file_name_trunk)

        if isinstance(records, list):
            file_path_ext = file_path + ".json"
            with open(file_path_ext, "w") as fp:
                json.dump(records, fp, indent=4)

        elif isinstance(records, pd.DataFrame):
            file_path_ext = file_path + ".csv"
            records.to_csv(file_path_ext, sep=",", index=False)

        self._update_data_inventory(file_path_ext)

