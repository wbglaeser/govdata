import os
import json

import pandas as pd

class BaseDataHandler:

    def __init__(self,
                 input_dir: str,
                 output_dir: str,
                 input_filesheet: str,
                 output_filesheet: str,
                 project_directory: str):

        self.input_dir = input_dir
        self.output_dir = output_dir
        self.input_filesheet = input_filesheet
        self.output_filesheet = output_filesheet
        self.project_directory = project_directory

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
        """ extract list of files to be loaded """

        file_sheet_path = os.path.join(self.input_directory_path, self.input_filesheet)
        if not os.path.exists(file_sheet_path):
            raise FileNotFoundError("File-sheet does not exist")

        with open(file_sheet_path) as fp:
            file_sheet = json.load(fp)

        file_list = file_sheet["files"]
        file_list_full_path = [os.path.join(self.input_directory_path, fname) for fname in file_list]

        return file_list_full_path

    def _update_filesheet(self, filename: str):
        """ update filesheet to reflect extracted files """

        filesheet_path = os.path.join(self.out_directory_path, self.output_filesheet)
        if not os.path.exists(filesheet_path):
            print("Filesheet for output data does not exist currently. Creating file... ")

            with open(filesheet_path, "w") as fp:
                empty_file_list = {"files":[]}
                json.dump(empty_file_list, fp, indent=4)

        with open(filesheet_path, "r") as fp:
            file_sheet = json.load(fp)

        if filename not in file_sheet["files"]:
            file_sheet["files"].append(filename)

        with open(filesheet_path, "w") as fp:
            json.dump(file_sheet, fp, indent=4)

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

        self._update_filesheet(file_path_ext)

