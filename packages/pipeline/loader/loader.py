""" Class to laad load raw input files """

import os
import json

from packages.pipeline.parser.parser import XmlParser

class Loader:

    IN_DIR = "raw_input"
    OUT_DIR = "extracted_input"
    FILESHEET = "file_sheet.json"

    def __init__(self, directory, parser):

        self.parser = parser
        self.directory = directory

        # verify file structure
        self._verify_file_structure()

    def _verify_file_structure(self):
        """ make sure necessary directories exist """

        if not os.path.exists(self.directory):
            raise FileNotFoundError(f"Provided data directory does not exist: {self.directory}")

        self.input_directory = os.path.join(self.directory, self.IN_DIR)
        if not os.path.exists(self.input_directory):
            raise FileNotFoundError(f"Provided data directory does not exist: {self.input_directory}")

        self.out_directory = os.path.join(self.directory, self.OUT_DIR)
        if not os.path.exists(self.out_directory):
            print("Target directory does not exist. Trying to create ...")

    def _load_file_list(self) -> list:
        """ extract list of files to be loaded """

        file_sheet_path = os.path.join(self.input_directory, self.FILESHEET)
        if not os.path.exists(file_sheet_path):
            raise FileNotFoundError("File-sheet does not exist")

        with open(file_sheet_path) as fp:
            file_sheet = json.load(fp)

        file_list = file_sheet["files"]
        file_list_full_path = [os.path.join(self.input_directory, fname) for fname in file_list]

        return file_list_full_path

    def store_data(self, records: list, filename: str):
        """ store extracted data in output directory """

        # prepare file name
        file_name_short = filename.split("/")[-1]
        file_name_trunk, _ = os.path.splitext(file_name_short)
        file_name_json = file_name_trunk + ".json"

        # full file path
        file_name_full = os.path.join(self.out_directory, file_name_json)

        with open(file_name_full, "w") as fp:
            json.dump(records, fp, indent=4)


    def load_data(self):
        """ load and parse data before storing result in extracted data directory """

        file_list = self._load_file_list()

        for file in file_list:

            records = self.parser.parse(file)
            self.store_data(records, file)

if __name__ == "__main__":
    loader = Loader(
        directory="bmz_data/data",
        parser=XmlParser
    )
    loader.load_data()









