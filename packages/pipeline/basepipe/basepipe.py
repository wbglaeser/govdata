from abc import ABC, abstractmethod
import os
import json

class BasePipe(ABC):

    @property
    def input_dir(self):
        return self.__input_dir

    @input_dir.setter
    def input_dir(self, input_dir):
        self.__input_dir = input_dir

    @property
    def output_dir(self):
        return self.__output_dir

    @output_dir.setter
    def output_dir(self, output_dir):
        self.__output_dir = output_dir

    @property
    def input_filesheet(self):
        return self.__input_filesheet

    @input_filesheet.setter
    def input_filesheet(self, input_filesheet):
        self.__input_filesheet = input_filesheet

    @property
    def output_filesheet(self):
        return self.__output_filesheet

    @output_filesheet.setter
    def output_filesheet(self, output_filesheet):
        self.__output_filesheet = output_filesheet

    @property
    def project_directory(self):
        return self.__project_directory

    @project_directory.setter
    def project_directory(self, project_directory):
        self.__project_directory = project_directory

    def __init__(self):

        self.__input_dir = ""
        self.__output_dir = ""
        self.__input_filesheet = ""
        self.__output_filesheet = ""
        self.__project_directory = ""

    def _verify_file_structure(self):
        """ make sure necessary directories exist """

        if not os.path.exists(self.project_directory):
            print(self.project_directory)
            raise FileNotFoundError(f"Provided data directory does not exist: {self.project_directory}")

        self.input_directory = os.path.join(self.project_directory, self.input_dir)
        if not os.path.exists(self.input_directory):
            raise FileNotFoundError(f"Provided data directory does not exist: {self.input_directory}")

        self.out_directory = os.path.join(self.project_directory, self.output_dir)
        if not os.path.exists(self.out_directory):
            print("Target directory does not exist. Trying to create ...")

    def _load_file_list(self) -> list:
        """ extract list of files to be loaded """

        file_sheet_path = os.path.join(self.input_directory, self.__input_filesheet)
        if not os.path.exists(file_sheet_path):
            raise FileNotFoundError("File-sheet does not exist")

        with open(file_sheet_path) as fp:
            file_sheet = json.load(fp)

        file_list = file_sheet["files"]
        file_list_full_path = [os.path.join(self.input_directory, fname) for fname in file_list]

        return file_list_full_path

    def _update_filesheet(self, filename: str):
        """ update filesheet to reflect extracted files """

        filesheet_path = os.path.join(self.out_directory, self.__output_filesheet)
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

    def store_data(self, records: list, filename: str):
        """ store extracted data in output directory """

        # prepare file name
        file_name_short = filename.split("/")[-1]
        file_name_trunk, _ = os.path.splitext(file_name_short)
        file_name_json = file_name_trunk + ".json"

        # full file path
        file_name_full = os.path.join(self.out_directory, file_name_json)

        # store data
        with open(file_name_full, "w") as fp:
            json.dump(records, fp, indent=4)

        self._update_filesheet(file_name_json)

    @abstractmethod
    def pipe_data(self):
        pass
