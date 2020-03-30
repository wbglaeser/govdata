from abc import ABC

class BaseAnalysis:

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

        self.input_directory = os.path.join(self.project_directory, self.input_dir)
        if not os.path.exists(self.input_directory):
            raise FileNotFoundError(f"Provided data directory does not exist: {self.input_directory}")

        self.out_directory = os.path.join(self.project_directory, self.output_dir)
        if not os.path.exists(self.out_directory):
            print("Target directory does not exist. Trying to create ...")
            os.makedirs(self.out_directory)

    def store_data(self, records, filename: str):
        """ store extracted data in output directory """

        assert type(records) in [list, pd.DataFrame], "File has unsupported type for saving."

        # prepare file name
        file_name_short = filename.split("/")[-1]
        file_name_trunk, _ = os.path.splitext(file_name_short)
        file_path = os.path.join(self.out_directory, file_name_trunk)

        if isinstance(records, list):
            file_path_ext = file_path + ".json"
            with open(file_path_ext, "w") as fp:
                json.dump(records, fp, indent=4)

        elif isinstance(records, pd.DataFrame):
            file_path_ext = file_path + ".csv"
            records.to_csv(file_path_ext, sep=",", index=False)

        self._update_filesheet(file_path_ext)