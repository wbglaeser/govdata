""" transform data for later use """

class Transformer:

    IN_DIR = "extracted_input"
    OUT_DIR = "production_input"

    def __init__(self, directory, transformer):

        self.transformer = transformer
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

    def _