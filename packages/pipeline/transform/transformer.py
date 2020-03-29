""" transform data for later use """

from packages.pipeline.basepipe.basepipe import BasePipe

class Transformer(BasePipe):

    def __init__(self, directory):
        super().__init__()

        self.input_dir = "extracted_input"
        self.output_dir = "production_input"
        self.input_filesheet = "file_sheet.json"
        self.output_filesheet = "file_sheet.json"
        self.project_directory = directory

        # verify file structure
        self._verify_file_structure()

    def pipe_data(self):

        file_list = self._load_file_list()

        for file in file_list:
            print(file)

if __name__ == "__main__":
    transformer = Transformer(
        directory="data/bmz",
    )
    transformer.pipe_data()