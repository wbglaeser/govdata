""" transform data for later use """

from packages.toolkit.data_handler_base.data_handler_base import BaseDataHandler
from packages.pipeline.transform.bmz_transform import BmzTransformer
from packages.pipeline.transform.google_transform import GoogleTransformer
from packages.pipeline.transform.mpost_transform import MPostTransformer

class Transformer(BaseDataHandler):

    def __init__(self, directory, engine):
        super().__init__(
            input_dir="extracted_input",
            output_dir="production_input",
            input_filesheet="file_sheet.json",
            output_filesheet="file_sheet.json",
            project_directory=directory
        )

        self.engine = engine

        # verify file structure
        self._verify_file_structure()

    def pipe_data(self):

        file_list = self._load_file_list()

        for file in file_list:
            records = self.engine.pipe(file)
            self.store_data(records, file)

if __name__ == "__main__":
    transformer = Transformer(
        directory="data/mpost",
        engine=MPostTransformer
    )
    transformer.pipe_data()