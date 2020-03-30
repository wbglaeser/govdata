""" Class to laad load raw input files """

from packages.toolkit.data_handler_base.data_handler_base import BaseDataHandler
from packages.pipeline.parser.parser import XmlParser

class Extractor(BaseDataHandler):

    def __init__(self, directory, engine):
        super().__init__(
            input_dir="raw_input",
            output_dir="extracted_input",
            input_filesheet="file_sheet.json",
            output_filesheet="file_sheet.json",
            project_directory=directory
        )

        self.engine = engine

        # verify file structure
        self._verify_file_structure()

    def pipe_data(self):
        """ load and parse data before storing result in extracted data directory """
        file_list = self._load_file_list()

        for file in file_list:

            records = self.engine.pipe(file)
            self.store_data(records, file)

if __name__ == "__main__":
    extractor = Extractor(
        directory="data/bmz",
        engine=XmlParser
    )
    extractor.pipe_data()









