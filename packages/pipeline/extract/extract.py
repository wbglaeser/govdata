""" Class to laad load raw input files """

from packages.pipeline.basepipe.basepipe import BasePipe
from packages.pipeline.parser.parser import XmlParser

class Extractor(BasePipe):

    def __init__(self, directory, engine):
        super().__init__()

        self.input_dir = "raw_input"
        self.output_dir = "extracted_input"
        self.input_filesheet = "file_sheet.json"
        self.output_filesheet = "file_sheet.json"
        self.project_directory = directory
        self.engine = engine

        # verify file structure
        self._verify_file_structure()

if __name__ == "__main__":
    extractor = Extractor(
        directory="data/bmz",
        engine=XmlParser
    )
    extractor.pipe_data()









