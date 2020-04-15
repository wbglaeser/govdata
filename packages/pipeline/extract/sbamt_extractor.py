import pandas as pd
import os

class SbamtExtractor:
    """ Class to extract and clean data from sbamt project data """

    def __init__(self, file_name):

        self.file_name = file_name

    def load_data(self) -> str:
        """ load raw data file from """

        if not os.path.exists(self.file_name):
            raise FileNotFoundError(f"File does not exist: {self.file_name}")

        _, ext = os.path.splitext(self.file_name)
        assert ext == ".txt", "Invalid filetype attempted to load"

        with open(self.file_name, 'r') as fd:
            file = fd.read()

        return file

    @staticmethod
    def replace_chars(to_be_replaced: str, replacement: dict) -> str:
        
        tmp = to_be_replaced

        for old, new in replacement.items():
            tmp = tmp.replace(old, new)
        
        return tmp

    @staticmethod
    def extract_data(file: str) -> pd.DataFrame:
        
        content = file.split('\n')
        
        replacement = {' ':'',
                        ',':'',
                        '.':''}
        
        content = [SbamtExtractor.replace_chars(line, replacement) for line in content]
        content = [line.split('\t') for line in content ] 
        
        return pd.DataFrame(content[1:], columns=content[0])

    @classmethod
    def pipe(cls, filename: str) -> pd.DataFrame:
        """ run entire transform pipeline """

        # initialise parser
        extractor = cls(filename)
        
        # get records
        raw_data = extractor.load_data()
        
        # extract relevant data
        extracted_data = extractor.extract_data(raw_data)

        return extracted_data
