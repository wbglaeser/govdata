import os

class SbamtTransformer:
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
            tmp = tmp.replace(char, new)
        
        return tmp

    @staticmethod
    def format_data(file: str) -> pd.DataFrame:
        
        content = file.split('\n')
        
        replacement = {' ':'',
                        ',':'',
                        '.':''}
        
        content = [replace_chars(line, replacement) for line in content]
        content = [line.split('\t') for line in content ] 
        
        return pd.DataFrame(mat[1:], columns=mat[0])

    def transform_document(self) -> pd.DataFrame:
        """ Parse entire file """

        file = self.load_data()
        df = self.format_data(file)

        return df

    @classmethod
    def pipe(cls, filename: str) -> pd.DataFrame:
        """ run entire transform pipeline """

        transformer = cls(filename)
        data = transformer.transform_document()

        return data
