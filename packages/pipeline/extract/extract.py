""" Class to laad load raw input files """

from packages.toolkit.data_handler_base.data_handler_base import BaseDataHandler
from packages.pipeline.extract.xml_extractor import XmlExtractor
from packages.pipeline.extract.google_mob_extractor import GoogleExtractor
from packages.pipeline.extract.mpost_extractor import MPostExtractor
from packages.pipeline.extract.demographics_extractor import DemographicsExtractor
from packages.pipeline.extract.states_population_extractor import StatesPopulationExtractor 
from packages.pipeline.extract.medical_distribution_extractor import MedicalDistributionExtractor  

class Extractor(BaseDataHandler):

    def __init__(self, engine):
        super().__init__(
            data_tag=engine.tag,
            stage="extraction"
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
            return records

if __name__ == "__main__":
    extractor = Extractor(
        engine=MedicalDistributionExtractor
    )
    rec = extractor.pipe_data()









