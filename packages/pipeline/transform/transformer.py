""" transform data for later use """

from packages.toolkit.data_handler_base.data_handler_base import BaseDataHandler
from packages.pipeline.transform.bmz_transform import BmzTransformer
from packages.pipeline.transform.google_transform import GoogleTransformer
from packages.pipeline.transform.mpost_transform import MPostTransformer
from packages.pipeline.transform.demographics_transform import DemographicsTransformer
from packages.pipeline.transform.states_population_transform import StatesPopulationTransformer
from packages.pipeline.transform.medical_distribution_transform import MedicalDistributionTransformer 

class Transformer(BaseDataHandler):

    def __init__(self, engine):
        super().__init__(
            data_tag=engine.tag,
            stage="transformation"
        )

        self.engine = engine

        # verify file structure
        self._verify_file_structure()

    def pipe_data(self):

        file_list = self._load_file_list()
        print(file_list)
        for file in file_list:
            records = self.engine.pipe(file)
            self.store_data(records, file)

if __name__ == "__main__":
    transformer = Transformer(
        engine=MedicalDistributionTransformer
    )
    transformer.pipe_data()