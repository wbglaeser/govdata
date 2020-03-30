from collections import namedtuple
import os
import json

import pandas as pd

NewRecord = namedtuple("NewRecord",
                       """
                       identifier
                       title
                       reporting_org
                       description
                       participation_org_1
                       participation_org_2
                       participation_org_3
                       participation_org_4
                       participation_org_5
                       owner_org
                       activity_status
                       recipient_country
                       location_city
                       location_description
                       location_coordinates
                       sector_1
                       sector_2
                       sector_3
                       sector_4
                       budget  
                       """)

class BmzTransformer:
    """ Class to extract relevant data from bmz project data """

    def __init__(self, file_name):

        self.file_name = file_name

        # load data
        self.data = self._load_data()

    def _load_data(self) -> dict:
        """ load and parse xml file """

        if not os.path.exists(self.file_name):
            raise FileNotFoundError(f"File does not exist: {self.file_name}")

        _, ext = os.path.splitext(self.file_name)
        assert ext == ".json", "Invalid filetype attempted to load"

        with open(self.file_name, "r") as fp:
            data = json.load(fp)

        return data

    def _iterate_through_records(self) -> list:
        """ iterate through records to extract data """

        assert isinstance(self.data, list), "Input data does not conform to list datatype"

        all_records = []

        for record in self.data:

            new_record_tuple = self._transform_record(record)
            all_records.append(new_record_tuple._asdict())

        return all_records

    @staticmethod
    def _convert_to_dataframe(records: list) -> pd.DataFrame:
        """ convert list of dictionaries into dataframe """
        return pd.DataFrame(records)

    @staticmethod
    def __extract_identifier(rec) -> str:
        return rec["iati-identifier"]

    @staticmethod
    def __extract_multilang_dict(rec: dict, field: str) -> str:
        try:
            item = rec[field][0]
            try:
                result = item["de"]
            except KeyError:
                try:
                    result = item["en"]
                except KeyError:
                    result = "Invalid language"
        except KeyError:
            result = "None"

        return result

    @staticmethod
    def __extract_description(rec: dict) -> str:
        try:
            item = rec["description"][0]
            try:
                result = item[0]["de"]
            except KeyError:
                try:
                    result = item[0]["en"]
                except KeyError:
                    result = "Invalid language"
        except KeyError:
            result = "None"

        return result

    @staticmethod
    def __extract_participating_record(rec: dict) -> dict:
        part_org_list = ["org_1", "org_2", "org_3", "org_4", "org_5"]
        result = dict(zip(part_org_list, ["", "", "", "", ""]))
        try:
            items = rec["participating-org"]
            for idx, org in enumerate(items):
                result[part_org_list[idx]] = org[0]
        except KeyError:
            pass
        return result

    @staticmethod
    def __extract_key_value(rec: dict, field: str) -> str:
        return rec[field]

    @staticmethod
    def __extract_nested_dict(rec: dict, field: str) -> str:
        return rec[field][0]

    @staticmethod
    def __extract_coordinates(rec: dict) -> tuple:
        longitude = rec["location"][6]["point"][0]["pos"].split()[0]
        latitude = rec["location"][6]["point"][0]["pos"].split()[1]
        return (longitude, latitude)

    @staticmethod
    def __extract_sector(rec: dict) -> dict:
        sec_list = ["sec_1", "sec_2", "sec_3", "sec_4"]
        result = dict(zip(sec_list, ["", "", "", ""]))
        try:
            items = rec["sector"]
            try:
                result["sec_1"] = items["code"]
            except TypeError:
                for idx, sec in enumerate(rec["sector"]):
                    if idx < 4:
                        result[sec_list[idx]] = sec["code"]
        except KeyError:
            pass

        return result

    @staticmethod
    def __extract_budget(rec: dict) -> float:
        try:
            items = rec["budget"]
            try:
                result = items[-1][2]["value"]
            except KeyError:
                result = items[2]["value"]
        except KeyError:
            result = -9999

        return result


    def _transform_record(self, record: dict) -> NewRecord:
        """ retrieve relevant information from individual records """

        # retrieve identifier
        identifier = self.__extract_identifier(record)

        # reporting org
        reporting_org = self.__extract_multilang_dict(record, "reporting-org")

        # title
        title = self.__extract_multilang_dict(record, "title")

        # description
        description = self.__extract_description(record)

        # participation orgs. currently we accept up to five participating organisations
        participating_dict = self.__extract_participating_record(record)

        # owner org
        owner_org = self.__extract_multilang_dict(record["other-identifier"][0], "owner-org")

        # activity status
        activity_status = self.__extract_key_value(record["activity-status"], "code")

        # recipient country
        recipient_country = self.__extract_nested_dict(record, "recipient-country")

        # location city
        location_city = self.__extract_nested_dict(record["location"][2], "name")

        # location description
        location_description = self.__extract_nested_dict(record["location"][4], "activity-description")

        # coordinates
        location_coordinates = self.__extract_coordinates(record)

        # sector
        sec_dict = self.__extract_sector(record)

        # budget
        budget = self.__extract_budget(record)

        # populate record dictionary
        new_record = NewRecord(
            identifier,
            title,
            reporting_org,
            description,
            participating_dict["org_1"],
            participating_dict["org_2"],
            participating_dict["org_3"],
            participating_dict["org_4"],
            participating_dict["org_5"],
            owner_org,
            activity_status,
            recipient_country,
            location_city,
            location_description,
            location_coordinates,
            sec_dict["sec_1"],
            sec_dict["sec_2"],
            sec_dict["sec_3"],
            sec_dict["sec_4"],
            budget,
        )

        return new_record

    @classmethod
    def pipe(cls, filename: str) -> pd.DataFrame:
        """ run entire transform pipeline """

        transformer = BmzTransformer(filename)
        all_records = transformer._iterate_through_records()
        all_records_df = transformer._convert_to_dataframe(all_records)

        return all_records_df




