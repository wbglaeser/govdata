from collections import Counter
import xml.etree.ElementTree as ET
import json
import os

""" Class contains parser for xml data files. Input is xml data file and json file describing parsing scheme """

class XmlParser:
    """ Class to parse XML node """

    def __init__(self, file_name):

        self.file_name = file_name

        # load data
        self.tree = self._load_tree()
        print(type(self.tree))

        self.datasheet = self._load_datasheet()

    def _load_tree(self):
        """ load and parse xml file """

        if not os.path.exists(self.file_name):
            raise FileNotFoundError(f"File does not exist: {self.file_name}")

        return ET.parse(self.file_name)

    def _load_datasheet(self) -> dict:
        """ load datasheet json """

        file_name_trunk, extension = os.path.splitext(self.file_name)
        data_sheet = file_name_trunk + ".json"

        if not os.path.exists(data_sheet):
            raise (FileNotFoundError, f"Datasheet does not exist for: {file_name_trunk}")

        with open(data_sheet) as f:
            data_sheet = json.load(f)

        return data_sheet

    def _extract_record_list(self):
        """ from tree extract list of records """

        root = self.datasheet["root"]

        return [node for node in self.tree.iter(root)]

    def save_records(self):
        """ Save parsed set of records into json file """
        with open("test_save.json", "w") as fp:
            json.dump(self.records_all, fp, indent=4)

    def iterate_through_records(self) -> list:
        """ iterate through all xml records """
        record_list = self._extract_record_list()

        records_all = []

        for record in record_list:

            new_record = self.parse_record(record)

            records_all.append(new_record)

        return records_all

    def parse_record(self, record) -> list:
        """ parse individual record """
        record_dict = {}

        node_counter = Counter([node.tag for node in list(record)])

        for node in record:

            current_node = node.tag

            try:
                node_description = self.datasheet["fields"][current_node]

                # check for children
                if node_description["type"] == "value":
                    result = self.parse_value(node, node_description["value-element"])

                elif node_description["type"] == "list":
                    result = self.parse_list(node, node_description["list-element"])

                elif node_description["type"] == "dict":
                    result = self.parse_dict(node, node_description["dict-element"])

                elif node_description["type"] == "mixed-list":
                    result = self.parse_mixed_list(node, node_description["mixed-list-element"])
                else: result = ""

            except (KeyError, IndexError) as e:
                print(e)
                result = ""

            if node_description["tag-value"] == 1:
                kv = self.parse_dict_item(node, node_description["tag-element"])

                if isinstance(result, list):
                    result.append(kv)
                elif isinstance(result, dict):
                    result = [result, kv]

            if node_counter[node.tag] > 1:
                if node.tag in record_dict.keys():
                    record_dict[node.tag].append(result)
                else:
                    record_dict[node.tag] = [result]
            else:
                record_dict.update({
                    node.tag: result
                })

        return record_dict

    def parse_list(self, node, node_description):
        """ parse list of nodes where nodes are identical in their structure """

        node_result = []

        for _node in list(node):

            if node_description["type"] == "dict":
                _node_result = self.parse_dict(_node, node_description["dict-element"])

            elif node_description["type"] == "list":
                _node_result = self.parse_list(_node, node_description["list-element"])

            elif node_description["type"] == "mixed-list":
                _node_result = self.parse_mixed_list(_node, node_description["mixed-list-element"])

            elif node_description["type"] == "value":
                _node_result = self.parse_value(_node, node_description["value-element"])
            else: _node_result = ""

            node_result.append(_node_result)

        if node_description["named"] == 1:
            node_result = {
                node.tag: node_result
            }

        return node_result

    def parse_mixed_list(self, node, node_description):
        """ parse list of nodes where nodes type vary along the list """

        node_result = []

        for idx, _node in enumerate(list(node)):

            _node_result = None

            if node_description[idx]["type"] == "list":
                _node_result = self.parse_list(_node, node_description[idx]["list-element"])

            elif node_description[idx]["type"] == "dict":
                _node_result = self.parse_dict(_node, node_description[idx]["dict-element"])

            elif node_description[idx]["type"] == "value":
                _node_result = self.parse_value(_node, node_description[idx]["value-element"])

            node_result.append(_node_result)

        return node_result


    def parse_dict(self, node, node_description):
        """ parse nested node as dictionary """

        node_dict = {}

        if node_description["value-type"] == "text":
            kv = self.parse_dict_text(node, node_description)
            node_dict.update(kv)

        elif node_description["value-type"] == "item":
            kv = self.parse_dict_item(node, node_description)
            node_dict.update(kv)

        elif node_description["value-type"] == "multiple-items":
            for key in node_description["keys"]:
                kv = self.parse_dict_mitems(node, key)
                node_dict.update(kv)

        if node_description["named"] == 1:
            node_dict = {
                node.tag: node_dict
            }

        return node_dict

    @staticmethod
    def parse_value(node, node_description):
        """ parse leaf node with text value """
        if node_description["named"] == 1:
            return {node.tag: node.text}

        return node.text

    @staticmethod
    def parse_dict_mitems(node, key):
        """ parse leaf node containing multiple items """
        kv_pairs = {}
        if node.get(key):
            kv_pairs.update({
                key: node.get(key)
            })
        return kv_pairs

    @staticmethod
    def parse_dict_text(node, node_description):
        """ parse leaf node containing key and test value"""
        kv_pair = {
            node.get(node_description["key"]):node.text
        }
        return kv_pair

    @staticmethod
    def parse_dict_item(node, node_description):
        """ parse leaf node containing key and value in descriptor """
        kv_pair = {
            node_description["key"]: node.get(node_description["key"])
        }
        return kv_pair

    @classmethod
    def parse(cls, file_name: str) -> list:
        """ classmethod to provide more succinct access to parser """

        # initialise parser
        parser = cls(file_name)

        # parse records
        parsed_records = parser.iterate_through_records()

        return parsed_records
