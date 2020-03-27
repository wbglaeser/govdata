from collections import Counter
import xml.etree.ElementTree as ET
import json

# file to be imported
filename = "../../data/projects_25032020.xml"
json_filename = "../../data/projects_25032020.json"

class Node:

    name = ""
    container = None

    def __init__(self, name, container_type):
        self.name = name

        self._initialise_container(container_type)

    def _initialise_container(self, container_type):
        if container_type == "list":
            self.container = []
        elif container_type == "dict":
            self.container = {}

    @classmethod
    def new(cls, name, container_type):
        return cls(name, container_type)

    def append_container(self, item):
        assert isinstance(item, list), "Inadequate extension operation attempted"
        self.container.append(item)

    def update_container(self, item):
        assert isinstance(item, dict), "Inadequate extension operation attempted"
        self.container.udpate(item)

class Parser:
    """ Class to parse XML node """

    def __init__(self, file_name, file_datasheet):

        # load data
        self.tree = self._load_tree(file_name)
        self.datasheet = self._load_datasheet(file_datasheet)

        # initialise record ledger
        self.records_all = []

    @staticmethod
    def _load_tree(file_name: str):
        """ load and parse xml file """
        return ET.parse(file_name)

    @staticmethod
    def _load_datasheet(file_datasheet: str):
        """ load datasheet json """

        with open(file_datasheet) as f:
            datasheet = json.load(f)

        return datasheet

    def _extract_record_list(self):
        """ from tree extract list of records """

        root = self.datasheet["root"]

        return [node for node in self.tree.iter(root)]

    def save_records(self):
        with open("test_save.json", "w") as fp:
            json.dump(self.records_all, fp, indent=4)

    def iterate_through_records(self):

        record_list = self._extract_record_list()

        for record in record_list:

            new_record = self.parse_record(record)

            self.records_all.append(new_record)

    def parse_record(self, record):

        # initialise new record dictionary
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

    @staticmethod
    def parse_value(node, node_description):

        if node_description["named"] == 1:
            return {node.tag: node.text}

        return node.text

    def parse_list(self, node, node_description):

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
    def parse_dict_mitems(node, key):
        kv_pairs = {}
        if node.get(key):
            kv_pairs.update({
                key: node.get(key)
            })
        return kv_pairs

    @staticmethod
    def parse_dict_text(node, node_description):
        kv_pair = {
            node.get(node_description["key"]):node.text
        }
        return kv_pair

    @staticmethod
    def parse_dict_item(node, node_description):
        kv_pair = {
            node_description["key"]: node.get(node_description["key"])
        }
        return kv_pair

if __name__ == "__main__":
    parser = Parser(filename, json_filename)
    parser.iterate_through_records()
    print(parser.records_all[1])
    parser.save_records()
