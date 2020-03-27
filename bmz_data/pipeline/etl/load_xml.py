import xml.etree.ElementTree as ET
import xmltodict
import json

# file to be imported
filename = "../../data/projects_25032020.xml"
json_filename = "../../data/projects_25032020.json"

class Parser:
    """ Class to parse XML node """

    def __init__(self, file_name, file_datasheet):

        # load data
        self.tree = self._load_tree(file_name)
        self.datasheet = self._load_datasheet(file_datasheet)

    @staticmethod
    def _load_tree(file_name: str):
        """ load and parse xml file """
        return ET.parse(file_name)

    @staticmethod
    def _load_datasheet(file_name: str):
        """ load datasheet json """

        # load datasheet
        with open(file_datasheet) as f:
            datasheet = json.load(f)

        return datasheet

    def _extract_record_list(self):
        """ from tree extract list of records """

        root = self.datasheet["root"]

        return [node for node in self.tree.iter(root)]

    def iterate_through_records(self):

        record_list = self._extract_record_list()

        for record in list(root_node):
            pass

    def parse_start_node(self, node, node_description):

        node_dict = {}
        all_records.append(node_dict)

        if node_description["type"] == "value":
            parse_value(node, node_dict)

        elif node_description["type"] == "list":
            parse_list(node, node_description["list-element"], node_dict)

        elif node_description["type"] == "dict":
            parse_dict(node, node_description["dict-element"], node_dict)

        elif node_description["type"] == "mixed-list":
            parse_mixed_list(node, node_description["mixed-list-element"], node_dict)

    def parse_list(self):
        pass




def parse_value(node, node_dict):
    if node.text:
        print("     ", node.text)
        node_dict.update(
            {node.tag: node.text}
        )

def parse_list(node, node_description, node_dict):

    for _node in list(node):

        if node_description["type"] == "dict":
            parse_dict(_node, node_description["dict-element"], node_dict)

        elif node_description["type"] == "list":
            parse_list(_node, node_description["list-element"], node_dict)

        elif node_description["type"] == "mixed-list":
            parse_mixed_list(_node, node_description["mixed-list-element"], node_dict)

        elif node_description["type"] == "value":
            parse_value(_node, node_dict)

def parse_mixed_list(node, node_description, node_dict):

    for _node in list(node):

        for element in node_description:
            if element["type"] == "list":
                try:
                    parse_list(_node, element["list-element"], node_dict)
                except: continue
            elif element["type"] == "value":
                try:
                    parse_value(_node, node_dict)
                except: continue
            elif element["type"] == "dict":
                try:
                    parse_dict(_node, element["dict-element"], node_dict)
                except: continue

def parse_dict(node, node_description, node_dict):

    if node_description["value-type"] == "text":
        key = node.get(node_description["key"])
        value = node.text
        print("     ", (key, value))

    elif node_description["value-type"] == "item":
        key = node_description["key"]
        value = node.get(node_description["key"])
        if value:
            print("     ", (key, value))

    elif node_description["value-type"] == "multiple-items":
        for key in node_description["keys"]:
            value = node.get(key)
            if value:
                print("     ", (key, value))

# parse into tree
tree = ET.parse(filename)
activities = [node for node in tree.iter(obj["root"])]

for _idx, record in enumerate(list(activities)):
    if _idx < 1:
        print("\n")
        for idx, node in enumerate(list(record)):
            if idx < 34:
                print(node.tag, ": ")
                parse_node(node, obj["fields"][node.tag])
    _idx = _idx + 1
