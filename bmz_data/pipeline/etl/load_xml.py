from packages.pipeline.parser.parser import XmlParser

# file to be imported
import os
print(os.getcwd())

filename = "bmz_data/data/raw_input/projects_25032020.xml"
json_filename = "bmz_data/data/raw_input/projects_25032020.json"




if __name__ == "__main__":
    parser = XmlParser(filename, json_filename)
    parser.iterate_through_records()
    print(parser.records_all[1])
    parser.save_records()
