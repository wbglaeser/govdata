import xml.etree.ElementTree as ET

# file to be imported
filename = "data/projects_25032020.xml"

# parse into tree
tree = ET.parse(filename)
root = tree.getroot()
print(dir(root))
# extract first layer
node_layer_1 = set([node.tag for node in tree.iter()])

i = 0
for node in tree.iter("iati-activity"):
    i = 1 + i
    if i < 3:
        for _node in node.getchildren():
            print([_node.tag for _node in node.getchildren()])
