import os
import xml.etree.ElementTree as ET
import re

def findNode(node, nodename):
    res = None
    for child in node:
        if child.tag == nodename:
            res = child
        else:
            if len(child) > 0:
                res = findNode(child, nodename)
        if res is not None:
            break
    return res

def findNodes(node, nodename, res):
    for child in node:
        if child.tag == nodename:
            res.append(child)
        else:
            if len(child) > 0:
                findNodes(child, nodename, res)

def getValue(node, names):
    res = None
    aname = node.attrib["NAME"]
    if aname in names:
        res = node.attrib["VALUE"]
    return res

def matches_pattern(string):
    pattern = r'^[A-Z][a-z]*(_[A-Z][a-z]*)*$'
    return re.match(pattern, string) is not None

def process_file(filename):
    names = [] 

    with open(filename, "r", encoding="iso-8859-15") as fh:
        doc = ET.parse(fh)
        root = doc.getroot()
        for folder in root.iter('FOLDER'):
               for target in folder.iter('SOURCE'):
                    name = target.get('NAME')
                    names.append(name)

    return names

# Folder path to scan
folder_path = "/Users/MaciejSicinski/infa_exports"

# Output file path
output_file_path = "/Users/MaciejSicinski/xml_parsing/output.txt"

# Variables to store the total counts
total_targets = 0
targets_case_issue = 0
names = []

# Open the output file in write mode
#with open(output_file_path, "w") as output_file:
    # Process each file in the folder
for filename in os.listdir(folder_path):
    if filename.endswith(".XML"):
        file_path = os.path.join(folder_path, filename)
        nm = process_file(file_path)
        names += nm
            #print(nm)
            #print(names)
unique_names = list(set(names))
#print(unique_names)
for name in unique_names:
    total_targets += 1
    if not matches_pattern(name):
        print(f"wrong casing: {name}")
        targets_case_issue += 1
    else:
        print(f"correct casing: {name}")
   # print(f"Target NAME: {name}")
print(f"Total number of targets:  {total_targets}")
print(f"Total number of targets with casing issue: {targets_case_issue}")