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
        mcnt = 0
        sql_qry_cnt = 0
        for child in root.iter():
            if(child.tag == "MAPPING"):
                mnam = child.attrib["NAME"]
                mcnt += 1
                print(f"mapping {child.tag} {mnam}")
                if len(child) > 0:
                    tas = []
                    findNodes(child, "TABLEATTRIBUTE", tas)
                    for ta in tas:
                        val = getValue(ta,["Sql Query"])
                        if val is not None:
                            if len(val) > 0:
                                sql_qry_cnt += 1
                                print("  has Sql Query")
                                names.append(val)
                                #print(val)

    return names

# Folder path to scan
folder_path = "/Users/MaciejSicinski/xml_parsing/dwh-dumper-project/xml_metadata"

# Output file path
output_file_path = "/Users/MaciejSicinski/xml_parsing/dwh-dumper-project/sql_extract/sql_queries.txt"

# Variables to store the total counts
total_targets = 0
targets_case_issue = 0
names = []

# Process each file in the folder
for filename in os.listdir(folder_path):
    if filename.endswith(".XML"):
        file_path = os.path.join(folder_path, filename)
        nm = process_file(file_path)
        names += nm

with open(output_file_path, "a") as output_file:
    for name in names:
        output_file.write(name + "\n")