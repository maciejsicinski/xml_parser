import os
import xml.etree.ElementTree as ET
import re
import sqlglot
import sqlglot.expressions as exp
from sqlglot import parse_one, exp

def find_columns(query):
    column_names = []
    for expression in sqlglot.parse_one(query).find(exp.Select).args["expressions"]:
        column_names.append(expression.alias_or_name)
    return column_names

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

def process_file(filename, output_dir):
    with open(filename, "r", encoding="iso-8859-15") as fh:
        doc = ET.parse(fh)
        root = doc.getroot()
        for folder in root.iter("FOLDER"):
            folder_name = folder.attrib["NAME"]
            for child in folder.iter("MAPPING"):
                mapping_name = child.attrib["NAME"]
                seq = 0
                for sq in child.iter("TRANSFORMATION"):
                    tname = sq.attrib["NAME"]
                    ttype = sq.attrib["TYPE"]
                    if ttype == "Source Qualifier":
                        port_names = []
                        for field in sq.iter("TRANSFORMFIELD"):
                            port_names.append(field.attrib["NAME"])
                    for ta in sq.iter("TABLEATTRIBUTE"):
                        val = getValue(ta, ["Sql Query"])
                        if val is not None and len(val) > 0:
                            output_file_name = f"{folder_name}#{mapping_name}#{tname}#{ttype}#{seq}.sql"
                            output_file_name = output_file_name.replace(" ", "")
                            output_file_path = os.path.join(output_dir, output_file_name)
                            print(output_file_path)
                            print(val)
                            sql_columns = []
                            sql_columns = find_columns(val)

                            print(f"Creating file: {output_file_path}") 
                            print(sql_columns)
                            print(port_names)
                            with open(output_file_path, "w") as output_file:
                                output_file.write("ports" + "\n")
                                output_file.write(str(port_names) + "\n")
                                output_file.write("columns" + "\n")
                                output_file.write(str(sql_columns) + "\n")


# Folder path to scan
folder_path = "/Users/MaciejSicinski/xml_parsing/dwh-dumper-project/xml_metadata"

# Output directory path
output_dir_path = "/Users/MaciejSicinski/xml_parsing/dwh-dumper-project/sql_extract/columns"

# Create the output directory if it doesn't exist
os.makedirs(output_dir_path, exist_ok=True)

# Process each file in the folder
for filename in os.listdir(folder_path):
    if filename.endswith(".XML"):
        print(filename)
        file_path = os.path.join(folder_path, filename)
        process_file(file_path, output_dir_path)
