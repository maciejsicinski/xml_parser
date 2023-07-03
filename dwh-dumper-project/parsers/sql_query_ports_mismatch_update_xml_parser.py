import os
import xml.etree.ElementTree as ET
import re
import sqlglot
import sqlglot.expressions as exp
from sqlglot import parse_one, exp

# Folder path to scan
folder_path = "/Users/MaciejSicinski/xml_parsing/dwh-dumper-project/xml_metadata_test"

# Output directory path
output_dir_path = "/Users/MaciejSicinski/xml_parsing/dwh-dumper-project/sql_extract/columns_test"

# Create the output directory if it doesn't exist
os.makedirs(output_dir_path, exist_ok=True)

def find_columns(query):
    column_names = []
    try:
        for expression in sqlglot.parse_one(query).find(exp.Select).args["expressions"]:
            column_names.append(expression.alias_or_name)
    except Exception as e:
        return ["parse_error"]
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

def process_file(filename, output_dir):
    with open(filename, "r", encoding="iso-8859-15") as fh:
        doc = ET.parse(fh)
        root = doc.getroot()
        for folder in root.iter("FOLDER"):
            folder_name = folder.attrib["NAME"]
            for child in folder.iter("MAPPING"):
                ef = 0
                mapping_name = child.attrib["NAME"]
                seq = 0
                for sq in child.iter("TRANSFORMATION"):
                    tname = sq.attrib["NAME"]
                    ttype = sq.attrib["TYPE"]
                    # For each transformation in each mapping get only Source Qualifier transformation and save it's port
                    if ttype == "Source Qualifier":                      
                        port_names = []
                        field_nodes = list(sq.iter("TRANSFORMFIELD"))
                        for field in field_nodes:
                            # HERE ADD TO EXTRACT ONLY CONNECTED PORTS
                            port_names.append(field.attrib["NAME"])
                        port_names = [item.lower() for item in port_names]
                    # For each transformation get only Sql Query attribute to parse and save columns returned by a SQL query    
                    for ta in sq.iter("TABLEATTRIBUTE"):
                        val = getValue(ta, ["Sql Query"])
                        if val is not None and len(val) > 0:
                            sql_columns = []
                            sql_columns = find_columns(val)
                            # HERE ADD EMPTY ALIASES HANDLING
                            sql_columns= [item.lower() for item in sql_columns]
                            # Don't update if the SQL parser returned an error
                            if sql_columns == ["parse_error"]:
                                continue
                            else:
                            # Don't update if both lists contains the same values TO ADD: it only holds true if all ports are connected!!    
                                if port_names == sql_columns:
                                    continue
                            # Update NAME attribute values
                                for i, field in field_nodes:   
                                    field = field_nodes[i]                             
                                    if i < len(sql_columns):
                                        field.attrib["NAME"] = sql_columns[i] 
                                # HERE ADD NAME CONFLICT RESOLUTION
                            # Update XML file with modified values

                                # HERE ADD XML FORMATTING/ENCODING/ETC to fit the source file format
                                with open(filename, "w", encoding="iso-8859-15") as fh:
                                    fh.write(ET.tostring(root).decode("iso-8859-15"))
                                    
                                    #tree = ET.ElementTree(root)
                                    #tree.write(filename, encoding="iso-8859-15", xml_declaration=True)

                                    # Update XML file with modified values
                                    #with open(filename, "r", encoding="iso-8859-15") as file:
                                    #    xml_content = file.read()

                                   # xml_content = re.sub(r'<TRANSFORMATION .*?</TRANSFORMATION>', ET.tostring(sq).decode("iso-8859-15"), xml_content, flags=re.DOTALL)

                                    #with open(filename, "w", encoding="iso-8859-15") as file:
                                     #   file.write(xml_content)

# Process each file in the folder
for filename in os.listdir(folder_path):
    if filename.endswith(".XML"):
        #print(filename)
        file_path = os.path.join(folder_path, filename)
        process_file(file_path, output_dir_path)

