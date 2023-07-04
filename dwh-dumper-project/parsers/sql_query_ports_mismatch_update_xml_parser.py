import os
import xml.etree.ElementTree as ET
import sqlglot
import sqlglot.expressions as exp
from sqlglot import parse_one, exp

# Folder path to scan
folder_path = "/Users/MaciejSicinski/xml_parsing/dwh-dumper-project/xml_metadata_test"

# Output directory path
output_dir_path = "/Users/MaciejSicinski/xml_parsing/dwh-dumper-project/sql_extract/columns_test"

# Create the output directory if it doesn't exist
os.makedirs(output_dir_path, exist_ok=True)

def getSqlQuery(transformation):
    sql_query =[]
    for ta in transformation.iter("TABLEATTRIBUTE"):
        sql_query.append(getValue(ta, ["Sql Query"]))
    return sql_query[0]

def getConnectedFrom(mapping, transformation_name):
    connected_from = []
    for connector in mapping.iter("CONNECTOR"):
        if connector.attrib["FROMINSTANCE"] == transformation_name:
            connected_from.append(connector.attrib["FROMFIELD"])
    return connected_from

def getConnectedTo(mapping, transformation_name):
    connected_from = []
    for connector in mapping.iter("CONNECTOR"):
        if connector.attrib["TOINSTANCE"] == transformation_name:
            connected_from.append(connector.attrib["TOFIELD"])
    return connected_from

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

def process_file(filename):
    with open(filename, "r", encoding="iso-8859-15") as fh:
        doc = ET.parse(fh)
        root = doc.getroot()
        for folder in root.iter("FOLDER"):
            folder_name = folder.attrib["NAME"]
            for child in folder.iter("MAPPING"):
                mapping_name = child.attrib["NAME"]
                for sq in child.iter("TRANSFORMATION"):
                    ttype = sq.attrib["TYPE"]
                    transformation_name = sq.attrib["NAME"]
                    connected_from = getConnectedFrom(child, transformation_name)
                    connected_to = getConnectedTo(child, transformation_name)
                    # For each transformation get only Sql Query attribute to parse and save columns returned by a SQL query   
                    val = getSqlQuery(sq)
                    # For each transformation in each mapping get only Source Qualifier transformation and save it's port
                    if ttype == "Source Qualifier" and val is not None and len(val) > 0:                      
                        port_names = []
                        field_nodes = list(sq.iter("TRANSFORMFIELD"))
                        print(folder_name)
                        print(mapping_name)
                        print(transformation_name)
                        for field in field_nodes:
                                print (field.attrib["NAME"])
                        #Only manipulate ports if they are connected (port is used further down the mapping)
                                if field.attrib["NAME"] in connected_from:
                                    port_names.append(field.attrib["NAME"])
                        print (sorted(port_names))
                        print (sorted(connected_from))
                        print (sorted(connected_to))
                        #port_names = [item.lower() for item in port_names]
                        sql_columns = []
                        sql_columns = find_columns(val)
                        print(sorted(sql_columns))
                            # HERE ADD EMPTY ALIASES HANDLING
                            #sql_columns= [item.lower() for item in sql_columns]
                            # Don't update if the SQL parser returned an error
                        if sql_columns == ["parse_error"]:
                            continue
                        else:
                            # Don't update if both lists contains the same values TO ADD: it only holds true if all ports are connected!!    
                            if port_names == sql_columns:
                                continue
                            # Update NAME attribute values
                            for i, field in enumerate(sq.iter("TRANSFORMFIELD")):                           
                                if i < len(sql_columns):
                                    if sql_columns[i]:
                                        field.attrib["NAME"] = sql_columns[i] 
                                # HERE ADD NAME CONFLICT RESOLUTION
                            # Update XML file with modified values

                                # HERE ADD XML FORMATTING/ENCODING/ETC to fit the source file format
                                #with open(filename, "w", encoding="iso-8859-15") as fh:
                                 #   fh.write(ET.tostring(root).decode("iso-8859-15"))
                                    
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
        process_file(file_path)

