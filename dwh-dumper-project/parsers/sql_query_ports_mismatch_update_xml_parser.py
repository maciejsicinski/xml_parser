import os
import xml.etree.ElementTree as ET
import sqlglot
from sqlglot import exp

# Folder path to scan
folder_path = "/Users/MaciejSicinski/xml_parsing/dwh-dumper-project/xml_metadata_test"

# Output directory path
output_dir_path = "/Users/MaciejSicinski/xml_parsing/dwh-dumper-project/sql_extract/columns_test"

# Create the output directory if it doesn't exist
os.makedirs(output_dir_path, exist_ok=True)

def replace_load_id_exp(transformation):
    if transformation.attrib["TYPE"] == "Expression":
        for field in transformation.iter("TRANSFORMFIELD"):
            print(field)
            if field.attrib["EXPRESSION"] == "$$LOAD_ID":
                print(field.attrib["EXPRESSION"])
                field.attrib["EXPRESSION"] = "TO_DECIMAL($$LOAD_ID,5)"
                print(field.attrib["EXPRESSION"])

def updateConnectors(mapping, transformation_name, map):
    for connector in mapping.iter("CONNECTOR"):
        if connector.attrib["FROMINSTANCE"] == transformation_name and connector.attrib["FROMFIELD"] in map.keys():
            old = connector.attrib["FROMFIELD"] 
            new = map[connector.attrib["FROMFIELD"]]['to']
            if old != new:
                print("changing connector from")
                print(f"old name: {old}, new name: {new}")
                connector.attrib["FROMFIELD"] = map[connector.attrib["FROMFIELD"]]['to']
        elif connector.attrib["TOINSTANCE"] == transformation_name and connector.attrib["TOFIELD"] in map.keys():
            old = connector.attrib["TOFIELD"] 
            new = map[connector.attrib["TOFIELD"]]['to']
            if old != new:
                print("changing connector to")
                print(f"old name: {old}, new name: {new}")
                connector.attrib["TOFIELD"] = map[connector.attrib["TOFIELD"]]['to']

def updateConflictingPortNames(mapping, transformation_name, old_port, new_port, port_names):
    updated_ports = []
    conflict_update = {}
    for transformation in mapping.iter("TRANSFORMATION"):
        if transformation.attrib["NAME"] == transformation_name:
            for index, port in enumerate(transformation.iter("TRANSFORMFIELD")):
                if new_port == port.attrib["NAME"]:
                    updated_name = port.attrib["NAME"] + "__" + str(index)
                    if port.attrib["NAME"] in port_names:
                        updated_ports.append(updated_name)
                    conflict_update [updated_name] = {
                        "original" : new_port
                    }
                    port.attrib["NAME"] = updated_name
                    print("new field dict inside")
                    print(conflict_update)
                    #updateConnectorFrom(mapping, transformation_name, new_port, updated_name) 
                    #updateConnectorTo(mapping, transformation_name, new_port, updated_name) 
    return updated_ports, conflict_update

def updateConnectorFrom(mapping, transformation_name, name_from, name_to):
    for connector in mapping.iter("CONNECTOR"):
        if connector.attrib["FROMINSTANCE"] == transformation_name and connector.attrib["FROMFIELD"] == name_from:
            connector.attrib["FROMFIELD"] = name_to
            #For the testing purposes print the changed port name
            print(ET.tostring(connector))

def updateConnectorTo(mapping, transformation_name, name_from, name_to):
    for connector in mapping.iter("CONNECTOR"):
        if connector.attrib["TOINSTANCE"] == transformation_name and connector.attrib["TOFIELD"] == name_from:
            connector.attrib["TOFIELD"] = name_to
            print(ET.tostring(connector))

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
    with open(filename, "r", encoding="iso-8859-1") as fh:
        doc = ET.parse(fh)
        root = doc.getroot()
        for folder in root.iter("FOLDER"):
            folder_name = folder.attrib["NAME"]
            for child in folder.iter("MAPPING"):
                mapping_name = child.attrib["NAME"]
                for sq in child.iter("TRANSFORMATION"):
                    #replace $$LOAD_ID with the converted version TO_DECIMAL($$LOAD_ID,5)               
                    replace_load_id_exp(sq)
                    field_dict = {} 
                    conflict_update = {}
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
                        for field in field_nodes:
                        #Only manipulate ports if they are connected (port is used further down the mapping)
                                if field.attrib["NAME"] in connected_from:
                                    port_names.append(field.attrib["NAME"])
                        #port_names = [item.lower() for item in port_names]
                        sql_columns = []
                        sql_columns = find_columns(val)
                            # HERE ADD EMPTY ALIASES HANDLING
                            #sql_columns= [item.lower() for item in sql_columns]
                            # Don't update if the SQL parser returned an error
                        if sql_columns == ["parse_error"]:
                            continue
                        else:
                            # Don't update if both lists contains the same values (lowercased) 
                            #if [item.lower() for item in port_names] == [item.lower() for item in sql_columns]:
                            #    continue
                            # Update NAME attribute values
                            i = 0
                            for field in sq.iter("TRANSFORMFIELD"):
                                if i < len(sql_columns) and field.attrib["NAME"] in port_names:
                                    print(field.attrib["NAME"])
                                    if sql_columns[i] != field.attrib["NAME"]:
                                        field_dict [field.attrib["NAME"]]= {  
                                        "from": field.attrib["NAME"],
                                         "to" : sql_columns[i]
                                        }
                                        current = field.attrib["NAME"]
                                        next = sql_columns[i]
                                        updated_ports, conflict_update_tmp = updateConflictingPortNames(child, transformation_name, field.attrib["NAME"], next, port_names)
                                        if (conflict_update_tmp):
                                            conflict_update.update(conflict_update_tmp)
                                        for item in updated_ports:
                                            port_names.append(item)
                                        print(conflict_update)                           
                                        #updateConnectorFrom(child, transformation_name, current, next) 
                                        #updateConnectorTo(child, transformation_name, current, next) 
                                        field.attrib["NAME"] = sql_columns[i] 
                                        #updateConflictingPortNames(child, transformation_name, next)
                                    i+=1
                            field_dict_2 = {}
                            if(conflict_update):
                                for key, value in field_dict.items():
                                    if key in conflict_update:
                                        original_key = conflict_update[key]['original']
                                        field_dict_2[original_key] = value
                                        field_dict_2[original_key]['from'] = original_key
                                    else:
                                        field_dict_2[key] = value
                                for key, value in conflict_update.items():
                                    if key not in field_dict.keys() :
                                        print(f"missoing:  {key}, {value}")
                                        field_dict_2 [conflict_update[key]['original']]= {  
                                        "from": conflict_update[key]['original'],
                                         "to" : key
                                        }
                            print(f"original dict: {field_dict}")    
                            print(f"changes due to conflicts: {conflict_update}")
                            print(f"adjusted dict: {field_dict_2}")    
                            updateConnectors(child, transformation_name, field_dict_2)
 
        # Update XML file with modified values
        filename_out = filename + "_OUT.XML"
        with open(filename_out, "w", encoding="iso-8859-1") as fh:
            #Write informatica header into a file
            fh.write("<?xml version=\"1.0\" encoding=\"ISO-8859-1\"?>\n")
            fh.write("<!-- Informatica proprietary -->\n")
            fh.write("<!DOCTYPE POWERMART SYSTEM \"powrmart.dtd\">\n")
            fh.write(ET.tostring(root).decode("iso-8859-1"))
                                    
# Process each file in the folder
for filename in os.listdir(folder_path):
    if filename.endswith(".XML"):
        file_path = os.path.join(folder_path, filename)
        process_file(file_path)

