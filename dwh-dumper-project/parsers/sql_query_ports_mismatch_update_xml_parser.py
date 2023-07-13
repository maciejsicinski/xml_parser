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

def replaceLoadIdExp(transformation):
    if transformation.attrib["TYPE"] == "Expression":
        for field in transformation.iter("TRANSFORMFIELD"):
            if field.attrib["EXPRESSION"] == "$$LOAD_ID":
                field.attrib["EXPRESSION"] = "TO_DECIMAL($$LOAD_ID,5)"

def updateConnectors(mapping, transformation_name, map):
    for connector in mapping.iter("CONNECTOR"):
        if connector.attrib["FROMINSTANCE"] == transformation_name and connector.attrib["FROMFIELD"] in map.keys():
            old = connector.attrib["FROMFIELD"] #functionalize
            new = map[connector.attrib["FROMFIELD"]]['to']
            if old != new:
                connector.attrib["FROMFIELD"] = map[connector.attrib["FROMFIELD"]]['to']
        elif connector.attrib["TOINSTANCE"] == transformation_name and connector.attrib["TOFIELD"] in map.keys():
            old = connector.attrib["TOFIELD"] #reuse function
            new = map[connector.attrib["TOFIELD"]]['to']
            if old != new:
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

def getSqlQuery(transformation):
    for ta in transformation.iter("TABLEATTRIBUTE"):
        sql_query= ta.attrib["Sql Query"]
    return sql_query

def getConnectedFrom(mapping, transformation_name):
    connected_from = []
    for connector in mapping.iter("CONNECTOR"):
        if connector.attrib["FROMINSTANCE"] == transformation_name:
            connected_from.append(connector.attrib["FROMFIELD"])
    return connected_from

def findColumns(query):
    column_names = []
    try:
        for expression in sqlglot.parse_one(query).find(exp.Select).args["expressions"]:
            column_names.append(expression.alias_or_name)
    except Exception as e:
        return ["parse_error"] #try to print the error
    return column_names

def processFile(filename):
    with open(filename, "r", encoding="iso-8859-1") as fh:
        doc = ET.parse(fh)
        root = doc.getroot()
        for folder in root.iter("FOLDER"):
            folder_name = folder.attrib["NAME"]
            for child in folder.iter("MAPPING"):
                mapping_name = child.attrib["NAME"]
                for sq in child.iter("TRANSFORMATION"):
                    #replace $$LOAD_ID with the converted version TO_DECIMAL($$LOAD_ID,5)               
                    replaceLoadIdExp(sq)
                    field_dict = {} 
                    conflict_update = {}
                    ttype = sq.attrib["TYPE"]
                    transformation_name = sq.attrib["NAME"]
                    connected_from = getConnectedFrom(child, transformation_name)
                    # For each transformation get only Sql Query attribute to parse and save columns returned by a SQL query   
                    query = getSqlQuery(sq)
                    # For each transformation in each mapping get only Source Qualifier transformation and save it's port
                    if ttype == "Source Qualifier" and query is not None and len(query) > 0:                      
                        port_names = []
                        field_nodes = list(sq.iter("TRANSFORMFIELD"))
                        for field in field_nodes:
                        #Only manipulate ports if they are connected (port is used further down the mapping)
                                if field.attrib["NAME"] in connected_from:
                                    port_names.append(field.attrib["NAME"])
                        #port_names = [item.lower() for item in port_names]
                        sql_columns = findColumns(query)
                            # HERE ADD EMPTY ALIASES HANDLING
                            #sql_columns= [item.lower() for item in sql_columns]
                            # Don't update if the SQL parser returned an error
                        if sql_columns == ["parse_error"]:
                            #create file with the mapping.sq so we know where the error occured
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
                                    if sql_columns[i].lower() != field.attrib["NAME"].lower():    
                                        field_dict[field.attrib["NAME"]]= {  
                                        "from": field.attrib["NAME"],
                                         "to" : sql_columns[i]
                                        }
                                        next = sql_columns[i]
                                        updated_ports, conflict_update_tmp = updateConflictingPortNames(
                                            mapping=child,
                                            transformation_name=transformation_name,
                                            old_port=field.attrib["NAME"],
                                            new_port=next,
                                            port_names = port_names
                                        )
                                        if conflict_update_tmp:
                                            conflict_update.update(conflict_update_tmp)
                                        for item in updated_ports:
                                            port_names.append(item)
                                        field.attrib["NAME"] = sql_columns[i] 
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
                                        field_dict_2 [conflict_update[key]['original']]= {  
                                        "from": conflict_update[key]['original'],
                                         "to" : key
                                        }
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
        processFile(file_path)

