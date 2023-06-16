import os
import xml.etree.ElementTree as ET

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

def process_file(filename, output_file):
    with open(filename, "r", encoding="iso-8859-15") as fh:
        doc = ET.parse(fh)
        root = doc.getroot()
        mcnt = 0
        sql_qry_cnt = 0
        for child in root.iter():
            if(child.tag == "MAPPING"):
                mnam = child.attrib["NAME"]
                mcnt += 1
                sql_qry_fnd = False
                if len(child) > 0:
                    tas = []
                    findNodes(child, "TABLEATTRIBUTE", tas)
                    for ta in tas:
                        val = getValue(ta, ["Sql Query"])
                        if sql_qry_fnd == False:
                            if val is not None and len(val) > 0:
                                sql_qry_cnt += 1
                                sql_qry_fnd = True
                if sql_qry_fnd:
                    output_file.write(f"Mapping {mnam} has Sql Query\n")

        output_file.write(f"File: {filename}\n")
        output_file.write(f"Total Mappings: {mcnt}\n")
        output_file.write(f"Mappings with Sql Query: {sql_qry_cnt}\n")
        output_file.write("\n")

        return mcnt, sql_qry_cnt

# Folder path to scan
folder_path = "/Users/MaciejSicinski/xml_parsing/infa_exports"

# Output file path
output_file_path = "/Users/MaciejSicinski/xml_parsing/output.txt"

# Variables to store the total counts
total_mcnt = 0
total_sql_qry_cnt = 0

# Open the output file in write mode
with open(output_file_path, "w") as output_file:
    # Process each file in the folder
    for filename in os.listdir(folder_path):
        if filename.endswith(".XML"):
            file_path = os.path.join(folder_path, filename)
            mcnt, sql_qry_cnt = process_file(file_path, output_file)
            total_mcnt += mcnt
            total_sql_qry_cnt += sql_qry_cnt

# Write the total counts to the output file
with open(output_file_path, "a") as output_file:
    output_file.write("Total Counts:\n")
    output_file.write(f"Total Mappings: {total_mcnt}\n")
    output_file.write(f"Total Mappings with Sql Query: {total_sql_qry_cnt}\n")

print("Program executed successfully. Output saved to the file:", output_file_path)
