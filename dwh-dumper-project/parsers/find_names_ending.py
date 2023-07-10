import os
import re
import xml.etree.ElementTree as ET

def check_xml_files(directory):
    columns=[]
    for filename in os.listdir(directory):
        if filename.endswith(".XML"):
            file_path = os.path.join(directory, filename)
            #print(file_path)
            try:
                tree = ET.parse(file_path)
                root = tree.getroot()
                #print(root)
                for transformfield in root.iter("TRANSFORMFIELD"):
                    name = transformfield.get("NAME")
                    if name and re.match(r"^.+__\d+$", name):
                        #print(f"Match found in file: {file_path}")
 #                       print(name)
                        columns.append(name)
            except ET.ParseError:
                print(f"Error parsing XML file: {file_path}")
    return columns

# Example usage
input_directory = "/Users/MaciejSicinski/xml_parsing/dwh-dumper-project/xml_metadata/"
columns = check_xml_files(input_directory)
a = set(columns)

for value in a:
    print(value)

