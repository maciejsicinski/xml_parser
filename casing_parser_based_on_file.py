import os
import xml.etree.ElementTree as ET
import re


def matches_pattern(string):
    pattern = r'^[A-Z][a-z]*(_[A-Z][a-z]*)*$'
    return re.match(pattern, string) is not None

def process_file(file_path):
    with open(file_path, "r") as file:
        total_cnt = 0
        good_count = 0
        names = []
        for line in file:
            total_cnt += 1
            table_names = matches_pattern(line)
            print(f"table name: {line}")
            print(table_names)
            if (table_names):
                good_count += 1    
            name_v = line + ": " +str(table_names)
            names.append(name_v)
    return names, total_cnt, good_count
# File path to scan
file_path = "/Users/MaciejSicinski/xml_parsing/sql_queries_tables.txt"

# Output file path
output_file_path = "/Users/MaciejSicinski/xml_parsing/casing_isues_sql_override_tables.txt"

# Variables to store the total counts
names = []
total = 0
good = 0
# Process each file in the folder

names, total, good = process_file(file_path)

with open(output_file_path, "a") as output_file:
    for name in names:
        output_file.write(name + "\n")

print(total)
print(good)