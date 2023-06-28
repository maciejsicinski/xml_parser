import re

def extract_column_table_names(file_path):
    with open(file_path, "r") as file:
        sql_script = file.read()

        # Extract column names
        column_names = re.findall(r"\b(?:SELECT|select)\s+(.*?)\s+(?:FROM|from)\s+", sql_script, flags=re.IGNORECASE)

        # Extract table names
        table_names = re.findall(r"\b(?:FROM|from)\s+(.*?)\s+(?:WHERE|where|\bJOIN|join|\bINTO|into|\bUPDATE|update|\bDELETE|delete|\bTABLE|table)\b", sql_script, flags=re.IGNORECASE)

        return column_names, table_names

# Specify the file path
file_path = "/Users/MaciejSicinski/xml_parsing/sql_queries.txt"

# Output file path
output_file_path_tables = "/Users/MaciejSicinski/xml_parsing/sql_queries_tables.txt"

# Output file path
output_file_path_columns = "/Users/MaciejSicinski/xml_parsing/sql_queries_columns.txt"


# Extract column and table names
column_names, table_names = extract_column_table_names(file_path)

# Print the extracted names
#print("Column Names:")
unique_column_names = list(set(column_names))
#for name in unique_column_names:
#    print(f"Column name: {name}")
#print("\nTable Names:")

unique_table_names = list(set(table_names))
#for name in unique_table_names:
#    print(f"Table name: {name}")

with open(output_file_path_tables, "a") as output_file:
    for name in unique_table_names:
        output_file.write(name + "\n")

with open(output_file_path_columns, "a") as output_file:
    for name in unique_column_names:
        output_file.write(name + "\n")