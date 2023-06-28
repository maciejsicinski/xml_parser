import os

def extract_view_ddl(line):
    # Extract the view DDL from the line
    if line.startswith('CREATE VIEW') or line.startswith('REPLACE VIEW'):
        ddl = line.strip()  # Assuming the entire DDL is on a single line
        return ddl
    else:
        return None

def create_view_files(input_file):
    # Create a directory to store the SQL files
    output_dir = "/Users/MaciejSicinski/xml_parsing/view_ddl_files/"
    os.makedirs(output_dir, exist_ok=True)

    # Encodings to try
    encodings = ['utf-8', 'latin-1', 'ISO-8859-1']

    # Read the input file line by line using different encodings
    for encoding in encodings:
        try:
            with open(input_file, 'r', encoding=encoding) as file:
                lines = file.readlines()
            break  # Break out of the loop if the file is successfully read
        except UnicodeDecodeError:
            continue  # Try the next encoding if decoding fails

    # Process each line and create separate SQL files for views
    for line in lines:
        ddl = extract_view_ddl(line)
        if ddl:
            # Extract the view name from the DDL
            view_name = ddl.split()[2]

            # Create a SQL file for the view
            output_file = os.path.join(output_dir, f'{view_name}.sql')
            with open(output_file, 'w', encoding='utf-8') as file:
                file.write(ddl)

            print(f"Created SQL file for view '{view_name}': {output_file}")

    print("View DDL files created successfully!")

# Usage example
input_file = "/Users/MaciejSicinski/xml_parsing/dev_stg_views.txt"  # Replace with your input file path
create_view_files(input_file)
