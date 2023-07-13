import os
import xml.etree.ElementTree as ET

def count_transformations(input_directory):
    transformation_counts = {}

    for filename in os.listdir(input_directory):
        if filename.endswith(".XML"):
            file_path = os.path.join(input_directory, filename)
            tree = ET.parse(file_path)
            root = tree.getroot()

            for transformation in root.iter("TRANSFORMATION"):
                transformation_type = transformation.get("TYPE")
                if transformation_type not in transformation_counts:
                    transformation_counts[transformation_type] = 0
                transformation_counts[transformation_type] += 1

    return transformation_counts

# Example usage
input_directory = "/Users/MaciejSicinski/xml_parsing/dwh-dumper-project/xml_metadata"
counts = count_transformations(input_directory)

for transformation_type, count in counts.items():
    print(f"Transformation type: {transformation_type}, Count: {count}")
