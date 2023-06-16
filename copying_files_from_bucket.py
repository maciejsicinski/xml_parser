from google.cloud import storage

def download_bucket_content(bucket_name, destination_folder):
    # Create a client object using your Google Cloud credentials
    client = storage.Client()

    # Get the bucket object
    bucket = client.get_bucket(bucket_name)

    # List all blobs (files) in the bucket
    blobs = bucket.list_blobs()

    # Download each blob to the destination folder
    for blob in blobs:
        # Specify the local path for each downloaded file
        destination_path = f"{destination_folder}/{blob.name}"

        # Download the file
        blob.download_to_filename(destination_path)
        print(f"Downloaded: {destination_path}")

# Specify the bucket name and destination folder
bucket_name = "infa_metadata"
destination_folder = "/Users/MaciejSicinski/xml_parsing/infa_exports/"

# Call the function to download the bucket content
download_bucket_content(bucket_name, destination_folder)
