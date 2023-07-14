import logging
import os
import sys

# Add the directory path to the Python import path
# config_dir = '/Users/og/Desktop/Final_Project/Data-Engineering-Project/Pipeline/flows/deployment/'
# sys.path.append(config_dir)

# Now you can import the config module
from config import project_bucket

# Rest of your code
from google.cloud import storage


def create_folder(bucket_name, folder_name):
    """Create a new folder (empty object) within a Google Cloud Storage bucket."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    
    if folder_name:
        blob = bucket.blob(f"{folder_name}/")
        blob.upload_from_string("")
        logging.info(f"Created folder '{folder_name}' in bucket '{bucket_name}'")
    else:
        logging.warning("Cannot create an empty folder name.")

if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(level=logging.INFO)
    
    bucket_name = project_bucket
    
    # Create main directory
    create_folder(bucket_name, 'data')
    
    # Create main folders
    create_folder(bucket_name, 'data/demographic')
    create_folder(bucket_name, 'data/economic')
    create_folder(bucket_name, 'data/geographic')
    
    # Create subfolders within demographic
    create_folder(bucket_name, 'data/demographic/zip_code')
    create_folder(bucket_name, 'data/demographic/state')
    create_folder(bucket_name, 'data/demographic/city')
    
    # Create subfolders within economic
    create_folder(bucket_name, 'data/economic/zip_code')
    create_folder(bucket_name, 'data/economic/state')
    create_folder(bucket_name, 'data/economic/city')
    
    # Create subfolders within geographic
    create_folder(bucket_name, 'data/geographic/zip_code')
    create_folder(bucket_name, 'data/geographic/state')
    create_folder(bucket_name, 'data/geographic/city')
