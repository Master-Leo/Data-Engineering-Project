from google.cloud import storage
import logging
from config import project_bucket


def create_folder(bucket_name, folder_name):
    """Create a new folder (empty object) within a Google Cloud Storage bucket."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(f"{folder_name}/")
    blob.upload_from_string("")

    logging.info(f"Created folder '{folder_name}' in bucket '{bucket_name}'")

if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(level=logging.INFO)
    
    bucket_name = project_bucket
    
    # Create main folders
    create_folder(bucket_name, "data/demographic")
    create_folder(bucket_name, "data/economic")
    create_folder(bucket_name, "data/geographic")
    
    # Create subfolders within demographic
    create_folder(bucket_name, "data/demographic/zip_code")
    create_folder(bucket_name, "data/demographic/state")
    create_folder(bucket_name, "data/demographic/city")
    
    # Create subfolders within economic
    create_folder(bucket_name, "data/economic/zip_code")
    create_folder(bucket_name, "data/economic/state")
    create_folder(bucket_name, "data/economic/city")
    
    # Create subfolders within geographic
    create_folder(bucket_name, "data/geographic/zip_code")
    create_folder(bucket_name, "data/geographic/state")
    create_folder(bucket_name, "data/geographic/city")
