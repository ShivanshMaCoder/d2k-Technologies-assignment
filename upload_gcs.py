import logging
from google.cloud import storage
import urllib.request
import os
import time
from io import BytesIO
from dotenv import load_dotenv 

load_dotenv()
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.environ['GOOGLE_APPLICATION_CREDENTIALS']

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
def upload_to_gcs(bucket_name, source_urls, destination_folder, max_retries=3, backoff_factor=2):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    for url in source_urls:
        try:
            file_name = url.split('/')[-1] 
            blob = bucket.blob(f"{destination_folder}/{file_name}")
                
            if blob.exists():
                logging.info(f"{file_name} already exists in {destination_folder}/{file_name}. Skipping upload.") 
                continue
             
            retries = 0
            while retries < max_retries: 
                try: 
                    with urllib.request.urlopen(url) as response:
                            file_data=response.read()
                            blob.upload_from_file(BytesIO(file_data), rewind=True, content_type='application/octet-stream')
                    logging.info(f"Uploaded {file_name} to {destination_folder}/{file_name}") 
                    break
                except Exception as e: 
                    retries += 1
                    if retries < max_retries: 
                        sleep_time = backoff_factor ** retries 
                        logging.warning(f"Retrying upload of {file_name} in {sleep_time} seconds due to error: {e}")
                        time.sleep(sleep_time) 
                    else: 
                        logging.error(f"Failed to upload {file_name} to {destination_folder}/{file_name} after {max_retries} attempts: {e}") 
        except Exception as e: 
            logging.error(f"Failed to upload {file_name} to {destination_folder}/{file_name}: {e}")
def download_and_store_in_gcs(files_dict, bucket_name): 
    logging.info(f"Starting the upload process to bucket {bucket_name}")
    for folder, urls in files_dict.items():
        logging.info(f"Processing folder {folder}")
        upload_to_gcs(bucket_name, urls, folder)
        logging.info("Upload process completed")
