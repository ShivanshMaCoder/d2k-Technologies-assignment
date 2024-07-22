import time
import logging
from concurrent.futures import ThreadPoolExecutor
from google.cloud import storage
import os


logging.basicConfig(filename='transfer_log.txt', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

def list_blobs(bucket_name, prefix):
    
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix)
    return blobs

def rewrite_blob(source_bucket_name, destination_bucket_name, blob_name, destination_blob_name):
    
    try:
        storage_client = storage.Client()

        source_bucket = storage_client.bucket(source_bucket_name)
        source_blob = source_bucket.blob(blob_name)
        destination_bucket = storage_client.bucket(destination_bucket_name)
        destination_blob = destination_bucket.blob(destination_blob_name)

        
        token = None
        while True:
            token, _, _ = destination_blob.rewrite(source_blob, token=token)
            if token is None:
                break

        logging.info(f"Rewritten {blob_name} to {destination_blob_name}")

    except Exception as e:
        logging.error(f"Failed to rewrite {blob_name}: {e}")

def transfer_files_parallel(source_bucket, destination_bucket, folders, month):
    def transfer_file(folder):
        blob_name = f"{folder}/{folder}_2019-{month:02d}.parquet"
        if folder == "fhvhv_tripdata" and month == 1:
            logging.info(f"Skipping {blob_name} as it is missing")
            return  

        blobs = list_blobs(source_bucket, blob_name)
        for blob in blobs:
            destination_blob_name = f"{folder}/{blob.name.split('/')[-1]}"
            rewrite_blob(source_bucket, destination_bucket, blob.name, destination_blob_name)

    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = [executor.submit(transfer_file, folder) for folder in folders]
        for future in futures:
            try:
                future.result()  
            except Exception as e:
                logging.error(f"Error in transfer_files_parallel: {e}")

def upload_logs_to_bucket(bucket_name, log_file_path):
    
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(f'Logs/{log_file_path}')
        blob.upload_from_filename(log_file_path)
        logging.info(f"Uploaded log file {log_file_path} to {bucket_name}/Logs/")
    except Exception as e:
        logging.error(f"Failed to upload log file: {e}")

def main():
    source_bucket = 'raw-first-try'
    destination_bucket = 'd2k-raw'
    folders = ['yellow_tripdata', 'green_tripdata', 'fhv_tripdata', 'fhvhv_tripdata']
    months = range(1, 13)

    interval_minutes = 10 

    for month in months:
        transfer_files_parallel(source_bucket, destination_bucket, folders, month)
        time.sleep(interval_minutes * 60) 

   
    upload_logs_to_bucket('trip_ingestion_logs', 'transfer_log.txt')

if __name__ == "__main__":
    os.environ['GOOGLE_APPLICATION_CREDENTIALS']='d2k-technologies-430009-c958f5120b72.json'
    main()
