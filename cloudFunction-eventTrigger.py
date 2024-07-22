# Function dependencies, for example:
# package>=version


# google-cloud-dataproc==5.0.0
# google-cloud-storage==2.5.0
# google-cloud-functions



import logging
from google.cloud import dataproc_v1
from google.cloud import storage
import os

def trigger_dataproc_job(event, context):
    try:
        # Extract the file information from the event
        bucket_name = event['bucket']
        file_name = event['name']
        gcs_uri = f"gs://{bucket_name}/{file_name}"

        # Create a Dataproc client
        project_id = 'd2k-technologies-430009'
        region = 'us-central1'
        cluster_name = 'd2k-etl'
        pyspark_uri = 'gs://sparkbucket-d2k/spark.py'

        dataproc_client = dataproc_v1.JobControllerClient(client_options={"api_endpoint": f"{region}-dataproc.googleapis.com:443"})

        # Define the PySpark job configuration
        job = {
            "placement": {"cluster_name": cluster_name},
            "pyspark_job": {
                "main_python_file_uri": pyspark_uri,
                "args": [gcs_uri,file_name]  # Pass the GCS URI as an argument
            }
        }

        # Submit the job to Dataproc
        job_response = dataproc_client.submit_job(
            request={"project_id": project_id, "region": region, "job": job}
        )

        # Log the job submission details
        logging.info(f"Job submitted: {job_response.reference.job_id}")

    except Exception as e:
        logging.error(f"Error submitting Dataproc job: {e}")

# Ensure to set up logging properly
logging.basicConfig(level=logging.INFO)


