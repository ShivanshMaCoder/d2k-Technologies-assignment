# NYC Taxi Trip Data Ingestion and Analysis Pipeline

## Overview
This project involves building a data pipeline to ingest NYC taxi trip data, process it using PySpark on Google Cloud Dataproc, and store the processed data in BigQuery for analytical queries. The pipeline utilizes various Google Cloud services such as Cloud Storage, Compute Engine, Cloud Functions, and BigQuery.

## Steps and Implementation

### Step 1: Web Scraping
- **Objective**: Scrape the NYC taxi trip data URLs from the [NYC Taxi & Limousine Commission Trip Record Data page](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page).
- **Approach**: Use Python to fetch the Parquet file URLs and store them in a dictionary where the key is the file name and the value is a list of URLs corresponding to the Parquet files.

### Step 2: Storing Parquet Files
- **Objective**: Store the Parquet files in a Google Cloud Storage (GCS) bucket.
- **Bucket**: `raw-first-try`
- **Structure**: The bucket contains 4 folders, each corresponding to a key from the dictionary. Each folder contains 12 Parquet files.

![Cloud Storage](https://github.com/user-attachments/assets/06893d0d-8549-44c8-9b6c-9fa4d0417efe)

![raw-first-try](https://github.com/user-attachments/assets/19726796-4820-4f64-80a0-f2a9794794b5)


### Step 3: Ingestion Code
- **Objective**: Ingest Parquet files from the `raw-first-try` bucket and store them in another GCS bucket named `d2k-raw`.
- **Compute Engine VM**: Run an ingestion script on a VM.
- **Process**: The script moves one Parquet file from each of the 4 folders in the `raw-first-try` bucket to the corresponding folders in the `d2k-raw` bucket every 10 minutes.

*d2k-raw and logs IMAGE*

### Step 4: Triggering PySpark Job
- **Objective**: Trigger a PySpark ETL job on Google Cloud Dataproc upon detecting a new object in the `d2k-raw` bucket.
- **Cloud Functions**: A Cloud Function is triggered to start the PySpark job on Dataproc when a new file is added to the `d2k-raw` bucket.

*cloud function  and cloud monitoring IMAGE*

### Step 5: ETL Script
- **Objective**: Process the ingested Parquet files using PySpark and store the processed data.
- **Dataproc**: The PySpark script is deployed on Dataproc.
- **Output**: The processed data is stored in BigQuery and also as CSV files in the GCS bucket named `d2k-processed`.

*dataproc IMAGE*

### Step 6: Storing Processed Data
- **BigQuery**: The processed data is stored in 4 separate tables within a dataset, each table corresponding to a different schema.
- **GCS**: The processed data is also stored as CSV files in the `d2k-processed` bucket.

*bigquery and d2k-processed IMAGE*

### Step 7: Analytical Queries
- **Objective**: Gain insights from the processed data stored in BigQuery.
- **Queries**:
  1. **Total Trips and Average Fare per Day**:
     ```sql
     SELECT 
       DATE(tpep_pickup_datetime) AS trip_date,
       COUNT(*) AS total_trips,
       AVG(fare_amount) AS average_fare
     FROM `d2k-technologies-430009.Trip_Data.YellowTaxi`
     WHERE EXTRACT(YEAR FROM tpep_pickup_datetime) = 2019
     GROUP BY trip_date
     ORDER BY trip_date;
     ```
  2. **Total Trips per Hour**:
     ```sql
     SELECT
       EXTRACT(HOUR FROM tpep_pickup_datetime) AS hour,
       COUNT(*) AS total_trips
     FROM `d2k-technologies-430009.Trip_Data.YellowTaxi`
     GROUP BY hour
     ORDER BY total_trips DESC;
     ```
  3. **Average Fare and Total Trips by Passenger Count**:
     ```sql
     SELECT
       passenger_count,
       AVG(fare_amount) AS average_fare,
       COUNT(*) AS total_trips
     FROM `d2k-technologies-430009.Trip_Data.YellowTaxi`
     GROUP BY passenger_count
     ORDER BY passenger_count;
     ```
  4. **Total Trips and Average Fare per Month**:
     ```sql
     SELECT
       EXTRACT(MONTH FROM tpep_pickup_datetime) AS month,
       COUNT(*) AS total_trips,
       AVG(fare_amount) AS average_fare
     FROM `d2k-technologies-430009.Trip_Data.YellowTaxi`
     WHERE EXTRACT(YEAR FROM tpep_pickup_datetime) = 2019
     GROUP BY month
     ORDER BY month;
     ```

## Screenshots
Please refer to the attached screenshots document for visual representations of the implementation, including the IAM settings, Cloud Storage structure, Cloud Functions configuration, Compute Engine setup, Dataproc jobs, Cloud Monitoring alerts, and BigQuery queries.


## Conclusion
This README provides a comprehensive overview of the data pipeline, including web scraping, data ingestion, processing, and analysis. The use of Google Cloud services ensures scalability and reliability, enabling effective processing and analysis of large datasets.
