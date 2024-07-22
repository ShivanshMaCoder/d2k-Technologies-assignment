from pyspark.sql import SparkSession
import sys
import subprocess
import os
from pyspark.sql.functions import col, when, to_timestamp, avg, expr
from pyspark.sql.types import IntegerType

def main():

    foldername=""
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Dataproc Job") \
        .getOrCreate()

    # Retrieve arguments
    gcs_uri = sys.argv[1]
    file_name=str(sys.argv[2])


    print("*"*100)
    print(gcs_uri)
    print(file_name)
    print("*"*100)

    # Extract the base file name without extension
    file_name = os.path.basename(gcs_uri)
    base_name, _ = os.path.splitext(file_name)

    # Define the output path for the CSV file
    

    # Load Parquet data into DataFrame
    df = spark.read.parquet(gcs_uri)

    # for yellow
    if ("yellow" in file_name):
        print("yellllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllo")
        foldername="YelloTaxi"
    
        df = df.withColumn('tpep_pickup_datetime', to_timestamp(col('tpep_pickup_datetime'))) \
            .withColumn('tpep_dropoff_datetime', to_timestamp(col('tpep_dropoff_datetime')))

        df = df.fillna({'airport_fee': 0, 'congestion_surcharge': 0})

        df = df.withColumn("trip_duration", 
                        ((col("tpep_dropoff_datetime").cast("long") - col("tpep_pickup_datetime").cast("long")) / 60))

        df = df.withColumn('trip_duration', when(col('trip_duration') == 0, None).otherwise(col('trip_duration')))

        df = df.withColumn("average_speed", col("trip_distance") / col("trip_duration"))

        df = df.withColumn('average_speed', when(col('average_speed') == 0, None).otherwise(col('average_speed')))

        average_speed_mean = df.select(avg(col('average_speed'))).first()[0]
        df = df.fillna({'average_speed': average_speed_mean})

        passenger_count_mean = int(df.select(avg(col('passenger_count'))).first()[0])
        df = df.fillna({'passenger_count': passenger_count_mean})

        columns_to_cast = [ "VendorID", "passenger_count", "trip_distance", "RatecodeID", "PULocationID", "DOLocationID", "payment_type", "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount", "improvement_surcharge", "total_amount", "congestion_surcharge", "airport_fee", "trip_duration", "average_speed"]

        # Cast each column to float and overwrite the existing columns
        for column in columns_to_cast:
            df = df.withColumn(column, col(column).cast("float"))

        bigquery_table = "d2k-technologies-430009.Trip_Data.YellowTaxi"

        # Write the DataFrame to BigQuery
        df.write \
        .format("bigquery") \
        .option("table", bigquery_table) \
        .option("temporaryGcsBucket", "bigquerytempbucket-d2k/YellowTemp") \
        .option("createDisposition", "CREATE_IF_NEEDED") \
        .option("writeDisposition", "WRITE_APPEND") \
        .mode("append") \
        .save() 


    # for Green
    elif ("green" in file_name):

        foldername="GreenTaxi"

        print("greeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeen")
        df = df.fillna({'congestion_surcharge': 0, 'ehail_fee': 0})

        df = df.withColumn('lpep_pickup_datetime', to_timestamp(col('lpep_pickup_datetime'))) \
            .withColumn('lpep_dropoff_datetime', to_timestamp(col('lpep_dropoff_datetime')))

        df = df.withColumn("trip_duration", 
                        ((col("lpep_dropoff_datetime").cast("long") - col("lpep_pickup_datetime").cast("long")) / 60))

        df = df.withColumn('trip_duration', when(col('trip_duration') == 0, None).otherwise(col('trip_duration')))

        df = df.withColumn("average_speed", col("trip_distance") / col("trip_duration"))

        df = df.withColumn('average_speed', when(col('average_speed') == 0, None).otherwise(col('average_speed')))

        average_speed_mean = df.select(avg(col('average_speed'))).first()[0]
        df = df.fillna({'average_speed': average_speed_mean})

        passenger_count_mean = int(df.select(avg(col('passenger_count'))).first()[0])
        df = df.fillna({'passenger_count': passenger_count_mean})
        columns_types = {
    "VendorID": "INTEGER",
    "lpep_pickup_datetime": "TIMESTAMP",
    "lpep_dropoff_datetime": "TIMESTAMP",
    "store_and_fwd_flag": "STRING",
    "RatecodeID": "FLOAT",
    "PULocationID": "INTEGER",
    "DOLocationID": "INTEGER",
    "passenger_count": "FLOAT",
    "trip_distance": "FLOAT",
    "fare_amount": "FLOAT",
    "extra": "FLOAT",
    "mta_tax": "FLOAT",
    "tip_amount": "FLOAT",
    "tolls_amount": "FLOAT",
    "ehail_fee": "FLOAT",
    "improvement_surcharge": "FLOAT",
    "total_amount": "FLOAT",
    "payment_type": "FLOAT",
    "trip_type": "FLOAT",
    "congestion_surcharge": "FLOAT",
    "trip_duration": "FLOAT",
    "average_speed": "FLOAT"
}

        # List of columns to cast to float (INTEGER and FLOAT columns)
        columns_to_cast = [col_name for col_name, col_type in columns_types.items() if col_type in {"INTEGER", "FLOAT"}]

        # Cast each column to float and overwrite the existing columns
        for column in columns_to_cast:
            df = df.withColumn(column, col(column).cast("float"))


        bigquery_table = "d2k-technologies-430009.Trip_Data.GreenTaxi"

        # Write the DataFrame to BigQuery

        print("bigqueryyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy")
        df.write \
        .format("bigquery") \
        .option("table", bigquery_table) \
        .option("temporaryGcsBucket", "bigquerytempbucket-d2k/Greentemp") \
        .option("createDisposition", "CREATE_IF_NEEDED") \
        .option("writeDisposition", "WRITE_APPEND") \
        .mode("append") \
        .save()




    # for Hired
    elif ("fhv_trip" in file_name):
        foldername="HiredTaxi"
        print("hireeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeed")
        df = df.withColumn('pickup_datetime', to_timestamp(col('pickup_datetime'))) \
       .withColumn('dropOff_datetime', to_timestamp(col('dropOff_datetime')))

        df = df.withColumn("trip_duration", 
                        (expr("cast(dropOff_datetime as long)") - expr("cast(pickup_datetime as long)")) / 60)

        df = df.fillna({'PUlocationID': 0, 'DOlocationID': 0, 'SR_Flag': 0})

        columns_to_cast = ["PUlocationID", "DOlocationID", "SR_Flag", "trip_duration"]
        # Cast each column to float and overwrite the existing columns
        for column in columns_to_cast:
            df = df.withColumn(column, col(column).cast("float"))


        bigquery_table = "d2k-technologies-430009.Trip_Data.HiredTaxi"

        # Write the DataFrame to BigQuery
        df.write \
        .format("bigquery") \
        .option("table", bigquery_table) \
        .option("temporaryGcsBucket", "bigquerytempbucket-d2k/HiredTemp") \
        .option("createDisposition", "CREATE_IF_NEEDED") \
        .option("writeDisposition", "WRITE_APPEND") \
        .mode("append") \
        .save()


    # for High Volume Hired
    elif ("fhvhv_trip" in file_name):
        foldername="HighVolumeHiredTaxi"
        print("High volllllllllllllllllllllllllllllllllllllllllllllllllllllllllume")
        df = df.withColumn('request_datetime', to_timestamp(col('request_datetime'))) \
       .withColumn('on_scene_datetime', to_timestamp(col('on_scene_datetime'))) \
       .withColumn('pickup_datetime', to_timestamp(col('pickup_datetime'))) \
       .withColumn('dropoff_datetime', to_timestamp(col('dropoff_datetime')))
        
        df = df.withColumn("trip_duration", 
                        (expr("cast(dropoff_datetime as long)") - expr("cast(pickup_datetime as long)")) / 60)

        df = df.fillna({'PULocationID': 0, 'DOLocationID': 0, 'airport_fee': 0, 'congestion_surcharge': 0})
        
        columns_to_cast = ["PULocationID", "DOLocationID", "trip_miles", "trip_time", "base_passenger_fare", "tolls", "bcf", 
                   "sales_tax", "congestion_surcharge", "airport_fee", "tips", "driver_pay", "wav_match_flag", "trip_duration"]
        # Cast each column to float and overwrite the existing columns
        for column in columns_to_cast:
            df = df.withColumn(column, col(column).cast("float"))

        bigquery_table = "d2k-technologies-430009.Trip_Data.HighVolumeHiredTaxi"

        # Write the DataFrame to BigQuery
        df.write \
        .format("bigquery") \
        .option("table", bigquery_table) \
        .option("temporaryGcsBucket", "bigquerytempbucket-d2k/HighVolumeHiredTemp") \
        .option("createDisposition", "CREATE_IF_NEEDED") \
        .option("writeDisposition", "WRITE_APPEND") \
        .mode("append") \
        .save()



    # Write DataFrame to a single CSV file in a temporary directory
    temp_output_path = f'gs://d2k-processed/temp_csv_output_{base_name}'
    df.coalesce(1).write \
        .format('csv') \
        .option('header', 'true') \
        .mode('overwrite') \
        .save(temp_output_path)

    # Move the CSV file to the desired location
    try:
        output_path = f'gs://d2k-processed/{foldername}/{base_name}.csv'
        subprocess.run(["gsutil", "mv", f"{temp_output_path}/part-*.csv", output_path], check=True)
        print(f"CSV file saved to {output_path}")
    except subprocess.CalledProcessError as e:
        print(f"Failed to move CSV file: {e}")

    # Remove the temporary directory
    try:
        subprocess.run(["gsutil", "rm", "-r", temp_output_path], check=True)
        print("Temporary directory removed")
    except subprocess.CalledProcessError as e:
        print(f"Failed to remove temporary directory: {e}")

    # Stop the SparkSession
    spark.stop()

if __name__ == '__main__':
    main()
