from common.imports import *
from common.functions import *

# DLT One Time Batch Ingestion

# Read the raw data from the source location
citibike_raw_data = spark.conf.get("citibike_raw_data")
citibike_schema_location = spark.conf.get("citibike_schema_location")
custom_properties = {}

# Define the bronze table
@dlt.table (
    name="bronze_citi_trip_data",
    comment="Citi Bike Trip Data from 2016.",
    table_properties=get_table_properties("bronze",custom_properties)
)
def bronze_citi_tripdata():
    df = (spark.readStream.format("cloudFiles")
        .option("cloudFiles.schemaLocation", f"{citibike_schema_location}")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.maxFilesPerTrigger",1000)
        .option("cloudFiles.maxBytesPerTrigger","1g")
        .option("trigger.AvailableNow", "true")
        .load(f"{citibike_raw_data}")
        .withColumn("data_ingestion_ts", current_timestamp())
        .withColumn("file_modification_time", col("_metadata.file_modification_time"))
        .withColumn("source_file", col("_metadata.file_path").alias("source_file"))
    )
    df = (df.transform(clean_column_names))

    return df