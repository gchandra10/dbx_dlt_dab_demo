from common.imports import *
from common.functions import *

# DLT One Time Batch Ingestion

# Read the raw data from the source location
citibike_raw_data = spark.conf.get("citibike_raw_data")
citibike_schema_location = spark.conf.get("citibike_schema_location")
custom_properties = {}

log_dir = os.path.join(os.getcwd(), 'logs')
logger, file_handler = setup_logging(log_dir)
logger.info("Reading raw data from source location")

# Define the bronze table
@dlt.table (
    name="bronze_citi_trip_data",
    comment="Citi Bike Trip Data from 2016.",
    table_properties=get_table_properties("bronze",custom_properties)
)
def bronze_citi_tripdata():
    try:
        df = (spark.readStream.format("cloudFiles")
            .option("cloudFiles.schemaLocation", f"{citibike_schema_location}")
            .option("cloudFiles.format", "parquet")
            .option("cloudFiles.inferColumnTypes", "true")
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .option("cloudFiles.maxFilesPerTrigger",1000)
            .option("cloudFiles.maxBytesPerTrigger","1g")
            .option("trigger.AvailableNow", "true")
            .option("cloudFiles.includeExistingFiles", "true")
            .option("useNotifications", "true")
            .load(f"{citibike_raw_data}")
            .withColumn("data_ingestion_ts", current_timestamp())
            .withColumn("file_modification_time", col("_metadata.file_modification_time"))
            .withColumn("source_file", col("_metadata.file_path").alias("source_file"))
        )
        df = df.transform(clean_column_names)
        
        date_cols = ["starttime","stoptime","start_time","stop_time"]
        df = df.transform(lambda df: standardize_dates(df,date_cols))
        logger.info("Successfully read raw data")
        return df
    except Exception as e:
        logger.error(f"Error while reading raw data: {str(e)}")
        raise e
    


# Define the bronze table
# @dlt.table (
#     name="bronze_citi_trip_data",
#     comment="Citi Bike Trip Data from 2016.",
#     table_properties=get_table_properties("bronze",custom_properties),
#     spark_conf={"pipelines.trigger.interval": "30 seconds"}
# )
# def bronze_citi_tripdata():
#     try:
#         df = (spark.readStream.format("cloudFiles")
#             .option("cloudFiles.schemaLocation", f"{citibike_schema_location}")
#             .option("cloudFiles.format", "parquet")
#             .option("cloudFiles.inferColumnTypes", "true")
#             .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
#             .option("useNotifications", "true")
#             .load(f"{citibike_raw_data}")
#             .withColumn("data_ingestion_ts", current_timestamp())
#             .withColumn("file_modification_time", col("_metadata.file_modification_time"))
#             .withColumn("source_file", col("_metadata.file_path").alias("source_file"))
#         )
        
#         df = df.transform(clean_column_names)
#         date_cols = ["starttime","stoptime","start_time","stop_time"]
#         df = df.transform(lambda df: standardize_dates(df,date_cols))
#         logger.info("Successfully read raw data")
#         return df
#     except Exception as e:
#         logger.error(f"Error while reading raw data: {str(e)}")
#         raise e