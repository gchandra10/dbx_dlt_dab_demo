from common.imports import *
from common.functions import *

citibike_raw_data = spark.conf.get("citibike_raw_data")
citibike_schema_location = spark.conf.get("citibike_schema_location")

custom_properties = {}

## Liquid Clustering

@dlt.table(
    name="silver_citi_trip_data",
    comment="The cleaned citibike",
    cluster_by=["start_station_id","user_type"],
    table_properties=get_table_properties("silver",custom_properties)
)

def silver_citi_tripdata():
    try:
        df = spark.readStream.table("gannychan.dlt_demo.bronze_citi_trip_data")

        df = (df.select(
                coalesce(
                    col("tripduration"),
                    col("trip_duration")
                ).alias("trip_duration"),
            
                coalesce(
                    col("starttime"),
                    col("start_time")
                ).alias("start_time"),

                coalesce(
                    col("stoptime"),
                    col("stop_time")
                ).alias("stop_time"),

                col("start_station_id"),
                col("start_station_name"),
                col("start_station_latitude"),
                col("start_station_longitude"),
                col("end_station_id"),
                col("end_station_name"),
                col("end_station_latitude"),
                col("end_station_longitude"),
                
                coalesce(
                    col("bikeid"),
                    col("bike_id")
                ).alias("bike_id"),

                coalesce(
                    col("usertype"),
                    col("user_type")
                ).alias("user_type"),

                col("birth_year")
            )
        )

        df = df.withColumn("start_time", to_timestamp(df.start_time,'yyyy-M-d HH:mm:ss'))
        df = df.withColumn("stop_time", to_timestamp(df.stop_time,'yyyy-M-d HH:mm:ss'))
        
        df.transform(standardize_dataframe)
        df.transform(convert_data_to_title_case, ["user_type"])

        return df
    except:
        print(f"Error in silver_citybike: {str(e)}")
        raise
