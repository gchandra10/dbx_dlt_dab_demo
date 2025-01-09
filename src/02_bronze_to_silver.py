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

        df = (df.transform(clean_column_names)
            .select(
                coalesce(
                    col("tripduration"),
                    col("trip_duration"),
                ).alias("trip_duration"),
            
                coalesce(
                    col("starttime"),
                ).alias("start_time"),

                coalesce(
                    col("stoptime"),
                ).alias("stop_time"),

                coalesce(
                    col("start_station_id")
                ).alias("start_station_id"),

                coalesce(
                    col("start_station_name")
                ).alias("start_station_name"),

                coalesce(
                    col("start_station_latitude")
                ).alias("start_station_latitude"),

                coalesce(
                    col("start_station_longitude")
                ).alias("start_station_longitude"),

                coalesce(
                    col("end_station_id")
                ).alias("end_station_id"),

                coalesce(
                    col("end_station_name")
                ).alias("end_station_name"),

                coalesce(
                    col("end_station_latitude")
                ).alias("end_station_latitude"),

                coalesce(
                    col("end_station_longitude")
                ).alias("end_station_longitude"),
                
                coalesce(
                    col("bikeid"),
                    col("bike_id")
                ).alias("bike_id"),

                coalesce(
                    col("usertype"),
                    col("user_type")
                ).alias("user_type"),

                coalesce(
                    col("birth_year")
                ).alias("birth_year")
            )
        )

        df = df.withColumn("start_time", to_timestamp(df.start_time,'M/d/yyyy HH:mm:ss'))
        df = df.withColumn("stop_time", to_timestamp(df.stop_time,'M/d/yyyy HH:mm:ss'))
        
        df.transform(standardize_dataframe)
        df.transform(convert_data_to_title_case, ["user_type"])

        return df
    except:
        print(f"Error in silver_citybike: {str(e)}")
        raise
