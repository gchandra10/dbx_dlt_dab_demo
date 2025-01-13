from common.imports import *
from common.functions import *
from config import SCHEMA_PATH

catalog_database = spark.conf.get("catalog_database")

custom_properties = {}

log_dir = os.path.join(os.getcwd(), 'logs')
logger, file_handler = setup_logging(log_dir)
logger.info("Starting Silver to Gold")

# Gold table 1: Usage Analytics by Station
@dlt.table(
    name="gold_station_analytics",
    comment="Aggregated station-level metrics for bike usage patterns",
    cluster_by=["start_station_name"],
    table_properties=get_table_properties("gold",custom_properties)
)
def create_station_analytics():
    try:
        """
        Creates a gold MV with station-level analytics including:
        - Total trips starting from each station
        - Total trips ending at each station
        - Average trip duration from/to each station
        - Peak usage hours
        """
        return (
            spark.read.table(f"{catalog_database}.silver_citi_trip_data")
            .groupBy("start_station_id", "start_station_name", "start_station_latitude", "start_station_longitude","start_time")
            .agg(
                count("*").alias("total_trips_started"),
                avg("trip_duration").alias("avg_trip_duration_seconds"),
                hour(col("start_time")).alias("peak_start_hour"),
                countDistinct("bike_id").alias("unique_bikes_used")
            )
        )
    except Exception as e:
        logger.error(f"Error while creating gold_station_analytics: {str(e)}")
        raise e
    
# Gold table 2: Popular Routes Analysis
@dlt.table(
    name="gold_popular_routes",
    comment="Materialized view for route analysis with Liquid Clustering",
    cluster_by=["start_station_id","end_station_id"],
    table_properties=get_table_properties("gold",custom_properties)
)
@dlt.expect_or_fail("valid_stations", "start_station_id IS NOT NULL AND end_station_id IS NOT NULL")
@dlt.expect_or_fail("different_stations", "start_station_id != end_station_id")
def create_popular_routes():
    try:
        """
        Creates a gold materialized view analyzing popular routes using Liquid Clustering
        """
        return (
            spark.read.table(f"{catalog_database}.silver_citi_trip_data")
            .groupBy(
                "start_station_id", "start_station_name",
                "end_station_id", "end_station_name"
            )
            .agg(
                count("*").alias("total_trips"),
                avg("trip_duration").alias("avg_route_duration"),
                countDistinct("user_type").alias("unique_user_types"),
                collect_set("user_type").alias("user_types_list")
            )
            .where("start_station_id != end_station_id")
            .orderBy(desc("total_trips"))
        )
    except Exception as e:
        logger.error(f"Error while creating gold_popular_routes: {str(e)}")
        raise e
