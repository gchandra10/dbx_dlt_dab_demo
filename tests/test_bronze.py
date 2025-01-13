import pytest
from databricks.connect import DatabricksSession
from pyspark.testing.utils import assertDataFrameEqual, assertSchemaEqual 
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import sys
import os

databricks_env_path = os.path.join(os.getcwd(), '.databricks', '.databricks.env')

from dotenv import load_dotenv
load_dotenv(databricks_env_path)

sys.path.append('./src')

@pytest.fixture(scope="module")
def spark_session():
    try:
        cluster_id = os.environ.get("DATABRICKS_CLUSTER_ID")
        return DatabricksSession.builder.clusterId(cluster_id).getOrCreate()
        #return DatabricksSession.builder.serverless(True).getOrCreate()
    except ImportError:
        print("No Databricks Connect, build and return local SparkSession")
        exit(1)
    
def test_row_count(spark_session):
    df = spark_session.read.table("gannychan.dlt_demo.bronze_citi_trip_data")
    assert df.count() >= 50

