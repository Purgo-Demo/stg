-- PySpark Test Data Generation for d_product table

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DecimalType, TimestampType
from pyspark.sql.functions import lit
from pyspark.sql import functions as F

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Databricks Test Data Generation") \
    .getOrCreate()

# Define the schema for the d_product table
schema = StructType([
    StructField("prod_id", StringType(), True),
    StructField("item_nbr", StringType(), True),
    StructField("unit_cost", DoubleType(), True),
    StructField("prod_exp_dt", DecimalType(38,0), True),
    StructField("cost_per_pkg", DoubleType(), True),
    StructField("plant_add", StringType(), True),
    StructField("plant_loc_cd", StringType(), True),
    StructField("prod_line", StringType(), True),
    StructField("stock_type", StringType(), True),
    StructField("pre_prod_days", DoubleType(), True),
    StructField("sellable_qty", DoubleType(), True),
    StructField("prod_ordr_tracker_nbr", StringType(), True),
    StructField("max_order_qty", StringType(), True),
    StructField("flag_active", StringType(), True),
    StructField("crt_dt", TimestampType(), True),
    StructField("updt_dt", TimestampType(), True),
    StructField("src_sys_cd", StringType(), True),
    StructField("hfm_entity", StringType(), True)
])

# Generate test data using PySpark DataFrame operations
data = [
    # Happy path test data
    ("P001", "10001", 10.5, 20241231, 8.5, "Plant A", "LOC01", "Line1", "STCK", 2.0, 50.0, "ORD101", "500", "Y", "2024-03-21T00:00:00.000+0000", "2024-03-22T00:00:00.000+0000", "SYS01", "HFM01"),
    ("P002", "10002", 12.0, 20241230, 9.0, "Plant B", "LOC02", "Line2", "STCK", 3.0, 60.0, "ORD102", "600", "N", "2024-03-22T00:00:00.000+0000", "2024-03-23T00:00:00.000+0000", "SYS02", "HFM02"),
    # Edge cases
    ("P003", None, 0.0, 10000000, 10.0, "Plant C", "LOC03", "Line3", "STCK", 0.0, 0.0, "ORD103", "700", "Y", "2024-03-21T00:00:00.000+0000", "2024-03-23T00:00:00.000+0000", "SYS03", "HFM03"),
    ("P004", "10004", None, None, 15.0, "Plant D", "LOC04", "Line4", "STCK", 1.0, None, "ORD104", "800", "N", "2024-03-23T00:00:00.000+0000", "2024-03-24T00:00:00.000+0000", "SYS04", "HFM04"),
    # Error cases
    ("P005", "10005", 20.0, 99999999999999999999999999999999999999, 20.0, "Plant E", "LOC05", "Line5", "STCK", 5.0, 100.0, "ORD105", "-1", "Y", "2024-03-24T00:00:00.000+0000", "2024-03-25T00:00:00.000+0000", "SYS05", "HFM05"),
    # NULL handling
    ("P006", None, 25.0, 20240101, 12.0, "Plant F", "LOC06", None, "STCK", 4.0, 40.0, "ORD106", "100", None, None, None, None, None),
    # Special characters
    ("P007", "行番号", 30.0, 20241201, 11.0, "Plant G", "LOC07", "Line7", "STCK@", 5.0, 110.0, "ORD107", "300", "Y", "2024-03-19T00:00:00.000+0000", "2024-03-20T00:00:00.000+0000", "SYS07", "HFM07")
]

# Create DataFrame
test_data_df = spark.createDataFrame(data, schema)

# Display the DataFrame
test_data_df.show()

# SQL for Data Quality Check
create or replace view purgo_playground.d_product_quality_checks as
select 
  -- Count and sample records where item_nbr is NULL
  (select count(*) from d_product where item_nbr is null) as item_nbr_null_count,
  (select * from d_product where item_nbr is null limit 5) as item_nbr_null_samples,
  
  -- Count and sample records where sellable_qty is NULL
  (select count(*) from d_product where sellable_qty is null) as sellable_qty_null_count,
  (select * from d_product where sellable_qty is null limit 5) as sellable_qty_null_samples,
  
  -- Count and sample records where prod_exp_dt is not in YYYYMMDD format
  (select count(*) from d_product where length(prod_exp_dt) != 8) as prod_exp_dt_invalid_count,
  (select * from d_product where length(prod_exp_dt) != 8 limit 5) as prod_exp_dt_invalid_samples;

