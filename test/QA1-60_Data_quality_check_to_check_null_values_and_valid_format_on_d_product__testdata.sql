-- Install necessary libraries (if not already in place)
-- This is just a placeholder as precise libraries can't be installed directly here, assumed available

-- Test Data Generation for purgo_playground.d_product table in PySpark

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col, expr, lit

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("DatabricksTestDataGenerator") \
    .getOrCreate()

# Define the schema based on the d_product table definition
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

# Define test data (20-30 records) covering all test cases

data = [
    # Happy path data
    ("P0001", "ITM001", 12.50, 20240321, 15.00, "123 Plant St", "LOC001", "LINE1", "STOCKA", 7.0, 50.0, "ODR001", "100", "Y", '2024-03-21T00:00:00.000+0000', '2024-03-21T00:00:00.000+0000', "SYS1", "ENT1"),
    # Edge case: NULL item_nbr
    ("P0002", None, 10.00, 20240222, 14.00, "456 Plant St", "LOC002", "LINE2", "STOCKB", None, 30.0, "ODR002", "200", "N", '2024-03-22T00:00:00.000+0000', '2024-03-22T00:00:00.000+0000', "SYS2", "ENT2"),
    # Edge case: NULL sellable_qty
    ("P0003", "ITM003", 15.00, 20240320, None, "789 Plant St", "LOC003", "LINE3", "STOCKC", 10.0, None , "ODR003", "300", "Y", '2024-03-23T00:00:00.000+0000', '2024-03-23T00:00:00.000+0000', "SYS3", "ENT3"),
    # Special characters in item_nbr
    ("P0004", "ITM$%&", 20.00, 20240319, 16.00, "101 Plant St", "LOC004", "LINE4", "STOCKD", 15.0, 70.0, "ODR004", "400", "Y", '2024-03-25T00:00:00.000+0000', '2024-03-25T00:00:00.000+0000', "SYS4", "ENT4"),
    # Multi-byte characters in plant_add
    ("P0005", "ITM005", 18.00, 20240318, 14.50, "工厂街道", "LOC005", "LINE5", "STOCKE", 12.0, 65.5, "ODR005", "500", "N", '2024-03-26T00:00:00.000+0000', '2024-03-26T00:00:00.000+0000', "SYS5", "ENT5"),
    # Error case: prod_exp_dt not in yyyymmdd format
    ("P0006", "ITM006", 22.00, 20240305, 17.00, "2023 Foo St", "LOC006", "LINE6", None, 0.0, 80.0, "ODR006", "600", "Y", '2024-03-28T00:00:00.000+0000', '2024-03-28T00:00:00.000+0000', "SYS6", "ENT6"),
    # NULL scenario for multiple fields
    ("P0007", None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None),
    # Additional records covering other combinations like no stock_type, invalid and NULL crt_dt
    ("P0008", "ITM007", 22.00, 20240317, 19.00, None, None, "LINE7", "STOCKF", 20.0, 90.0, "ODR007", "700", "N", None, '2024-03-30T00:00:00.000+0000', "SYS7", "ENT7"),
    ("P0009", "ITM008", 25.00, 20240310, 20.00, "404 Plant St", "LOC008", "LINE8", "STOCKG", 8.0, 35.0, "ODR008", "800", "Y", '2024-03-10T00:00:00.000+0000', None, "SYS8", "ENT8"),
    ("P0010", "ITM#", 18.25, 20240316, 11.25, "606 Plant Rd", "LOC010", "LINE10", "STOCKI", 9.0, 44.0, "ODR010", "1000", "N", '2024-03-11T00:00:00.000+0000', '2024-03-11T00:00:00.000+0000', "SYS10", "ENT10"),
]

# Create DataFrame from test data
df = spark.createDataFrame(data, schema=schema)

# Show the DataFrame
df.show(truncate=False)

# Data Quality Checks
# Check for NULL values in 'item_nbr' and 'sellable_qty' columns
df.filter(col("item_nbr").isNull()).show(5)
df.filter(col("sellable_qty").isNull()).show(5)

# Check for invalid 'prod_exp_dt' format
df.filter(~expr("prod_exp_dt between 20240101 and 20241231")).show(5)

# End of test data generation and checks


### SQL for Data Quality Check


-- Count and sample records where 'item_nbr' is NULL
SELECT COUNT(*) as item_nbr_null_count 
FROM purgo_playground.d_product
WHERE item_nbr IS NULL;

SELECT * 
FROM purgo_playground.d_product
WHERE item_nbr IS NULL
LIMIT 5;

-- Count and sample records where 'sellable_qty' is NULL
SELECT COUNT(*) as sellable_qty_null_count 
FROM purgo_playground.d_product
WHERE sellable_qty IS NULL;

SELECT * 
FROM purgo_playground.d_product
WHERE sellable_qty IS NULL
LIMIT 5;

-- Count and sample records with incorrect 'prod_exp_dt' format
SELECT COUNT(*) as prod_exp_dt_invalid_count 
FROM purgo_playground.d_product
WHERE NOT prod_exp_dt BETWEEN 20240101 AND 20241231;

SELECT * 
FROM purgo_playground.d_product
WHERE NOT prod_exp_dt BETWEEN 20240101 AND 20241231
LIMIT 5;

