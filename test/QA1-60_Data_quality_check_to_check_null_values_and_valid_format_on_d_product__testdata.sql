-- Install necessary libraries if not available in Databricks environment.
-- Install pyarrow if necessary for data manipulation.
%pip install pyarrow

-- Generate test data using PySpark DataFrame operations

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, expr
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DecimalType, TimestampType, LongType

spark = SparkSession.builder \
    .appName("GenerateTestData") \
    .getOrCreate()

schema = StructType([
    StructField("prod_id", StringType(), False),
    StructField("item_nbr", StringType(), True),
    StructField("unit_cost", DoubleType(), True),
    StructField("prod_exp_dt", DecimalType(38, 0), True),
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

data = [
    # Happy Path Test Data (Valid Scenarios)
    ("P001", "I001", 100.50, 20240101, 50.20, "Plant A", "LOC01", "Line1", "StockX", 10.5, 500.0, "T001", "1000", "Y", current_timestamp(), current_timestamp(), "SYS1", "Entity1"),
    ("P002", "I002", 200.75, 20240201, 60.55, "Plant B", "LOC02", "Line2", "StockY", 15.5, 600.0, "T002", "2000", "Y", current_timestamp(), current_timestamp(), "SYS2", "Entity2"),
    # Edge Cases (Boundary Conditions)
    ("P003", None, 300.00, None, 70.80, "Plant C", "LOC03", "Line3", None, 20.0, None, "T003", None, "N", current_timestamp(), None, "SYS3", "Entity3"),
    ("P004", "I004", None, 20240101, None, "Plant D", None, "Line4", "StockZ", None, 800.0, None, "4000", "N", None, current_timestamp(), None, None),
    # Error Cases (Invalid Input Scenarios)
    ("P005", "I005", -50.00, 20241301, -10.00, "Plant E", "LOC05", "Line5", "StockX", -5.0, 0.0, "T005", "5000", "N", current_timestamp(), current_timestamp(), "SYS5", "Entity5"),
    # NULL Handling Scenarios
    ("P006", None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None),
    # Special Characters and Multi-byte Characters
    ("P007", "I007@#", 120.99, 20240101, 33.45, "गोल्डीकोस्ट प्लांट", "LOC07", "生产线", "Тип запаса", 9.5, 700.0, "T007", "7000", "Y", current_timestamp(), current_timestamp(), "SYS7", "Entity7"),
    ("P008", "I008é", 135.99, 20240101, 45.55, "Plant H", "LOC08", "Línea8", "Stock€", 18.5, 750.0, "T008", "8000", "Y", current_timestamp(), current_timestamp(), "SYS8", "Entity8"),

    # ... Generate additional test records as required ...
]

# Create DataFrame with the test data
df = spark.createDataFrame(data, schema)

# Display the DataFrame
df.show(truncate=False)

-- SQL logic to check data quality based on specified requirements

-- Count and sample records where item_nbr is null
SELECT COUNT(*) AS null_item_nbr_count
FROM purgo_playground.d_product
WHERE item_nbr IS NULL;

SELECT *
FROM purgo_playground.d_product
WHERE item_nbr IS NULL
LIMIT 5;

-- Count and sample records where sellable_qty is null
SELECT COUNT(*) AS null_sellable_qty_count
FROM purgo_playground.d_product
WHERE sellable_qty IS NULL;

SELECT *
FROM purgo_playground.d_product
WHERE sellable_qty IS NULL
LIMIT 5;

-- Count and sample records where prod_exp_dt is not yyyymmdd
SELECT COUNT(*) AS invalid_prod_exp_dt_count
FROM purgo_playground.d_product
WHERE NOT (prod_exp_dt BETWEEN 19000101 AND 20991231);

SELECT *
FROM purgo_playground.d_product
WHERE NOT (prod_exp_dt BETWEEN 19000101 AND 20991231)
LIMIT 5;
