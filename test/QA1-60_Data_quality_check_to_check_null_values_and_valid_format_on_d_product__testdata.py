from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, expr
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DecimalType, TimestampType

# Initialize Spark session
spark = SparkSession.builder.appName("DatabricksTestData").getOrCreate()

# Define the schema for the d_product table
schema = StructType([
    StructField("prod_id", StringType(), True),
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

# Create a PySpark DataFrame with diverse test records
test_data = [
    # Happy Path
    ("1", "A123", 100.0, 20250301, 10.5, "123 Main St", "USA", "Line A", "Type X", 7.0, 100.0, "OT123", "50", "Y", "2024-03-21T00:00:00.000+0000", "2024-03-21T00:00:00.000+0000", "SRC1", "Entity1"),
    # Edge Cases
    ("2", "", 0.0, 20251201, 0.0, "", "", "", "", 0.0, 0.0, "", "", "", None, None, "", ""),
    ("3", "B456", 9999999.99, 20259999, 99999.99, "456 South St", "CAN", "Line B", "Type Y", 365.0, 99999999.99, "OT456", "999", "N", "2024-03-21T00:00:00.000+0000", "2023-03-21T00:00:00.000+0000", "SRC2", "Entity2"),
    # Error Cases
    ("4", None, -10.0, 0, -5.5, "789 East St", "MEX", "Line C", "Type Z", -1.0, None, "OT789", "-1", "Y", "2024-03-21T00:00:00.000+0000", "2024-03-21T00:00:00.000+0000", None, ""),
    # NULL Handling
    ("5", None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None),
    # Special Characters
    ("6", "世界", 50.5, 20241201, 20.5, "100 North St", "ESP", "Line D", "Type W", 180.0, 5000.5, "OT999", "200", "Y", "2024-03-21T00:00:00.000+0000", "2022-03-21T00:00:00.000+0000", "SRC3", "Entity3")
]

# Create DataFrame
df = spark.createDataFrame(data=test_data, schema=schema)

# Show DataFrame
df.show()

# Check the count of records where 'item_nbr' is null
item_nbr_null_count = df.filter(col("item_nbr").isNull()).count()
print(f"Count of records with 'item_nbr' as null: {item_nbr_null_count}")
df.filter(col("item_nbr").isNull()).show(5)

# Check the count of records where 'sellable_qty' is null
sellable_qty_null_count = df.filter(col("sellable_qty").isNull()).count()
print(f"Count of records with 'sellable_qty' as null: {sellable_qty_null_count}")
df.filter(col("sellable_qty").isNull()).show(5)

# Check for records where 'prod_exp_dt' is not in yyyymmdd format (Since prod_exp_dt is decimal, this is shown as error scenario conceptually)
invalid_prod_exp_dt_count = df.filter(~(col("prod_exp_dt").cast("string").rlike("^[1-9][0-9]{7}$"))).count()
print(f"Count of records with incorrect 'prod_exp_dt' format: {invalid_prod_exp_dt_count}")
df.filter(~(col("prod_exp_dt").cast("string").rlike("^[1-9][0-9]{7}$"))).show(5)

# Stop the Spark session
spark.stop()

