from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, col
from pyspark.sql.types import StructType, StructField, StringType, LongType, DateType, DoubleType

# Create Spark session
spark = SparkSession.builder \
    .appName("DatabricksTest") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .enableHiveSupport() \
    .getOrCreate()

# Schema definition for validation
schema = StructType([
    StructField("product_id", LongType(), True),
    StructField("product_name", StringType(), True),
    StructField("product_type", StringType(), True),
    StructField("revenue", LongType(), True),
    StructField("country", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("purchased_date", DateType(), True),
    StructField("invoice_date", DateType(), True),
    StructField("invoice_number", StringType(), True),
    StructField("is_returned", LongType(), True),
    StructField("customer_satisfaction_score", LongType(), True),
    StructField("product_details", StringType(), True),
    StructField("customer_first_purchased_date", DateType(), True),
    StructField("customer_first_product", StringType(), True),
    StructField("customer_first_revenue", DoubleType(), True)
])

# Define the database
database = "purgo_playground"

# Define table names
original_table = f"{database}.d_product_revenue"
clone_table = f"{database}.d_product_revenue_clone"

# Drop the clone table if it exists
spark.sql(f"DROP TABLE IF EXISTS {clone_table}")

# Create a clone of the original table
spark.sql(f"CREATE TABLE {clone_table} AS SELECT * FROM {original_table}")

# Load the cloned table into a DataFrame for processing
df_clone = spark.table(clone_table)

# Validate schema
assert df_clone.schema == schema, "Schema of the clone table does not match expected schema"

# Mask verification function
def mask_last_four(invoice_numbers):
    for inv in invoice_numbers:
        if len(inv) < 4:
            raise ValueError(f"Invalid invoice_number: Cannot mask invoice of length less than 4")

# Extract distinct invoice numbers for validation
invoice_numbers = df_clone.select("invoice_number").distinct().rdd.flatMap(lambda x: x).collect()
mask_last_four(invoice_numbers)

# Test masking by replacing last 4 digits with '****'
df_masked = df_clone.withColumn("masked_invoice_number", expr("CONCAT(SUBSTRING(invoice_number, 1, LENGTH(invoice_number)-4), '****')"))

# Verify that masked operation complies
for row in df_masked.collect():
    assert len(row.invoice_number) >= 4, "Masked operation failed on length"
    assert row.masked_invoice_number.endswith('****'), "Masked invoice does not end with '****'"

# Overwrite the cloned table with masked data
df_masked.drop("invoice_number").withColumnRenamed("masked_invoice_number", "invoice_number").write.mode("overwrite").saveAsTable(clone_table)

# Test case: the clone should contain masked invoice numbers
df_test = spark.table(clone_table)
for row in df_test.collect():
    assert row.invoice_number.endswith('****'), "Masked invoice does not end with '****'"

# Cleanup operation by dropping the test table
spark.sql(f"DROP TABLE IF EXISTS {clone_table}")

# Stop the Spark session
spark.stop()

