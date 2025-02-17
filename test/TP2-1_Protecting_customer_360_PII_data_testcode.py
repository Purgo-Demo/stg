# Databricks PySpark script for testing encryption in a Databricks environment

# Necessary imports and library setup
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sha2, date_format, lit, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, LongType, DateType, IntegerType
import json

# Initialize Spark session
spark = SparkSession.builder \
    .appName("PII Encryption - Test Code") \
    .getOrCreate()

# Setup and configuration comments
# --------------------------------
# Drop the old clone table if it exists
# Create a new clone table from the existing raw table

# Drop table if it exists
spark.sql("DROP TABLE IF EXISTS purgo_playground.customer_360_raw_clone")

# Clone the raw customer table to a new clone for testing
spark.sql("""
  CREATE TABLE purgo_playground.customer_360_raw_clone
  AS SELECT * FROM purgo_playground.customer_360_raw
""")

# Schema for testing and validation purposes
schema = StructType([
    StructField("id", LongType(), True),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("zip", StringType(), True),
    # Other columns omitted for brevity
])

# Test data for customer_360_raw_clone table
testData = [
    (1, "John Doe", "john.doe@example.com", "123-456-7890", "12345"),
    (2, "Alice Lee", "alice.lee@example.com", "098-765-4321", "54321"),
    (3, None, None, None, None),  # NULL handling test case
    # Other records omitted
]

# Create a DataFrame from test data
df = spark.createDataFrame(data=testData, schema=schema)

# Encrypt PII columns using SHA-256 for demonstration purposes
encrypted_df = df.withColumn("name", sha2(col("name"), 256)) \
    .withColumn("email", sha2(col("email"), 256)) \
    .withColumn("phone", sha2(col("phone"), 256)) \
    .withColumn("zip", sha2(col("zip"), 256))

# Define a static encryption key for demonstration purposes
encryption_key = {"key": "static_dummy_key"}
key_file_path = f"/Volumes/agilisium_playground/purgo_playground/de_dq/encryption_key_{date_format(current_timestamp(), 'yyyyMMdd_HHmmss')}.json"

# Save the encryption key as a JSON file
dbutils.fs.put(key_file_path, json.dumps(encryption_key))

# Write the encrypted DataFrame back to the clone table
encrypted_df.write.format("delta").mode("overwrite").saveAsTable("purgo_playground.customer_360_raw_clone")

# Show resulting DataFrame to verify encryption result
encrypted_df.show(truncate=False)

# Perform validations
# --------------------------------
# Validate that schema matches expected structure
assert encrypted_df.schema == df.schema, "Schema mismatch after encryption"

# Validate that encryption key was saved successfully
assert dbutils.fs.head(key_file_path) is not None, "Encryption key was not saved correctly"

# Validate data integrity for non-PII columns
original_df = spark.table("purgo_playground.customer_360_raw").select("id", "name", "email", "phone", "zip")
modified_df = spark.table("purgo_playground.customer_360_raw_clone").select("id")

assert original_df.count() == modified_df.count(), "Row count mismatch after encryption"

# Perform cleanup
# --------------------------------
# Clean up tables and files used in this test
spark.sql("DROP TABLE IF EXISTS purgo_playground.customer_360_raw_clone")
dbutils.fs.rm(key_file_path)

