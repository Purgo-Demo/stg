from pyspark.sql import SparkSession
from pyspark.sql.functions import col, create_map, lit, struct
from pyspark.sql.types import StringType, StructType, StructField, ArrayType, MapType
from cryptography.fernet import Fernet
import json
from datetime import datetime

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Encrypt PII Data") \
    .enableHiveSupport() \
    .getOrCreate()

# Generate encryption key and initialize encryption object
encryption_key = Fernet.generate_key()
fernet = Fernet(encryption_key)

# Function to encrypt a column
def encrypt_column(column_value):
    if column_value is not None:
        return fernet.encrypt(column_value.encode()).decode()
    return None

encrypt_udf = spark.udf.register("encrypt_udf", encrypt_column, StringType())

# Drop the table if it exists
spark.sql("DROP TABLE IF EXISTS purgo_playground.customer_360_raw_clone")

# Create a replica table for processing
spark.sql("""
    CREATE TABLE purgo_playground.customer_360_raw_clone 
    AS SELECT * FROM purgo_playground.customer_360_raw
""")

# Read the replica table into a DataFrame
customer_360_raw_clone_df = spark.table("purgo_playground.customer_360_raw_clone")

# Encrypt specified PII columns using a UDF
customer_360_encrypted_df = customer_360_raw_clone_df.withColumn("name", encrypt_udf(col("name"))) \
    .withColumn("email", encrypt_udf(col("email"))) \
    .withColumn("phone", encrypt_udf(col("phone"))) \
    .withColumn("zip", encrypt_udf(col("zip")))

# Write encrypted data back to the customer_360_raw_clone table
customer_360_encrypted_df.write.mode("overwrite").saveAsTable("purgo_playground.customer_360_raw_clone")

# Save encryption key as a JSON file
current_datetime = datetime.now().strftime("%Y%m%d_%H%M%S")
key_file_path = f"/dbfs/Volumes/agilisium_playground/purgo_playground/de_dq/encryption_key_{current_datetime}.json"

with open(key_file_path, "w") as key_file:
    json.dump({"encryption_key": encryption_key.decode()}, key_file)

spark.stop()

# Performance test: Measure time taken to encrypt a large dataset (if required)

# Schema validation tests
# Create expected schema
expected_schema = StructType([
    StructField("id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("company", StringType(), True),
    StructField("job_title", StringType(), True),
    StructField("address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("country", StringType(), True),
    StructField("industry", StringType(), True),
    StructField("account_manager", StringType(), True),
    StructField("creation_date", StringType(), True),
    StructField("last_interaction_date", StringType(), True),
    StructField("purchase_history", StringType(), True),
    StructField("notes", StringType(), True),
    StructField("zip", StringType(), True)
])

# Assert schema matches expected
assert customer_360_encrypted_df.schema == expected_schema, "Schema does not match expected schema"

# Encryption Functionality validation tests
# Read encrypted data
encrypted_df = spark.table("purgo_playground.customer_360_raw_clone")

# Decryption and validation logic can be added here (if needed) for end-to-end integration testing
