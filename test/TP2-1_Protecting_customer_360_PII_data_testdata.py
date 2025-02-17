from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, udf, when
from pyspark.sql.types import StructType, StructField, StringType, DateType, BooleanType, LongType, DoubleType
import json
import secrets
import os
from datetime import datetime

# Initialize Spark session
spark = SparkSession.builder.appName("Encrypt PII Data").getOrCreate()

# Define UDF for encryption
def dummy_encrypt(value, key):
    return value[::-1] + key[:5]  # Simple reversible encryption (for demonstration only)

encryption_key = secrets.token_hex(16)

encrypt_udf = udf(lambda val: dummy_encrypt(val, encryption_key), StringType())

# Define the schema for the purgo_playground.customer_360_raw table
schema = StructType([
    StructField("id", LongType(), True),
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
    StructField("creation_date", DateType(), True),
    StructField("last_interaction_date", DateType(), True),
    StructField("purchase_history", StringType(), True),
    StructField("notes", StringType(), True),
    StructField("zip", StringType(), True)
])

# Load data from purgo_playground.customer_360_raw table
customer_360_raw_df = spark.createDataFrame([], schema)

# Drop clone table if exists
spark.sql("DROP TABLE IF EXISTS purgo_playground.customer_360_raw_clone")

# Create a new clone table
customer_360_raw_df.write.saveAsTable("purgo_playground.customer_360_raw_clone")

# Load data from the created clone table
customer_360_raw_clone_df = spark.table("purgo_playground.customer_360_raw_clone")

# Encrypt the PII columns
encrypted_df = customer_360_raw_clone_df \
    .withColumn("name", encrypt_udf(col("name"))) \
    .withColumn("email", encrypt_udf(col("email"))) \
    .withColumn("phone", encrypt_udf(col("phone"))) \
    .withColumn("zip", encrypt_udf(col("zip")))

# Overwrite the clone table with the encrypted data
encrypted_df.write.mode('overwrite').saveAsTable("purgo_playground.customer_360_raw_clone")

# Save the encryption key as a JSON file
key_file_path = f"/Volumes/agilisium_playground/purgo_playground/de_dq/encryption_key_{datetime.now().strftime('%Y%m%d%H%M%S')}.json"
with open(key_file_path, "w") as key_file:
    json.dump({"encryption_key": encryption_key}, key_file)
    
# Verify saved key location
assert os.path.exists(key_file_path), f"Error: Unable to save encryption key at {key_file_path}"

