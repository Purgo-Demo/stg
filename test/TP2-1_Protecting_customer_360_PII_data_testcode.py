from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sha2, concat, lit
from datetime import datetime
import json
import os

# Initialize Spark session
spark = SparkSession.builder.appName("Test PII Encryption").getOrCreate()

# ---------------------------------------------------
# Setup: Drop the cloned table if it exists and recreate it from customer_360_raw
# ---------------------------------------------------

# Drop existing clone table if exists
spark.sql("DROP TABLE IF EXISTS purgo_playground.customer_360_raw_clone")

# Create clone table
spark.sql("""
  CREATE TABLE purgo_playground.customer_360_raw_clone 
  AS SELECT * FROM purgo_playground.customer_360_raw
""")

# Define encryption function
def encrypt_column(df, column_name, key):
    return df.withColumn(column_name, sha2(concat(col(column_name), lit(key)), 256))

# Load data from customer_360_raw
customer_raw_df = spark.table("purgo_playground.customer_360_raw_clone")

# Key for pseudo encryption (to be replaced with an actual encryption logic)
encryption_key = "sample_encryption_key"

# Encrypt PII columns
encrypted_df = customer_raw_df \
    .transform(lambda df: encrypt_column(df, 'name', encryption_key)) \
    .transform(lambda df: encrypt_column(df, 'email', encryption_key)) \
    .transform(lambda df: encrypt_column(df, 'phone', encryption_key)) \
    .transform(lambda df: encrypt_column(df, 'zip', encryption_key))

# ---------------------------------------------------
# Validate schema of the encrypted DataFrame
# ---------------------------------------------------

expected_schema = customer_raw_df.schema
if encrypted_df.schema != expected_schema:
    raise AssertionError("Schema validation failed after encryption.")

# Save encrypted data back to cloned table
encrypted_df.write.mode("overwrite").saveAsTable("purgo_playground.customer_360_raw_clone")

# ---------------------------------------------------
# JSON Key Storage
# Generate encryption key file name and save the key as a JSON
# ---------------------------------------------------

current_timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
encryption_key_path = f"/dbfs/Volumes/agilisium_playground/purgo_playground/de_dq/encryption_key_{current_timestamp}.json"
encryption_key_dict = {"encryption_key": encryption_key}

# Save the encryption key to a JSON file
os.makedirs(os.path.dirname(encryption_key_path), exist_ok=True)
with open(encryption_key_path, 'w') as file:
    json.dump(encryption_key_dict, file)

# ---------------------------------------------------
# Validation of Key File
# Check if the JSON file is appropriately saved
# ---------------------------------------------------

if not os.path.exists(encryption_key_path):
    raise FileNotFoundError(f"Encryption key JSON file was not found at {encryption_key_path}")

# Load saved JSON to check contents
with open(encryption_key_path, 'r') as file:
    saved_key = json.load(file)
    assert saved_key == encryption_key_dict, "Mismatch between expected and saved encryption keys."

# ---------------------------------------------------
# Clean up Spark session
# ---------------------------------------------------

spark.stop()

