from pyspark.sql.functions import col, sha2, concat, lit
from datetime import datetime
import json
import os

# ---------------------------------------------------
# Setup: Drop the cloned table if it exists and recreate it from customer_360_raw
# ---------------------------------------------------

# Drop existing clone table if it exists
spark.sql("DROP TABLE IF EXISTS purgo_playground.customer_360_raw_clone")

# Create a clone of the customer_360_raw table
spark.sql("""
  CREATE TABLE purgo_playground.customer_360_raw_clone 
  AS SELECT * FROM purgo_playground.customer_360_raw
""")

# Function to encrypt columns
def encrypt_column(df, column_name, key):
    return df.withColumn(column_name, sha2(concat(col(column_name), lit(key)), 256))

# Load data from the cloned table
customer_raw_df = spark.table("purgo_playground.customer_360_raw_clone")

# Example encryption key for demonstration purposes
encryption_key = os.urandom(16).hex()

# Encrypt the PII columns in the DataFrame
encrypted_df = customer_raw_df \
    .transform(lambda df: encrypt_column(df, 'name', encryption_key)) \
    .transform(lambda df: encrypt_column(df, 'email', encryption_key)) \
    .transform(lambda df: encrypt_column(df, 'phone', encryption_key)) \
    .transform(lambda df: encrypt_column(df, 'zip', encryption_key))

# Validate schema
expected_schema = customer_raw_df.schema
if encrypted_df.schema != expected_schema:
    raise AssertionError("Schema validation failed after encryption.")

# Save encrypted data back to the table
encrypted_df.write.format("delta").mode("overwrite").saveAsTable("purgo_playground.customer_360_raw_clone")

# ---------------------------------------------------
# Save the encryption key to a JSON file
# ---------------------------------------------------

current_timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
encryption_key_path = f"/dbfs/Volumes/agilisium_playground/purgo_playground/de_dq/encryption_key_{current_timestamp}.json"
encryption_key_dict = {"encryption_key": encryption_key}

# Save the encryption key
os.makedirs(os.path.dirname(encryption_key_path), exist_ok=True)
with open(encryption_key_path, 'w') as file:
    json.dump(encryption_key_dict, file)

# Check that the JSON file was saved correctly
if not os.path.exists(encryption_key_path):
    raise FileNotFoundError(f"Encryption key JSON file was not found at {encryption_key_path}")

# Load and validate the saved encryption key
with open(encryption_key_path, 'r') as file:
    saved_key = json.load(file)
    assert saved_key == encryption_key_dict, "Mismatch between expected and saved encryption keys."
