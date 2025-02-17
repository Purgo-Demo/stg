# Required Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, current_timestamp, struct, concat, lit
from pyspark.sql.types import StringType, TimestampType
from cryptography.fernet import Fernet
import json
import os

# Initialize Spark Session
spark = SparkSession.builder.appName('EncryptPIIDataTests').getOrCreate()

# Drop the customer_360_raw_clone table if it exists
spark.sql("DROP TABLE IF EXISTS purgo_playground.customer_360_raw_clone")

# Create a replica of customer_360_raw as customer_360_raw_clone
spark.sql("""
CREATE TABLE purgo_playground.customer_360_raw_clone AS 
SELECT * FROM purgo_playground.customer_360_raw
""")

# Define an encryption function UDF
@udf(returnType=StringType())
def encrypt(value):
    if value is not None:
        return cipher.encrypt(value.encode()).decode()
    else:
        return None

# Generate encryption key
encryption_key = Fernet.generate_key()
cipher = Fernet(encryption_key)

# Load Data from customer_360_raw_clone
customer_360_df = spark.read.table("purgo_playground.customer_360_raw_clone")

# Encrypt PII columns
encrypted_df = customer_360_df.withColumn("name", encrypt(col("name"))) \
    .withColumn("email", encrypt(col("email"))) \
    .withColumn("phone", encrypt(col("phone"))) \
    .withColumn("zip", encrypt(col("zip")))

# Save encrypted data to the clone table
encrypted_df.write.mode("overwrite").saveAsTable("purgo_playground.customer_360_raw_clone")

# Generate a JSON filename with the current timestamp
json_file_name = f"encryption_key_{current_timestamp().alias('current_timestamp')}.json"

# Write encryption key to JSON file
json_file_path = f"/Volumes/agilisium_playground/purgo_playground/de_dq/{json_file_name}"

encryption_key_data = {"encryption_key": encryption_key.decode()}
with open(json_file_path, 'w') as json_file:
    json.dump(encryption_key_data, json_file)

# Test case for schema validation
expected_schema = customer_360_df.schema
assert encrypted_df.schema == expected_schema, "Schema Validation Failed"

# Test case for ensuring column encryption
encrypted_sample = encrypted_df.select("name", "email", "phone", "zip").first()
assert all(
    encrypted_sample[col_name] is not None for col_name in ["name", "email", "phone", "zip"]
), "Column Encryption Failed"

# Test case for null handling
null_handled_df = spark.createDataFrame([
    (None, None, None, None)
], schema=["name", "email", "phone", "zip"])

encrypted_null_df = null_handled_df.select(
    encrypt("name").alias("name"),
    encrypt("email").alias("email"),
    encrypt("phone").alias("phone"),
    encrypt("zip").alias("zip")
)

encrypted_null_values = encrypted_null_df.first()
assert all(
    encrypted_null_values[col_name] is None for col_name in ["name", "email", "phone", "zip"]
), "Null Handling Test Failed"

# Cleanup after tests
spark.sql("DROP TABLE IF EXISTS purgo_playground.customer_360_raw_clone")
os.remove(json_file_path)

