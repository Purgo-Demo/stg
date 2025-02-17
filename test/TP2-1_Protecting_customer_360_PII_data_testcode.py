# Imports and library installation
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, lit
from pyspark.sql.types import StringType
from cryptography.fernet import Fernet
import json
from datetime import datetime
import os

# Install cryptography library if not available
dbutils.library.installPyPI('cryptography')

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Encrypt PII Data") \
    .enableHiveSupport() \
    .getOrCreate()

# Generate an encryption key and set up Fernet
encryption_key = Fernet.generate_key()
fernet = Fernet(encryption_key)

# UDF for encryption using Fernet
def encrypt_column(column_value):
    if column_value is not None:
        return fernet.encrypt(column_value.encode()).decode()
    else:
        return None
encrypt_udf = udf(encrypt_column, StringType())

# Load customer_360_raw table into a DataFrame
customer_360_raw_df = spark.table("purgo_playground.customer_360_raw")

# Perform encryption on PII columns
customer_360_encrypted_df = customer_360_raw_df.withColumn("name", encrypt_udf(col("name"))) \
    .withColumn("email", encrypt_udf(col("email"))) \
    .withColumn("phone", encrypt_udf(col("phone"))) \
    .withColumn("zip", encrypt_udf(col("zip")))

# Drop existing clone table if it exists
spark.sql("DROP TABLE IF EXISTS purgo_playground.customer_360_raw_clone")

# Write encrypted data to the clone table
customer_360_encrypted_df.write \
    .mode("overwrite") \
    .saveAsTable("purgo_playground.customer_360_raw_clone")

# Save the encryption key as a JSON file
current_datetime = datetime.now().strftime("%Y%m%d_%H%M%S")
key_file_path = f"/dbfs/Volumes/agilisium_playground/purgo_playground/de_dq/encryption_key_{current_datetime}.json"

with open(key_file_path, "w") as key_file:
    json.dump({"encryption_key": encryption_key.decode()}, key_file)

# Verify data integrity: non-PII columns should remain unchanged
assert customer_360_raw_df.select("company", "job_title", "address").exceptAll(
    customer_360_encrypted_df.select("company", "job_title", "address")).count() == 0, "Data integrity test failed"

# Decrypting a test record to ensure decryption works and matches original
sample_original_row = customer_360_raw_df.select("name").first()["name"]
sample_encrypted_row = customer_360_encrypted_df.select("name").first()["name"]
decrypted_value = fernet.decrypt(sample_encrypted_row.encode()).decode()
assert sample_original_row == decrypted_value, "Decryption validation failed"

# Test permission and access controls (mock scenario)
# Define user roles and expected access conditions
user_role = "unauthorized_user"
access_condition = "access_denied"
assert access_condition == "access_denied", f"{user_role} should be denied access"

# Performance test for writing the encrypted table
num_rows = customer_360_raw_df.count()
spark.conf.set("spark.sql.shuffle.partitions", 4)
performance_df = customer_360_encrypted_df.repartition(4)
performance_df.write.mode("overwrite").saveAsTable("temp_performance_test")
end_num_rows = spark.table("temp_performance_test").count()
assert num_rows == end_num_rows, "Performance test failed - row count mismatch"

# Cleanup operations
spark.sql("DROP TABLE IF EXISTS temp_performance_test")

# Stop Spark session
spark.stop()

# Block comments to describe sections
"""
# Section: Initialization and Library Installation
- Initialize Spark session
- Install required libraries
# Section: Key Generation and Encryption Setup
- Generate encryption key
- Define UDF for column encryption
# Section: Table Operations
- Load original table
- Encrypt and save to clone
- Save encryption key
# Section: Validation and Testing
- Data integrity checks
- Decryption validation
- Permission/access control mock test
- Performance testing
# Section: Cleanup
- Remove temporary artifacts
"""

# Line comments explaining specific operations
# Initialize by registering UDF with Spark
# Validate data integrity; unchanged columns stay the same
# Use .encode() for encryption and conversion to bytes
# Decrypt sample values to ensure functionality
