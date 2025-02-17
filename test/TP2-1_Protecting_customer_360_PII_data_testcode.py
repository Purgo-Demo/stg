from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType
from cryptography.fernet import Fernet
import json
from datetime import datetime
import os

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Encrypt PII Data Testing") \
    .enableHiveSupport() \
    .getOrCreate()

# Define encryption logic
def get_encryption_key():
    return Fernet.generate_key()

def encrypt_value(fernet_key, value):
    return fernet_key.encrypt(value.encode()).decode() if value is not None else None

# Create UDF for encryption
encryption_key = Fernet(get_encryption_key())
encrypt_udf = udf(lambda x: encrypt_value(encryption_key, x), StringType())

# Drop previous clone table if exists
spark.sql("DROP TABLE IF EXISTS purgo_playground.customer_360_raw_clone")

# Create clone table for testing
spark.sql("CREATE TABLE purgo_playground.customer_360_raw_clone AS SELECT * FROM purgo_playground.customer_360_raw")

# Read the clone table into a DataFrame
customer_360_raw_clone_df = spark.table("purgo_playground.customer_360_raw_clone")

# Encrypt specified PII columns
customer_360_encrypted_df = customer_360_raw_clone_df.withColumn("name", encrypt_udf(col("name"))) \
    .withColumn("email", encrypt_udf(col("email"))) \
    .withColumn("phone", encrypt_udf(col("phone"))) \
    .withColumn("zip", encrypt_udf(col("zip")))

# Validate encryption (Check for non-null encrypted values)
assert customer_360_encrypted_df.filter(col("name").isNotNull()).count() == customer_360_raw_clone_df.count(), "Name encryption failed"
assert customer_360_encrypted_df.filter(col("email").isNotNull()).count() == customer_360_raw_clone_df.count(), "Email encryption failed"
assert customer_360_encrypted_df.filter(col("phone").isNotNull()).count() == customer_360_raw_clone_df.count(), "Phone encryption failed"
assert customer_360_encrypted_df.filter(col("zip").isNotNull()).count() == customer_360_raw_clone_df.count(), "ZIP encryption failed"

# Validate non-PII columns remain unchanged
comparison_df = customer_360_encrypted_df.join(customer_360_raw_clone_df, "id", "inner")
assert comparison_df.filter(col("company") != col("company_clone")).count() == 0, "Non-PII column modified"

# Write encrypted data back to the clone table
customer_360_encrypted_df.write.mode("overwrite").saveAsTable("purgo_playground.customer_360_raw_clone")

# Save encryption key as a JSON file
current_datetime = datetime.now().strftime("%Y%m%d_%H%M%S")
key_file_path = f"/dbfs/Volumes/agilisium_playground/purgo_playground/de_dq/encryption_key_{current_datetime}.json"

# Ensure directory exists
os.makedirs(os.path.dirname(key_file_path), exist_ok=True)

# Write the key to a file
with open(key_file_path, "w") as key_file:
    json.dump({"encryption_key": encryption_key._signing_key.decode()}, key_file)

# Validate encryption key file creation
assert os.path.exists(key_file_path), f"Failed to save encryption key at {key_file_path}"

spark.stop()

# Block comments detailing each test case included in the code
"""
Section: Schema validation
- Validate the structural integrity of encrypted data by ensuring non-PII columns remain unchanged

Section: Data type testing
- Include NULL handling for encryption logic

Section: Encryption validation
- Validate successful encryption of key PII columns ('name', 'email', 'phone', 'zip')

Section: Key management and validation
- Test the saving of encryption key to specified JSON file with timestamped naming
"""

