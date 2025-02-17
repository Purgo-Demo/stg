# Databricks PySpark script for encrypting PII data using an encryption algorithm (e.g., AES encryption)
# This script assumes an appropriate encryption library is used that supports AES encryption.
# Note: AES encryption details like key management are omitted in this mock-up.

from pyspark.sql.functions import col, lit, current_timestamp, date_format, expr
from pyspark.sql.types import StringType
from pyspark.sql import DataFrame
import json
from Crypto.Cipher import AES
import base64
import os

# Encryption function for PII data using AES
def encrypt(text, secret_key):
    """Encrypt the plain text using AES encryption."""
    cipher = AES.new(secret_key, AES.MODE_EAX)
    nonce = cipher.nonce
    ciphertext, tag = cipher.encrypt_and_digest(text.encode("utf-8"))
    return base64.b64encode(nonce + ciphertext).decode("utf-8")

# Generate a secret key for AES encryption
secret_key = os.urandom(32)  # 256-bit key

# Store the encryption key as JSON
encryption_key = {"key": base64.b64encode(secret_key).decode("utf-8")}
key_file_path = f"/Volumes/agilisium_playground/purgo_playground/de_dq/encryption_key_{date_format(current_timestamp(), 'yyyyMMdd_HHmmss')}.json"

# Drop the old clone table if it exists
spark.sql("DROP TABLE IF EXISTS purgo_playground.customer_360_raw_clone")

# Clone the raw customer table to a new clone table for processing
spark.sql("""
  CREATE TABLE purgo_playground.customer_360_raw_clone
  AS SELECT * FROM purgo_playground.customer_360_raw
""")

# Load data from the cloned table
df: DataFrame = spark.table("purgo_playground.customer_360_raw_clone")

# UDF for encrypting columns
encrypt_udf = spark.udf.register("encrypt_udf", lambda val: encrypt(val, secret_key) if val else val, StringType())

# Encrypt PII columns and create an encrypted DataFrame
encrypted_df = df.withColumn("name", encrypt_udf(col("name"))) \
    .withColumn("email", encrypt_udf(col("email"))) \
    .withColumn("phone", encrypt_udf(col("phone"))) \
    .withColumn("zip", encrypt_udf(col("zip")))

# Write the encrypted DataFrame back to the clone table
encrypted_df.write.format("delta").mode("overwrite").saveAsTable("purgo_playground.customer_360_raw_clone")

# Save the encryption key as a JSON file
dbutils.fs.put(key_file_path, json.dumps(encryption_key), overwrite=True)

# Perform validations
# Validate that encryption key was saved successfully
assert dbutils.fs.head(key_file_path) is not None, "Encryption key was not saved correctly"

# Validate schema to ensure structure remains consistent
assert encrypted_df.schema == df.schema, "Schema mismatch after encryption"

# Data quality checks to ensure data consistency
original_df = spark.table("purgo_playground.customer_360_raw").select("id", "name", "email", "phone", "zip")
modified_df = spark.table("purgo_playground.customer_360_raw_clone").select("id")

assert original_df.count() == modified_df.count(), "Row count mismatch after encryption"

# Optional: optimize the table and remove old files to save space (vacuum)
spark.sql("""
  OPTIMIZE purgo_playground.customer_360_raw_clone
  ZORDER BY (id)
""")

spark.sql("""
  VACUUM purgo_playground.customer_360_raw_clone RETAIN 0 HOURS
""")

