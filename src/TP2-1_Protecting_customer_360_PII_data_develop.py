# PySpark script to encrypt PII data in customer_360_raw_clone table in Databricks environment

from pyspark.sql.functions import col, sha2, date_format, current_timestamp
import json

# Drop the table if it exists
spark.sql("DROP TABLE IF EXISTS purgo_playground.customer_360_raw_clone")

# Create a clone of the customer_360_raw table
spark.sql("""
  CREATE TABLE purgo_playground.customer_360_raw_clone
  AS SELECT * FROM purgo_playground.customer_360_raw
""")

# Read data from the clone table
df = spark.table("purgo_playground.customer_360_raw_clone")

# Encrypt PII columns using SHA-256
encrypted_df = df.withColumn("name", sha2(col("name"), 256)) \
    .withColumn("email", sha2(col("email"), 256)) \
    .withColumn("phone", sha2(col("phone"), 256)) \
    .withColumn("zip", sha2(col("zip"), 256))

# Define and save the encryption key to a JSON file
encryption_key = {"key": "unique_encryption_key_for_demo"}  # Placeholder
key_file_path = f"/Volumes/agilisium_playground/purgo_playground/de_dq/encryption_key_{date_format(current_timestamp(), 'yyyyMMdd_HHmmss')}.json"
dbutils.fs.put(key_file_path, json.dumps(encryption_key))

# Save the encrypted DataFrame back to Delta table
encrypted_df.write.format("delta").mode("overwrite").saveAsTable("purgo_playground.customer_360_raw_clone")

# Perform data validation to ensure the encryption process
# Validate schema remains unchanged except for encrypted columns
assert encrypted_df.schema == df.schema, "Schema has changed after encryption"

# Validate encryption key was saved
assert dbutils.fs.head(key_file_path) is not None, "Encryption key was not saved correctly"

# Validate row count remains the same
original_count = spark.table("purgo_playground.customer_360_raw").count()
clone_count = spark.table("purgo_playground.customer_360_raw_clone").count()
assert original_count == clone_count, "Row count mismatch after encryption"

# Perform cleanup if necessary
# (Not recommended to call drop in production unless it's required after validation)
# spark.sql("DROP TABLE IF EXISTS purgo_playground.customer_360_raw_clone")
