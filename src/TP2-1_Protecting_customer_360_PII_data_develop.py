# Import necessary libraries and define required functions
from pyspark.sql.functions import col, sha2, when, current_timestamp, date_format
import json

# Drop the existing customer_360_raw_clone table if it exists
spark.sql("DROP TABLE IF EXISTS purgo_playground.customer_360_raw_clone")

# Clone customer_360_raw table to customer_360_raw_clone
spark.sql("""
  CREATE TABLE purgo_playground.customer_360_raw_clone
  AS SELECT * FROM purgo_playground.customer_360_raw
""")

# Load the cloned data into a DataFrame for transformation
df = spark.table("purgo_playground.customer_360_raw_clone")

# Encrypt the PII columns using SHA-256
encrypted_df = df.withColumn("name", when(col("name").isNotNull(), sha2(col("name"), 256)).otherwise(None)) \
    .withColumn("email", when(col("email").isNotNull(), sha2(col("email"), 256)).otherwise(None)) \
    .withColumn("phone", when(col("phone").isNotNull(), sha2(col("phone"), 256)).otherwise(None)) \
    .withColumn("zip", when(col("zip").isNotNull(), sha2(col("zip"), 256)).otherwise(None))

# Write the transformed data back to the customer_360_raw_clone table with Delta format
encrypted_df.write.format("delta").mode("overwrite").saveAsTable("purgo_playground.customer_360_raw_clone")

# Generate an encryption key for demonstration purposes
encryption_key = {"key": "static_dummy_key"}
key_file_path = f"/Volumes/agilisium_playground/purgo_playground/de_dq/encryption_key_{date_format(current_timestamp(), 'yyyyMMdd_HHmmss')}.json"

# Save the encryption key as a JSON file at specified location
dbutils.fs.put(key_file_path, json.dumps(encryption_key), overwrite=True)

# Perform simple validation checks
assert dbutils.fs.head(key_file_path) is not None, "Error: Encryption key was not saved."

# Verify encrypted DataFrame to ensure encryption
encrypted_df.show(truncate=False)
