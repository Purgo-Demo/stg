from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, udf
from pyspark.sql.types import StringType
from pyspark.sql.functions import current_timestamp
import json
import datetime
import os

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Databricks Test Code for PII Encryption") \
    .enableHiveSupport() \
    .getOrCreate()

# Define file paths
source_table = "purgo_playground.customer_360_raw"
clone_table = "purgo_playground.customer_360_raw_clone"
encryption_key_path = "/dbfs/Volumes/agilisium_playground/purgo_playground/de_dq"

# Drop table if exists
spark.sql(f"DROP TABLE IF EXISTS {clone_table}")

# Create a replica of customer_360_raw
spark.sql(f"CREATE TABLE {clone_table} AS SELECT * FROM {source_table}")

# Define a simple encryption placeholder function
def encrypt_pii(value: str) -> str:
    if value:
        return f"enc({value})"
    return value

# Register UDF
encrypt_udf = udf(encrypt_pii, StringType())

# Read the clone table
df_clone = spark.table(clone_table)

# Encrypt PII columns
encrypted_df = df_clone.withColumn("name", encrypt_udf(col("name"))) \
    .withColumn("email", encrypt_udf(col("email"))) \
    .withColumn("phone", encrypt_udf(col("phone"))) \
    .withColumn("zip", encrypt_udf(col("zip")))

# Overwrite the encrypted data back to the clone table
encrypted_df.write.format("delta").mode("overwrite").saveAsTable(clone_table)

# Validate schema
expected_schema = df_clone.schema
encrypted_schema = spark.table(clone_table).schema
assert expected_schema == encrypted_schema, "Schema validation failed"

# Save the encryption key to JSON file
encryption_key = {
    "name_key": "key_for_name",
    "email_key": "key_for_email",
    "phone_key": "key_for_phone",
    "zip_key": "key_for_zip"
}
current_datetime = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
key_filename = f"{encryption_key_path}/encryption_key_{current_datetime}.json"

# Save encryption key file
with open(key_filename, "w") as f:
    json.dump(encryption_key, f)

# Verify encryption key file existence
assert os.path.exists(key_filename), f"Encryption key file was not saved: {key_filename}"

# Clean up operations (optional): Remove test files or intermediate tables if needed

