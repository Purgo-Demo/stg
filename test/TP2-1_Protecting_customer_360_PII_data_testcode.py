from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, udf
from pyspark.sql.types import StringType
from pyspark.sql.functions import current_timestamp
import json
import datetime
import os

# Initialize Spark session
spark = SparkSession.builder.appName("Encrypt PII Data").getOrCreate()

# Create a replica of the original table
source_table = "purgo_playground.customer_360_raw"
replica_table = "purgo_playground.customer_360_raw_clone"

# Drop if exists and create a replica of the source table
spark.sql(f"DROP TABLE IF EXISTS {replica_table}")
spark.sql(f"CREATE TABLE {replica_table} AS SELECT * FROM {source_table}")

# Load DataFrame from the created replica table
df = spark.table(replica_table)

# Define a simple encryption placeholder function. Replace this with appropriate encryption logic.
def encrypt_pii(value: str) -> str:
    if value:
        return f"enc({value})"
    return value

# Register UDF
encrypt_udf = udf(encrypt_pii, StringType())

# Encrypt PII columns
encrypted_df = df.withColumn("name", encrypt_udf(col("name"))) \
    .withColumn("email", encrypt_udf(col("email"))) \
    .withColumn("phone", encrypt_udf(col("phone"))) \
    .withColumn("zip", encrypt_udf(col("zip")))

# Validate that DataFrame schema matches expected schema for encrypted data
expected_schema = df.schema
assert encrypted_df.schema == expected_schema, "Schema mismatch after encryption"

# Show the encrypted_df for manual validation
encrypted_df.show(truncate=False)

# Save the DataFrame as a Delta table
encrypted_df.write.format("delta").mode("overwrite").saveAsTable(replica_table)

# Save the encryption key to JSON file
encryption_key = {
    "name_key": "key_for_name",
    "email_key": "key_for_email",
    "phone_key": "key_for_phone",
    "zip_key": "key_for_zip"
}
current_datetime = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
key_filename = f"/dbfs/Volumes/agilisium_playground/purgo_playground/de_dq/encryption_key_{current_datetime}.json"

# Ensure the directory exists
os.makedirs(os.path.dirname(key_filename), exist_ok=True)

# Save encryption key file
with open(key_filename, "w") as f:
    json.dump(encryption_key, f)

# Validate the filename and directory of the saved encryption key
assert os.path.isfile(key_filename), "Encryption key file was not saved correctly"

# Validate Delta Lake operations
assert "id" in encrypted_df.columns, "ID column is missing after Delta operation"

# Cleanup operations
# Drop the temporary replica table
spark.sql(f"DROP TABLE IF EXISTS {replica_table}")

