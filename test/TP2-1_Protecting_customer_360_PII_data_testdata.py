from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, sha2, concat
import json
import datetime

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Encrypt PII Data") \
    .getOrCreate()

# Drop existing clone table if exists
spark.sql("DROP TABLE IF EXISTS purgo_playground.customer_360_raw_clone")

# Clone customer_360_raw table to customer_360_raw_clone
spark.sql("CREATE TABLE purgo_playground.customer_360_raw_clone AS SELECT * FROM purgo_playground.customer_360_raw")

# Define encryption function - using SHA-256 hash for example
def encrypt_column(df, column_name):
    """
    Encrypt a column using SHA-256.
    """
    return df.withColumn(column_name, sha2(col(column_name), 256))

# Load data from the cloned table
df = spark.table("purgo_playground.customer_360_raw_clone")

# Encrypt PII columns
pii_fields = ['name', 'email', 'phone', 'zip']
for column in pii_fields:
    df = encrypt_column(df, column)

# Save the modified DataFrame back to the clone table
df.write.mode("overwrite").saveAsTable("purgo_playground.customer_360_raw_clone")

# Create and save the encryption key
encryption_key = {"key": "example_key_123456"}  # Placeholder key
current_datetime = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
key_filename = f"/dbfs/Volumes/agilisium_playground/purgo_playground/de_dq/encryption_key_{current_datetime}.json"
with open(key_filename, "w") as key_file:
    json.dump(encryption_key, key_file)

# Print out path of encrypted data key
print(f"Encryption key saved to: {key_filename}")

spark.stop()


