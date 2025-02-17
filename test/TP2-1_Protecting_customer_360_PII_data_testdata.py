from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, udf, current_timestamp, concat
from pyspark.sql.types import StringType
import json
from datetime import datetime
import os

# Initialize Spark session
spark = SparkSession.builder \
    .appName("TestDataEncryption") \
    .enableHiveSupport() \
    .getOrCreate()

# Create a random encryption function (placeholder)
def encrypt_value(value):
    # Assuming ROT13 as dummy encryption for illustration
    return ''.join([chr((ord(char) - 97 + 13) % 26 + 97) if 'a' <= char <= 'z' else char for char in value.lower()])

# Register UDF
encrypt_udf = udf(encrypt_value, StringType())

# Drop customer_360_raw_clone if it exists and create a clone
spark.sql("DROP TABLE IF EXISTS purgo_playground.customer_360_raw_clone")
spark.sql("CREATE TABLE purgo_playground.customer_360_raw_clone AS SELECT * FROM purgo_playground.customer_360_raw")

# Load data from the replica table
data_df = spark.table("purgo_playground.customer_360_raw_clone")

# Encrypt PII columns
encrypted_df = data_df.withColumn("name", encrypt_udf(col("name"))) \
                      .withColumn("email", encrypt_udf(col("email"))) \
                      .withColumn("phone", encrypt_udf(col("phone"))) \
                      .withColumn("zip", encrypt_udf(col("zip")))

# Save back to the clone table
encrypted_df.write.mode("overwrite").saveAsTable("purgo_playground.customer_360_raw_clone")

# Create encryption key (dummy example) as JSON
encryption_key = {
    "key": "dummy_key_for_encryption"
}
current_datetime = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
encryption_key_path = f"/dbfs/Volumes/agilisium_playground/purgo_playground/de_dq/encryption_key_{current_datetime}.json"

# Save the encryption key JSON
with open(encryption_key_path, 'w') as key_file:
    json.dump(encryption_key, key_file)

print(f"Encryption key saved to {encryption_key_path}")

# Generate test data
test_data = [
    (1, "Alice", "alice@example.com", "1234567890", "Wonderland Inc.", "Engineer", "123 Lane", "City", "State", "Country", "Tech", "Manager", "2023-01-01", "2023-03-21", "{}", "", "12345"),
    (2, "Bob Smith", "bsmith@domain.com", "0987654321", "Business Co.", "Analyst", "456 Street", "Town", "Region", "Country", "Finance", "Lead", "2023-02-01", "2023-02-25", "{}", "", None),
    # Edge cases and error cases
    (3, "C@rl<>", "invalid-email@", "phone!", "Company", "Title", "", "Village", "", "", "", "", "2023-03-05", "2023-04-12", "{}", "", "00001"), # special chars, invalid email
    (4, None, None, None, "Company B", "Admin", "789 Avenue", "Metropolis", "District", "Nation", "Retail", "Admin", "2023-03-03", "2023-03-04", "{}", None, "54321"), # NULL handling
    # ... more records ...
]

# Define schema
schema_fields = [
    "id", "name", "email", "phone", "company", "job_title", "address",
    "city", "state", "country", "industry", "account_manager", "creation_date",
    "last_interaction_date", "purchase_history", "notes", "zip"
]

# Create DataFrame with test data
test_df = spark.createDataFrame(test_data, schema=schema_fields)

# Save test data as a table for further testing
test_df.write.mode("overwrite").saveAsTable("purgo_playground.customer_360_raw_clone_test_data")

# Stop Spark session
spark.stop()

