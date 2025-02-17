# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, current_timestamp
from pyspark.sql.types import StringType
import json
import datetime
import os

# Initialize Spark session for Databricks
spark = SparkSession \
    .builder \
    .appName("PII Encryption Test") \
    .getOrCreate()

# Define schema for customer_360_raw table
schema = """
    id BIGINT,
    name STRING,
    email STRING,
    phone STRING,
    company STRING,
    job_title STRING,
    address STRING,
    city STRING,
    state STRING,
    country STRING,
    industry STRING,
    account_manager STRING,
    creation_date DATE,
    last_interaction_date DATE,
    purchase_history STRING,
    notes STRING,
    zip STRING
"""

# Define test data
data = [
    (1, "Alice Smith", "alice@example.com", "1234567890", "ABC Corp", "Manager", "123 Elm St", "Metropolis", "NY", "USA", "Finance", "John Doe", "2024-01-01", "2024-02-28", "None", "First purchase in 2024", "10001"),
    (2, "Bob Johnson", "bob.johnson@example.com", "0987654321", None, "Director", None, "Gotham", "NJ", "USA", "IT", "Jane Doe", None, "2024-03-01", "Repeat customer", "Important client", "07001")
]

# Create DataFrame from test data
df = spark.createDataFrame(data, schema=schema)

# Define encryption function
def encrypt_pii(value):
    return f"enc({value})" if value else None

# Register UDF for encryption
encrypt_udf = udf(encrypt_pii, StringType())

# Apply encryption on PII columns
encrypted_df = df.withColumn("name", encrypt_udf(col("name"))) \
                 .withColumn("email", encrypt_udf(col("email"))) \
                 .withColumn("phone", encrypt_udf(col("phone"))) \
                 .withColumn("zip", encrypt_udf(col("zip")))

# Validate the encryption by showing the result
encrypted_df.show(truncate=False)

# Write the encrypted DataFrame to Delta table
destination_table = "purgo_playground.customer_360_raw_clone"
encrypted_df.write.format("delta").mode("overwrite").saveAsTable(destination_table)

# Save encryption key as JSON
encryption_key = {
    "name_key": "key_for_name",
    "email_key": "key_for_email",
    "phone_key": "key_for_phone",
    "zip_key": "key_for_zip"
}

# Get current datetime for file naming
current_datetime = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
key_filename = f"/dbfs/Volumes/agilisium_playground/purgo_playground/de_dq/encryption_key_{current_datetime}.json"

# Check if directory exists, if not create it
if not os.path.exists(os.path.dirname(key_filename)):
    os.makedirs(os.path.dirname(key_filename))

# Write encryption key to JSON file
with open(key_filename, "w") as key_file:
    json.dump(encryption_key, key_file)

# -- Validate the proper creation and storage of the JSON encryption key

# Function to test if JSON encryption file was created successfully
def test_json_file_exists():
    try:
        assert os.path.exists(key_filename)
        print("Test Passed: JSON encryption key file created successfully.")
    except AssertionError:
        print("Test Failed: JSON encryption key file not found.")

# Execute test
test_json_file_exists()

# -- Additional Tests (e.g., for schema validation, PII columns, SQL operations) can be added below.
