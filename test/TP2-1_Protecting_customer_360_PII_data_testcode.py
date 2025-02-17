# Databricks PySpark Test Code for Encrypting PII Data
# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType
import json
from datetime import datetime
import os

# Initialize the Spark session
spark = SparkSession.builder \
    .appName("Databricks PII Encryption Test") \
    .getOrCreate()

# Define schema for customer_360_raw table
customer_360_raw_schema = StructType([
    StructField("id", LongType(), True),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("company", StringType(), True),
    StructField("job_title", StringType(), True),
    StructField("address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("country", StringType(), True),
    StructField("industry", StringType(), True),
    StructField("account_manager", StringType(), True),
    StructField("creation_date", TimestampType(), True),
    StructField("last_interaction_date", TimestampType(), True),
    StructField("purchase_history", StringType(), True),
    StructField("notes", StringType(), True),
    StructField("zip", StringType(), True)
])

# Sample data for testing
customer_360_raw_data = [
    {
        "id": 1,
        "name": "Jane Doe",
        "email": "jane.doe@example.com",
        "phone": "+12345678901",
        "company": "Example Corp",
        "job_title": "Engineer",
        "address": "456 Elm St",
        "city": "Sample City",
        "state": "SC",
        "country": "USA",
        "industry": "Engineering",
        "account_manager": "Manager 1",
        "creation_date": "2025-01-01T00:00:00.000+0000",
        "last_interaction_date": "2025-02-01T00:00:00.000+0000",
        "purchase_history": "Item X, Item Y",
        "notes": "VIP customer",
        "zip": "54321"
    }
]

# Create DataFrame from sample data
customer_360_raw_df = spark.createDataFrame(data=customer_360_raw_data, schema=customer_360_raw_schema)

# Show the raw data
customer_360_raw_df.show()

# Encrypt specified PII columns
def encrypt_column(df, col_name):
    """Encrypts a column using SHA-256 algorithm."""
    return df.withColumn(col_name, F.sha2(F.col(col_name), 256))

# Encrypt the PII columns: name, email, phone, zip
pii_columns = ['name', 'email', 'phone', 'zip']
for col in pii_columns:
    customer_360_raw_df = encrypt_column(customer_360_raw_df, col)

# Generate encryption key (For demonstration, use a hardcoded dummy key)
encryption_key = {
    "key": "dummy_encryption_key_1234567890"
}

# Define path to save encryption key
output_path = "/Volumes/agilisium_playground/purgo_playground/de_dq"
encryption_key_file = os.path.join(output_path, f"encryption_key_{datetime.now().strftime('%Y%m%d%H%M%S')}.json")

# Save encryption key to JSON file
with open(encryption_key_file, 'w') as key_file:
    json.dump(encryption_key, key_file)

# Log encryption key save location
print(f"Encryption key saved to: {encryption_key_file}")

# Drop table if it exists and replicate purgo_playground.customer_360_raw to purgo_playground.customer_360_raw_clone
spark.sql("DROP TABLE IF EXISTS purgo_playground.customer_360_raw_clone")
customer_360_raw_df.write.mode("overwrite").saveAsTable("purgo_playground.customer_360_raw_clone")

# Verify that the encrypted data is saved correctly
loaded_df = spark.table("purgo_playground.customer_360_raw_clone")
loaded_df.show()

# Clean up resources
spark.stop()
