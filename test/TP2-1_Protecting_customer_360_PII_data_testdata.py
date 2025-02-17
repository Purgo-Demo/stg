# Databricks PySpark script to encrypt PII columns and generate test data
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, concat
import pyspark.sql.functions as F
from pyspark.sql.types import (StructType, StructField, StringType, 
                               LongType, DateType, BooleanType, IntType)
import json
from datetime import datetime
import os

# Initialize Spark session
spark = SparkSession.builder.getOrCreate()

# Drop the existing table if it exists
spark.sql("DROP TABLE IF EXISTS purgo_playground.customer_360_raw_clone")

# Replicate the customer_360_raw table
spark.sql("""
CREATE TABLE purgo_playground.customer_360_raw_clone AS
SELECT * FROM purgo_playground.customer_360_raw
""")

# Load the encrypted data with PII columns
encryption_key = "my_secret_key"  # Replace with a proper key management solution

def encrypt_value(value):
    return f"enc_{value}"  # Example encryption function

# Encrypt specified columns
encrypted_df = spark.table("purgo_playground.customer_360_raw_clone") \
    .withColumn("name", F.udf(encrypt_value)(col("name"))) \
    .withColumn("email", F.udf(encrypt_value)(col("email"))) \
    .withColumn("phone", F.udf(encrypt_value)(col("phone"))) \
    .withColumn("zip", F.udf(encrypt_value)(col("zip")))

# Save encrypted data back to the table
encrypted_df.write.mode("overwrite").saveAsTable("purgo_playground.customer_360_raw_clone")

# Save the encryption key to a JSON file
key_data = {
    "encryption_key": encryption_key,
    "generated_at": datetime.now().isoformat()
}
file_path = f"/dbfs/Volumes/agilisium_playground/purgo_playground/de_dq/encryption_key_{datetime.now().strftime('%Y%m%d%H%M%S')}.json"

with open(file_path, 'w') as key_file:
    json.dump(key_data, key_file)

# Generate and show diverse test data

# Example schema
schema = StructType([
    StructField("id", LongType(), True),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("company", StringType(), True),
    StructField("zip", StringType(), True),
    StructField("creation_date", DateType(), True)
])

# Happy Path Data
happy_path_data = [
    (1, "Alice", "alice@example.com", "1234567890", "Wonderland Inc", "12345", "2024-03-21"),
    # Add more happy path records...
]

# Edge Case Data
edge_case_data = [
    (2, "", "@example.com", "", "Company", "99999", "2024-02-29"),  # Empty strings, leap day date
    # Add more edge case records...
]

# Error Case Data
error_case_data = [
    (3, "Bob", "bobexample.com", "abcdefghij", "Some Co", "00000", "2024-04-31"),  # Invalid email & phone, impossible date
    # Add more error case records...
]

# Special Characters and Multibyte Characters
special_character_data = [
    (4, "Chärlês", "chärlês@éxample.cöm", "特殊字符", "特殊公司", "特殊邮编", "2024-01-01"),  # Special and multibyte characters
    # Add more records with special characters...
]

# Combine all test data
test_data = happy_path_data + edge_case_data + error_case_data + special_character_data

# Create DataFrame and write to the target table
test_df = spark.createDataFrame(test_data, schema)
test_df.show()

# Validate replacements in the table purgo_playground.customer_360_raw_clone
test_df.write.mode("append").saveAsTable("purgo_playground.customer_360_raw_clone")

