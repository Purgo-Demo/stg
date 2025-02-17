from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, struct, current_timestamp, concat_ws
from pyspark.sql.types import (
    StructType, StructField, StringType, BigIntType, DateType
)
from cryptography.fernet import Fernet
import json
import dbutils

# Initialize Spark session
spark = SparkSession.builder \
    .appName("EncryptPIIData") \
    .getOrCreate()

# Schema for test data generation
schema = StructType([
    StructField("id", BigIntType(), True),
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
    StructField("creation_date", DateType(), True),
    StructField("last_interaction_date", DateType(), True),
    StructField("purchase_history", StringType(), True),
    StructField("notes", StringType(), True),
    StructField("zip", StringType(), True)
])

# Generate test data
data = [
    (1, "John Doe", "john@example.com", "+1234567890", "Example Corp", "CEO", "123 Elm St", 
     "Anytown", "AN", "USA", "Software", "Alice", "2023-01-01", "2023-05-01", "['item1', 'item2']", "Great customer", "12345"),
    (2, "Jane Doe", "jane@domain.com", "+0987654321", "Domain Inc", "CTO", "456 Oak St",
     "Anycity", "NY", "USA", "Technology", "Bob", "2022-02-15", "2023-03-10", "['item2']", "Frequent spender", "67890"),
    # ...
    # Add more diverse test records covering edge cases, null values, etc.
]

# Create DataFrame
df = spark.createDataFrame(data, schema)

# Encrypt specified PII columns (name, email, phone, zip) in the DataFrame
key = Fernet.generate_key()
cipher = Fernet(key)

df_encrypted = df.withColumn("name", lit(cipher.encrypt(df.name.cast("string").to_bytes("utf-8")).decode()))
df_encrypted = df_encrypted.withColumn("email", lit(cipher.encrypt(df.email.cast("string").to_bytes("utf-8")).decode()))
df_encrypted = df_encrypted.withColumn("phone", lit(cipher.encrypt(df.phone.cast("string").to_bytes("utf-8")).decode()))
df_encrypted = df_encrypted.withColumn("zip", lit(cipher.encrypt(df.zip.cast("string").to_bytes("utf-8")).decode()))

# Drop existing table if it exists
spark.sql("DROP TABLE IF EXISTS purgo_playground.customer_360_raw_clone")

# Write the encrypted DataFrame back to the clone table
df_encrypted.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("purgo_playground.customer_360_raw_clone")

# Generate encryption key JSON file
current_datetime = current_timestamp().strftime("%Y%m%d%H%M%S")
encryption_key_file_path = f'/Volumes/agilisium_playground/purgo_playground/de_dq/encryption_key_{current_datetime}.json'

encryption_key_data = {"encryption_key": key.decode()}
with open(encryption_key_file_path, 'w') as key_file:
    json.dump(encryption_key_data, key_file)

# Save key to Databricks filesystem
dbutils.fs.put(encryption_key_file_path, json.dumps(encryption_key_data))

# Completed data encryption and storage
print("Test data generation and encryption process completed.")

