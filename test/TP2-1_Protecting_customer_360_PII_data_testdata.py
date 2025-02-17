from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, udf, current_timestamp, struct, to_timestamp
from pyspark.sql.types import StringType, TimestampType
from cryptography.fernet import Fernet
import json
import os

# Initialize Spark session
spark = SparkSession.builder.appName('EncryptPIIData').getOrCreate()

# Generate encryption key
encryption_key = Fernet.generate_key()
cipher = Fernet(encryption_key)

# Encryption UDF
@udf(returnType=StringType())
def encrypt(value):
    if value is not None:
        return cipher.encrypt(value.encode()).decode()
    else:
        return None

# Load customer_360_raw table
customer_360_df = spark.read.table("purgo_playground.customer_360_raw")

# Handle PII encryption for cloned table
customer_360_clone_df = customer_360_df.withColumn("name", encrypt(col("name"))) \
    .withColumn("email", encrypt(col("email"))) \
    .withColumn("phone", encrypt(col("phone"))) \
    .withColumn("zip", encrypt(col("zip")))

# Save to customer_360_raw_clone
customer_360_clone_df.write.mode("overwrite").saveAsTable("purgo_playground.customer_360_raw_clone")

# Generate JSON filename with current timestamp
current_datetime = current_timestamp().sql('<string>').replace(':', '').replace(' ', '').replace('-', '')
json_file_name = f"encryption_key_{current_datetime}.json"
json_file_path = f"/Volumes/agilisium_playground/purgo_playground/de_dq/{json_file_name}"

# Save encryption key to JSON file
encryption_key_data = {"encryption_key": encryption_key.decode()}
with open(json_file_path, 'w') as json_file:
    json.dump(encryption_key_data, json_file)

# Sample Test Data for various scenarios

# Happy path test data
test_df = spark.createDataFrame([
    (1, "John Doe", "johndoe@example.com", "+123456789", "Acme Corp", "Engineer", "123 Elm St", "Springfield", "IL", "USA", "Technology", "Alice Johnson", "2023-05-20", "2023-10-04", "Purchase1", "Notes1", "12345"),
    # Edge case: max length of string
    (2, "A"*255, "verylongemailaddress@example.com", "+9999999999", "Very Long Company Name", "Chief Very Long Title Officer", "999 Long Street Name"*5, "Long City Name"*5, "LONGSTATE", "United States of America", "Very Long Industry Name", "Longest Manager Name"*3, "2023-01-01", "2023-12-31", "Long purchase history"*10, "Long notes description"*10, "99999"),
    # Error case: invalid email format
    (3, "Jane Doe", "janedoeatexampledotcom", "+987654321", "Tech Inc", "Manager", "456 Oak Rd", "Metropolis", "CA", "USA", "Finance", "Bob Smith", "2023-09-17", "2023-10-04", "Purchase2", "Notes2", "54321"),
    # Null handling
    (4, None, None, None, "Null Co", "None When Available", None, "NoCity", "NA", "None Country", "Null Values Industry", None, None, None, "", "", None)
], customer_360_df.schema)

# Special characters and multi-byte characters
special_char_df = spark.createDataFrame([
    (5, "Søren Kierkegård", "søren@example.com", "+112358132134", "Philosophy Org", "Philosopher", "Philosophy Blvd 42", "Copenhagen", "The Capital", "Denmark", "Philosophy", "Professor Søren", "2023-06-15", "2023-10-04", "Book Purchases", "Complex notes with special characters #!@#$%", "40000"),
    (6, "山田太郎", "yamada@example.com", "+819012345678", "Japan Inc.", "技術者", "東京1番地", "東京", "東京", "日本", "IT業界", "長谷川一郎", "2021-07-08", "2023-10-04", "日本国内購入", "日本語メモと特殊文字", "1234567")
], customer_360_df.schema)

# Append to customer_360_raw_clone with diverse test records
customer_360_test_data_df = customer_360_clone_df.unionAll(test_df).unionAll(special_char_df)
customer_360_test_data_df.write.mode("overwrite").saveAsTable("purgo_playground.customer_360_raw_clone")

