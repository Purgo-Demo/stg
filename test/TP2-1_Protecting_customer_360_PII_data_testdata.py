from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, sha2, concat, current_timestamp, date_format
from pyspark.sql.types import StructType, StructField, StringType, LongType, DateType

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Databricks Sample Data Generation") \
    .getOrCreate()

# Schema for customer_360_raw_clone table
schema = StructType([
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
    StructField("creation_date", DateType(), True),
    StructField("last_interaction_date", DateType(), True),
    StructField("purchase_history", StringType(), True),
    StructField("notes", StringType(), True),
    StructField("zip", StringType(), True),
    StructField("is_churn", LongType(), True)
])

# Sample test data for customer_360_raw_clone
testData = [
    (1, "John Doe", "john.doe@example.com", "123-456-7890", "ACME Corp", "Engineer", "123 Elm St", "Metropolis", "WA", "USA", "Manufacturing", "Jane Smith", "2022-03-21", "2023-03-21", "ItemA, ItemB", "Important client", "12345", 0),
    (2, "Alice Lee", "alice.lee@example.com", "123-456-7890", "Wayne Enterprises", "Analyst", "456 Oak Ave", "Gotham", "NY", "USA", "Finance", "Clark Kent", "2021-05-14", "2022-12-20", "ItemC", "Needs follow-up", "54321", 1),
    # Edge case: NULL values
    (3, None, None, None, "Initech", "Consultant", "789 Maple Rd", "Springfield", "IL", "USA", None, "Bruce Wayne", None, None, None, None, None, None),
    # Special characters in strings
    (4, "名字", "名字@例子.com", "+1-555-1234", "Sa-Sa-Sü", "💻 Developer", "😀 St", "例子市", "CA", "USA", "Technology", "Руководитель", "2020-01-01", "2021-01-01", "😊 Product", "🏆 Important", "!@#$%", 0),
    # Error case: Invalid date
    (5, "Error Record", "error@invalid.com", "999-999-9999", "Nowhere", "Ghost", "Nonexistent Ave", "Void", "NA", "", "Unknown", "Nobody", "3000-13-01", "4000-13-01", "Nothing", "No notes", "00", 0)
]

# Create DataFrame
df = spark.createDataFrame(data=testData, schema=schema)

# Encrypt PII columns using SHA-256 (for demonstration purposes)
encrypted_df = df.withColumn("name", sha2(col("name"), 256)) \
    .withColumn("email", sha2(col("email"), 256)) \
    .withColumn("phone", sha2(col("phone"), 256)) \
    .withColumn("zip", sha2(col("zip"), 256))

# Save encryption key (for example, using a static key here)
encryption_key = {"key": "static_dummy_key"}
key_file_path = f"/Volumes/agilisium_playground/purgo_playground/de_dq/encryption_key_{date_format(current_timestamp(), 'yyyyMMdd_HHmmss')}.json"

# Save the encryption key
import json
dbutils.fs.put(key_file_path, json.dumps(encryption_key))

# Write encrypted data back to clone table
# Assume the clone table has been created and replace it with new data
encrypted_df.write.format("delta").mode("overwrite").saveAsTable("purgo_playground.customer_360_raw_clone")

# Show resulting DataFrame
encrypted_df.show(truncate=False)
