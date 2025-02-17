from pyspark.sql.functions import col, lit, when, current_timestamp
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, DateType
import random
import json
import datetime

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Databricks Test Data Generation") \
    .getOrCreate()

# Schema definition for customer_360_raw_clone
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
    StructField("is_churn", LongType(), True),
])

# Sample test data generation
data = [
    (1, "John Doe", "john.doe@example.com", "+1234567890", "Example Inc", "Manager", "123 Elm St", "Ann Arbor", "MI", "USA", "Tech", "Rachel Smith", "2024-01-01", "2024-04-01", "Purchase 1, Purchase 2", "First note", "12345", 0), # Happy path
    (2, "Jane Doe", "jane.doe@example.com", "+0987654321", "", "Developer", "456 Oak St", "Chicago", "IL", "USA", "Fintech", "Robert Brown", "2023-12-31", "2024-01-15", "Purchase A, Purchase B", "Second note", "54321", 1), # Edge case, empty company name
    (3, "Invalid Date", "invalid.date@example.com", "+1122334455", "Invalid Corp", "Tester", "789 Pine St", "Seattle", "WA", "USA", "Retail", "Tina White", "2024-02-31", "2024-04-32", "Purchase X", "Invalid date note", "99999", -1), # Error case, invalid dates
    (None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None), # NULL handling
    (4, "Special Char 😊", "special.char@example.com", "+0000000000", "Unicode Co", "Designer", "Special St", "Tokyo", "TY", "Japan", "Design", "Kim Lee", "2024-03-21", "2024-04-20", "Purchase Y", "Special char note", "56789", 1) # Special character
]

# Create DataFrame
df_test_data = spark.createDataFrame(data, schema)

# Display DataFrame
df_test_data.show(truncate=False)

# Encryption simulation - simple mapping function (actual encryption logic to replace with a secure method)
def encrypt(value, key):
    if value is None:
        return None
    # Simple encryption simulation by reversing string and appending a keyword
    encrypted_value = value[::-1] + key
    return encrypted_value

# Generate encryption key (for illustration purposes)
encryption_key = f"key_{random.randint(1000, 9999)}"

# Encrypt PII columns
encrypted_df = df_test_data.withColumn("name", when(col("name").isNotNull(), lit(encrypt(col("name").cast("string"), encryption_key))).otherwise(None)) \
    .withColumn("email", when(col("email").isNotNull(), lit(encrypt(col("email").cast("string"), encryption_key))).otherwise(None)) \
    .withColumn("phone", when(col("phone").isNotNull(), lit(encrypt(col("phone").cast("string"), encryption_key))).otherwise(None)) \
    .withColumn("zip", when(col("zip").isNotNull(), lit(encrypt(col("zip").cast("string"), encryption_key))).otherwise(None))

# Display encrypted DataFrame
encrypted_df.show(truncate=False)

# Save encryption key as JSON
encryption_key_location = f"/Volumes/agilisium_playground/purgo_playground/de_dq/encryption_key_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
key_data = {"encryption_key": encryption_key}
with open(encryption_key_location, "w") as json_file:
    json.dump(key_data, json_file)

# Checkpoint: data integrity verification
# For this minimal example, assume encrypted_df should retain all non-PII data unchanged

# Stop Spark session
spark.stop()

