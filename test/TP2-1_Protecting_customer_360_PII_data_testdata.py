from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, encode, decode
from pyspark.sql.types import StructType, StructField, StringType, LongType, DateType
import json
from datetime import datetime

# Initialize Spark session
spark = SparkSession.builder.appName("PII Encryption").getOrCreate()

# Define schema for the test data
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
    StructField("zip", StringType(), True)
])

# Generate test data
data = [
    (1, "Alice Smith", "alice@example.com", "1234567890", "Company A", "Engineer", "123 St", "City A", "State A", "Country A", "Tech", "Manager A", None, None, "Purchase A", "Notes A", "12345"),
    (2, "Bob Johnson", "bob@example.com", "0987654321", "Company B", "Manager", "456 St", "City B", "State B", "Country B", "Finance", "Manager B", None, None, "Purchase B", "Notes B", "54321"),
    # Edge Cases
    (3, "", "invalidemail", "notaphone", "", "", "", "", "", "", "", "", None, None, "", "", ""),
    (4, "Bob 'The Builder™'", "unicode@example.com", "555-1234", "Company C", "Builder", "789 St", "City C", "State C", "Country C", "Construction", "Manager C", None, None, "Purchase C", "Special Characters: !@#$%^&*()", None),
    # Special character and NULL handling
    (5, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None)
]

# Create DataFrame
df = spark.createDataFrame(data, schema)

# Encrypt PII columns
encryption_key = "secret_key"  # This could be any key or generated securely
encrypted_df = df.select(
    col("id"),
    encode(col("name"), encryption_key).alias("name"),
    encode(col("email"), encryption_key).alias("email"),
    encode(col("phone"), encryption_key).alias("phone"),
    col("company"),
    col("job_title"),
    col("address"),
    col("city"),
    col("state"),
    col("country"),
    col("industry"),
    col("account_manager"),
    col("creation_date"),
    col("last_interaction_date"),
    col("purchase_history"),
    col("notes"),
    encode(col("zip"), encryption_key).alias("zip")
)

# Write the test data to the destination table
encrypted_df.write.mode("overwrite").saveAsTable("purgo_playground.customer_360_raw_clone")

# Save the encryption key to JSON file
encryption_key_metadata = {
    "encryption_key": encryption_key,
    "timestamp": datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f+0000")
}
with open(f"/Volumes/agilisium_playground/purgo_playground/de_dq/encryption_key_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json", 'w') as json_file:
    json.dump(encryption_key_metadata, json_file)
