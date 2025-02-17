from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, DateType
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, udf
from pyspark.sql.types import StringType, LongType, IntegerType, DateType
import datetime
import json

# Initialize Spark Session
spark = SparkSession.builder.appName("Test Data Generation").getOrCreate()

# Define schema for customer_360_raw_clone as per the requirements
schema = StructType([
    StructField("id", LongType(), True),
    StructField("name", StringType(), True),          # PII column
    StructField("email", StringType(), True),         # PII column
    StructField("phone", StringType(), True),         # PII column
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
    StructField("zip", StringType(), True),           # PII column
    StructField("is_churn", IntegerType(), True)
])

# Sample test data with various scenarios
data = [
    (1, "John Doe", "john.doe@example.com", "1234567890", "Acme Corp", "Software Engineer", "123 Elm Street", "Anytown", "CA", "USA", "Technology", "Alice Smith", datetime.date(2023, 3, 14), datetime.date(2023, 9, 22), "Purchase A", "Notes A", "90210", 0),
    (2, "Jane Smith", "jane.smith@example.com", "0987654321", "Beta Inc", "Data Analyst", "456 Oak Avenue", "Othertown", "NY", "USA", "Finance", "Bob Johnson", datetime.date(2023, 4, 20), datetime.date(2023, 10, 5), "Purchase B", "Notes B", "10001", 1),
    # Edge cases
    (3, None, None, None, "Gamma LLC", "Manager", "789 Pine Road", "Metropolis", "IL", "USA", "Retail", "Charlie Brown", datetime.date(2023, 1, 1), datetime.date(2023, 1, 1), "Purchase C", "Notes C", "60601", 0),
    # Special characters and multi-byte characters
    (4, "Renée O'Connor", "renée.oconnor@example.com", "+1-800-555-5555", "Delta & Co", "Consultant", "101 Maple Street", "New Town", "WA", "USA", "Consulting", "David Lee", datetime.date(2023, 2, 29), datetime.date(2023, 8, 30), "Purchase D", "Notes D", "98101", 1),
    # Invalid Email and Phone cases (happy path with error scenarios)
    (5, "Alex White", "invalid-email", "invalid-phone", "Epsilon Partners", "CEO", "202 Birch Street", "Old City", "TX", "USA", "Real Estate", "Eve Miller", datetime.date(2023, 7, 4), datetime.date(2023, 8, 15), "Purchase E", "Notes E", "75001", 0)
]

# Create DataFrame
df = spark.createDataFrame(data, schema)

# Define a dummy encryption function for demonstration purposes
def encrypt(value):
    if value is None:
        return None
    return f"encrypted_{value}"

# Register UDFs for encryption
encrypt_udf = udf(encrypt, StringType())

# Apply encryption to PII columns
pii_columns = ["name", "email", "phone", "zip"]
for column in pii_columns:
    df = df.withColumn(column, encrypt_udf(col(column)))

# Save encrypted data into customer_360_raw_clone
df.write.format("delta").mode("overwrite").saveAsTable("purgo_playground.customer_360_raw_clone")

# Generate and save encryption key as JSON
encryption_key = {"key": "example_key_for_encryption"}
current_datetime = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
key_path = f"/Volumes/agilisium_playground/purgo_playground/de_dq/encryption_key_{current_datetime}.json"

# Save encryption key to JSON file
with open(key_path, 'w') as json_file:
    json.dump(encryption_key, json_file)


