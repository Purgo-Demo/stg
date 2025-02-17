from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType
import json
import datetime

# Setup configuration
# Required libraries are part of PySpark, so no additional installation is needed

# Initialize Spark session
spark = SparkSession.builder \
    .appName("PII Data Encryption Tests") \
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

# Simulate test data for customer_360_raw
customer_360_raw_data = [
    (1, "John Doe", "john.doe@example.com", "123-456-7890", "John's Company", "CEO", "123 Elm St", "Gotham", "NY", "USA", "Retail", "Jane Manager", 
     datetime.datetime(2023, 1, 1), datetime.datetime(2023, 1, 15), "TV, Laptop", "Valued", "98765")
    # Add more test cases as needed
]

# Create DataFrame from the test data
customer_360_raw_df = spark.createDataFrame(data=customer_360_raw_data, schema=customer_360_raw_schema)

# Encrypt columns
def encrypt_cols(df, cols):
    for col in cols:
        df = df.withColumn(col, F.sha2(F.col(col), 256))
    return df

# Function to save encryption keys (for testing, use a mock key)
def save_encryption_key(directory, key_name):
    encryption_key = {"encryption_key": "1234567890abcdef1234567890abcdef"}
    key_path = f"{directory}/{key_name}.json"
    with open(key_path, 'w') as key_file:
        json.dump(encryption_key, key_file)
    return key_path

# Encryption and test procedures
pii_columns = ['name', 'email', 'phone', 'zip']
customer_360_encrypted_df = encrypt_cols(customer_360_raw_df, pii_columns)

# Save encryption key
key_directory = "/Volumes/agilisium_playground/purgo_playground/de_dq"
current_datetime = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
key_file_path = save_encryption_key(key_directory, f"encryption_key_{current_datetime}")

# Validate that the DataFrame has been encrypted correctly and saved
customer_360_encrypted_df.show(truncate=False)

# Validation can involve checking the format of the encrypted PII fields
encrypted_sample = customer_360_encrypted_df.select("name", "email", "phone", "zip").first()
assert encrypted_sample["name"] != "John Doe", "The name column was not encrypted properly."

# Verify the encryption key has been saved
print(f"Encryption Key Path: {key_file_path}")

# Test data loading functionality (mock)
def test_data_load():
    # Mock loading of data
    customer_360_encrypted_df.write.mode("overwrite").saveAsTable("purgo_playground.customer_360_raw_clone")
    clone_df = spark.table("purgo_playground.customer_360_raw_clone")
    
    # Check if the clone table is created and has the expected structure
    assert clone_df.count() == customer_360_encrypted_df.count(), "Data load to clone failed, row count mismatch."
    assert set(clone_df.columns) == set(customer_360_raw_df.columns), "Clone table schema mismatch."

    clone_df.show(truncate=False)

# Cleanup (make sure to drop test tables and temp views)
def cleanup():
    spark.sql("DROP TABLE IF EXISTS purgo_playground.customer_360_raw_clone")
    print("Test artifacts cleaned up.")

# Execute test functions
test_data_load()
cleanup()

