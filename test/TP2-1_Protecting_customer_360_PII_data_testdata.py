# Required Libraries
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit, current_timestamp, sha2, concat
import json

# Initialize Spark Session
spark = SparkSession.builder.appName("Encrypt PII Data").getOrCreate()

# Prerequisite: Drop old cloned table, if exists
spark.sql("DROP TABLE IF EXISTS purgo_playground.customer_360_raw_clone")

# Create a replica of the existing table
spark.sql("""
CREATE TABLE purgo_playground.customer_360_raw_clone
USING DELTA
AS SELECT *, NULL AS is_churn FROM purgo_playground.customer_360_raw
""")

# Function to encrypt PII columns
def encrypt_dataframe(df: DataFrame, columns: list, encryption_key: str) -> DataFrame:
    for col_name in columns:
        # Using SHA-256 for encryption; adapt based on actual requirements
        df = df.withColumn(col_name, sha2(concat(col(col_name), lit(encryption_key)), 256))
    return df

# Define PII Columns and generate encryption key
pii_columns = ["name", "email", "phone", "zip"]
encryption_key = "test_encryption_key"  # Dummy key for testing, generate using secure methods for production

# Load customer data
customer_df = spark.table("purgo_playground.customer_360_raw_clone")

# Encrypt PII data
encrypted_df = encrypt_dataframe(customer_df, pii_columns, encryption_key)

# Write encrypted data back to the clone table
encrypted_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("purgo_playground.customer_360_raw_clone")

# Save encryption key to JSON file
key_file_path = f"/Volumes/agilisium_playground/purgo_playground/de_dq/encryption_key_{current_timestamp()}.json"
with open(key_file_path, 'w') as key_file:
    json.dump({"encryption_key": encryption_key}, key_file)

# Test Data Generation Code
# Schema definition for test data
schema = """
id BIGINT, name STRING, email STRING, phone STRING, company STRING, job_title STRING,
address STRING, city STRING, state STRING, country STRING, industry STRING, account_manager STRING,
creation_date TIMESTAMP, last_interaction_date TIMESTAMP, purchase_history STRING, notes STRING,
zip STRING, is_churn INT
"""

# Happy path test data
happy_path_data = [
    (1, "John Doe", "john@example.com", "1234567890", "ExampleCorp", "Developer",
     "123 Elm St", "Springfield", "IL", "USA", "Technology", "Alice Johnson",
     "2023-01-15T00:00:00.000+0000", "2023-08-10T00:00:00.000+0000", "Laptop", "VIP Client", "62704", 0),
    (2, "Jane Smith", "jane@example.com", "0987654321", "Innovatech", "Manager",
     "456 Oak St", "Greenwich", "CT", "USA", "Finance", "Bob Brown",
     "2022-05-20T00:00:00.000+0000", "2023-02-19T00:00:00.000+0000", "Software", "Regular meetings", "06830", 1)
]

# Add more categories of test data as needed
# ...

# Create DataFrame and Write to Table
test_data_df = spark.createDataFrame(happy_path_data, schema=schema)
test_data_df.write.format("delta").mode("append").saveAsTable("purgo_playground.customer_360_raw_clone")

