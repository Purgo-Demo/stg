from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, BigIntType, BooleanType, DateType
import json
import os

# Initialize Spark session
spark = SparkSession.builder \
    .appName("TestDataGeneration") \
    .getOrCreate()

# Define the schema for customer_360_raw
customer_360_raw_schema = StructType([
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

# Generate valid test data - happy path
data_valid = [
    (1, "John Doe", "john.doe@example.com", "1234567890", "ABC Inc.", "Manager",
     "123 Main St", "Metropolis", "StateOne", "CountryA", "Tech", "Alice Johnson",
     "2021-01-01", "2023-01-01", "[{\"purchase\":\"item1\",\"qty\":10}]", "Regular customer.", "12345"),
    # Add 29 more valid records with varying data
]

# Generate edge case test data - boundary conditions
data_edge_cases = [
    (2, "", "", "", "", "", "", "", "", "", "", "", None, None, "", "", ""),
    (3, "A" * 256, "a@b.com", "+12345", "X" * 300, "Title" * 50, "Addr" * 50, 
     "City" * 50, "State" * 50, "Country" * 50, "Industry" * 50, "Mgr" * 50,
     "2024-12-31", "2024-12-31", "", "", "99999"), # Boundary data
    # Add additional edge cases as needed
]

# Generate error case test data - invalid data combinations
data_error_cases = [
    (4, "Invalid Email", "not an email", "NotAPhoneNumber", "InvalidCo", "NotATitle",
     "Addr", "City", "State", "Country", "Industry", "Mgr",
     "2021-01-01", "2023-01-01", "[{\"purchase\":\"invalid\"}]", "Error note", "InvalidZip"),
    # Add additional error cases as needed
]

# Generate cases with NULL values
data_null_cases = [
    (5, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None),
    # More null cases as needed
]

# Special char and multi-byte char test data
data_special_chars = [
    (6, "Sp3ci@l Chåråctèrs", "speci@l@example.com", "+1234@567", "Müller & Co", "Dîrectør",
     "50 St. John’s", "São Paulo", "Ståte", "Cøuntry", "Indüstêrý", "Månager",
     "2021-01-01", "2023-01-01", "", "Notes with special chars: © ™ ∆", "ABCDE"),
    # Additional special character records
]

# Combine all test data
all_test_data = data_valid + data_edge_cases + data_error_cases + data_null_cases + data_special_chars

# Create DataFrame with all test records
df_test_data = spark.createDataFrame(data=all_test_data, schema=customer_360_raw_schema)

# Write the test data to a target table
df_test_data.write.mode("overwrite").format("delta").saveAsTable("purgo_playground.customer_360_raw_clone")

# Encryption Example - Install cryptography if not already installed
# %pip install cryptography
from cryptography.fernet import Fernet

def encrypt_column(col_name):
    key = Fernet.generate_key()
    fernet = Fernet(key)
    return col(col_name).cast(StringType()), key

# Apply encryption on PII columns
keys = {}
for pii_column in ["name", "email", "phone", "zip"]:
    df_test_data = df_test_data.withColumn(pii_column, encrypt_column(pii_column)[0])
    keys[pii_column] = encrypt_column(pii_column)[1].decode()

# Save encryption keys
current_time = current_timestamp().cast("string").substr(0, 19).replace(":", ".")
json_location = f"/Volumes/agilisium_playground/purgo_playground/de_dq/encryption_key_{current_time}.json"

with open(json_location, 'w') as key_file:
    json.dump(keys, key_file)

