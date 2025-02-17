from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, udf
from pyspark.sql.types import StringType
from pyspark.sql.functions import current_timestamp
import json
import datetime

# Initialize Spark session
spark = SparkSession.builder.appName("Databricks Test Data Generation").getOrCreate()

# Test DataFrame schema
schema = """
    id BIGINT,
    name STRING,
    email STRING,
    phone STRING,
    company STRING,
    job_title STRING,
    address STRING,
    city STRING,
    state STRING,
    country STRING,
    industry STRING,
    account_manager STRING,
    creation_date DATE,
    last_interaction_date DATE,
    purchase_history STRING,
    notes STRING,
    zip STRING
"""

# Create test data including different scenarios such as NULLs, edge cases, etc.
data = [
    (1, "Alice Smith", "alice@example.com", "1234567890", "ABC Corp", "Manager", "123 Elm St", "Metropolis", "NY", "USA", "Finance", "John Doe", "2024-01-01", "2024-02-28", "None", "First purchase in 2024", "10001"),
    (2, "Bob Johnson", "bob.johnson@example.com", "0987654321", None, "Director", None, "Gotham", "NJ", "USA", "IT", "Jane Doe", None, "2024-03-01", "Repeat customer", "Important client", "07001"),
    (3, "Carlos López", "calos.lopez@example.com", "+6123456789", "XYZ Ltd", "CEO", "789 Pine St", "Star City", "CA", "USA", "Marketing", "Mary Jane", "2023-12-31", "2024-04-01", "Frequent buyer", None, "90001"),
    (4, "李华", "lihua@example.cn", "+861234567890", "Huawei", "Tech Lead", "456 Bamboo Ave", "Beijing", None, "China", "Telecom", "Ming Zhao", "2022-01-01", "2024-03-15", "Multiple transactions", "Long-time customer with important history", "100000"),
    (5, None, "invalid-email", "not-a-phone", "Unknown", "Unknown", "Unknown", "Unknown", "Unknown", "Unknown", "Unknown", "Unknown", "2024-03-21", "2024-03-22", "Corrupted data", "Data issues noted", None),
    (6, "Eve Adams", "eve.adams@example.org", "9998887777", "Corp Inc", "CFO", "101 Maple Dr", None, "TX", "USA", "Healthcare", "Tom White", "2024-02-29", "2024-03-31", None, "Note about Eve Adams", "75001"),
    (7, "François Dubois", "francois@exemple.fr", "+33123456789", "Café de Paris", "Owner", "333 Champs Elysees", "Paris", None, "France", "Hospitality", "Pierre Dubois", "2024-01-15", None, None, "Loyal customer from France", "75000")
]

# Create DataFrame
df = spark.createDataFrame(data, schema=schema)

# Define a simple encryption placeholder function. Replace this with appropriate encryption logic.
def encrypt_pii(value: str) -> str:
    if value:
        return f"enc({value})"
    return value

# Register UDF
encrypt_udf = udf(encrypt_pii, StringType())

# Encrypt PII columns
encrypted_df = df.withColumn("name", encrypt_udf(col("name"))) \
    .withColumn("email", encrypt_udf(col("email"))) \
    .withColumn("phone", encrypt_udf(col("phone"))) \
    .withColumn("zip", encrypt_udf(col("zip")))

# Show the encrypted_df for validation
encrypted_df.show(truncate=False)

# Save DataFrame as Delta table
destination_table = "purgo_playground.customer_360_raw_clone"
encrypted_df.write.format("delta").mode("overwrite").saveAsTable(destination_table)

# Save the encryption key to JSON file
encryption_key = {
    "name_key": "key_for_name",
    "email_key": "key_for_email",
    "phone_key": "key_for_phone",
    "zip_key": "key_for_zip"
}
current_datetime = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
key_filename = f"/Volumes/agilisium_playground/purgo_playground/de_dq/encryption_key_{current_datetime}.json"

# Save encryption key file
with open(key_filename, "w") as f:
    json.dump(encryption_key, f)
