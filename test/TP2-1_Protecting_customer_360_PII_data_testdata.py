from pyspark.sql import SparkSession
from pyspark.sql.functions import col, struct, lit, udf, current_timestamp
from pyspark.sql.types import StringType, LongType, StructType, StructField, DoubleType, DecimalType, ArrayType, BooleanType, TimestampType, DateType
import json
from pyspark.sql import functions as F
import datetime

# Create a Spark session
spark = SparkSession.builder.appName("Test Data Generation").getOrCreate()

# Defining schema for the customer_360_raw table
schema_customer_360_raw = StructType([
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

# Generate sample data for customer_360_raw table
data_customer_360_raw = [
    (1, "John Doe", "john.doe@example.com", "555-1234", "Doe Corp", "CEO", "123 Elm St", "Metropolis", "NY", "USA", "Technology", "Jane Smith", "2022-01-01", "2022-02-15", "Laptop,Phone", "Need follow-up", "10001"),
    (2, "Jane Roe", "jane.roe@example.com", "555-5678", "Roe Ltd", "CTO", "456 Oak St", "Gotham", "CA", "USA", "Finance", "Matt Brown", "2022-05-10", "2023-03-12", "Tablet", "Satisfied customer", "90210"),
    # Edge case with special characters
    (3, "Álvaro Núñez", "álvaro.nunez@ejemplo.com", "555-6789", "Núñez Solutions", "Engineer", "789 Pine St", "Star City", "TX", "USA", "Engineering", "Emily White", "2022-12-28", "2023-04-07", "Smartwatch", "Needs attention—special chars", "75432"),
    # Error case with out of range zip code
    (4, "John Smith", "john.smith@domain.org", "555-8765", "Smith Inc", "Manager", "321 Cedar St", "Smallville", "WA", "USA", "Retail", "Chris Black", "2023-01-17", "2023-05-30", "Monitor", "Edge of handling", "-1234")
]

# Create DataFrame for customer_360_raw
df_customer_360_raw = spark.createDataFrame(data_customer_360_raw, schema_customer_360_raw)

# Encryption setup
def simple_encrypt(value):
    return value[::-1] if value else None

encrypt_udf = udf(simple_encrypt, StringType())

# Drop table customer_360_raw_clone if exists
spark.sql("DROP TABLE IF EXISTS purgo_playground.customer_360_raw_clone")

# Define a new schema for the clone including the `is_churn` column
schema_customer_360_raw_clone = schema_customer_360_raw.add(StructField("is_churn", LongType(), True))

# Create replica DataFrame adding the `is_churn` column with default value
df_customer_360_raw_clone = df_customer_360_raw.withColumn("is_churn", lit(None).cast(LongType()))

# Apply encryption to PII columns
pii_columns = ["name", "email", "phone", "zip"]
for column in pii_columns:
    df_customer_360_raw_clone = df_customer_360_raw_clone.withColumn(column, encrypt_udf(col(column)))

# Write the replica DataFrame as a table
df_customer_360_raw_clone.write.mode("overwrite").saveAsTable("purgo_playground.customer_360_raw_clone")

# Generate encryption key and save it
encryption_key = {"key": "sample_encryption_key"}
current_time = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
key_file_path = f"/Volumes/agilisium_playground/purgo_playground/de_dq/encryption_key_{current_time}.json"

with open(key_file_path, "w") as key_file:
    json.dump(encryption_key, key_file)

# Sample test data verification
print("Data Generation and Encryption Completed for Table: customer_360_raw_clone")
df_customer_360_raw_clone.show()

