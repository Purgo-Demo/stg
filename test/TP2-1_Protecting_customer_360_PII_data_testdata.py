from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType, DecimalType, ArrayType, MapType, BooleanType
import random
from datetime import datetime

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Databricks Test Data Generation") \
    .getOrCreate()

# Define schemas for various tables
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

# Generate test data records
customer_360_raw_data = [
    {
        "id": i,
        "name": f"Customer {i}",
        "email": f"customer{i}@example.com",
        "phone": f"+1234567890{i%10}",
        "company": f"Company {i}",
        "job_title": "Engineer",
        "address": "123 Main St",
        "city": "Metropolis",
        "state": "CA",
        "country": "USA",
        "industry": "Technology",
        "account_manager": f"Manager {i}",
        "creation_date": "2025-03-21T00:00:00.000+0000",
        "last_interaction_date": "2025-03-22T00:00:00.000+0000",
        "purchase_history": "Item A, Item B",
        "notes": "Important customer",
        "zip": "12345"
    } for i in range(20)
]

# Create DataFrame
customer_360_raw_df = spark.createDataFrame(data=customer_360_raw_data, schema=customer_360_raw_schema)

# Display the DataFrame
customer_360_raw_df.show()

# Encrypt PII columns
def encrypt_col(df, col_name):
    return df.withColumn(col_name, F.sha2(F.col(col_name), 256))

pii_columns = ['name', 'email', 'phone', 'zip']
for col in pii_columns:
    customer_360_raw_df = encrypt_col(customer_360_raw_df, col)

# Save the encryption key (Dummy Key for simplicity, should use a secure method in production)
encryption_key = {"key": "dummy_encryption_key"}
encryption_key_path = f"/Volumes/agilisium_playground/purgo_playground/de_dq/encryption_key_{datetime.now().strftime('%Y%m%d%H%M%S')}.json"

with open(encryption_key_path, 'w') as key_file:
    key_file.write(f"{encryption_key}")

print(f"Encryption Key saved to: {encryption_key_path}")

# Load data to the clone table
customer_360_raw_clone_table = "purgo_playground.customer_360_raw_clone"

# Drop if exists and then overwrite with new data
spark.sql(f"DROP TABLE IF EXISTS {customer_360_raw_clone_table}")
customer_360_raw_df.write.mode("overwrite").saveAsTable(customer_360_raw_clone_table)

# Confirm data has been loaded
loaded_df = spark.table(customer_360_raw_clone_table)
loaded_df.show()

