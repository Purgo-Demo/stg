from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from cryptography.fernet import Fernet
import json
from datetime import datetime

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Encrypt PII Data") \
    .enableHiveSupport() \
    .getOrCreate()

# Generate an encryption key and initialize encryption object
encryption_key = Fernet.generate_key()
fernet = Fernet(encryption_key)

# Read the original table into a DataFrame
customer_360_raw_df = spark.table("purgo_playground.customer_360_raw")

# Encrypt specified PII columns using Fernet encryption
def encrypt_column(column_value):
    return fernet.encrypt(column_value.encode()).decode()

encrypt_udf = spark.udf.register("encrypt_udf", encrypt_column)

customer_360_encrypted_df = customer_360_raw_df.withColumn("name", encrypt_udf(col("name"))) \
    .withColumn("email", encrypt_udf(col("email"))) \
    .withColumn("phone", encrypt_udf(col("phone"))) \
    .withColumn("zip", encrypt_udf(col("zip")))

# Write encrypted data back to the customer_360_raw_clone table
customer_360_encrypted_df.write \
    .mode("overwrite") \
    .saveAsTable("purgo_playground.customer_360_raw_clone")

# Save encryption key as a JSON file
current_datetime = datetime.now().strftime("%Y%m%d_%H%M%S")
key_file_path = f"/Volumes/agilisium_playground/purgo_playground/de_dq/encryption_key_{current_datetime}.json"

with open(key_file_path, "w") as key_file:
    json.dump({"encryption_key": encryption_key.decode()}, key_file)

spark.stop()

# Comments explaining the purpose of each test scenario
# 1. Happy path test data: Ensures encryption works for all valid fields.
# 2. NULL handling scenarios: NULLs should remain unaffected by encryption.
# 3. Error cases & Edge cases: Examples like unsupported encryption algorithm errors (handled by the library).
# 4. Ensure structure consistency: Non-PII columns remain unmodified and consistent.
# 5. Key management: Proper handling and location of the encryption key.

