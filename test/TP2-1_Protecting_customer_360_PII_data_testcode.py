from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, lit, current_timestamp
from pyspark.sql.types import StringType
import json
import datetime

# Initialize Spark session
spark = SparkSession.builder.appName("Databricks PII Encryption Tests").getOrCreate()

# Encrypt function placeholder
def encrypt_pii(value: str) -> str:
    if value:
        return f"enc({value})"  # Placeholder encryption logic
    return value

# Register the UDF
encrypt_udf = udf(encrypt_pii, StringType())

# Load customer_360_raw table
customer_df = spark.sql("SELECT * FROM purgo_playground.customer_360_raw")

# Drop the clone table if it exists
spark.sql("DROP TABLE IF EXISTS purgo_playground.customer_360_raw_clone")

# Create the clone table and encrypt PII data
customer_df.withColumn("name", encrypt_udf(col("name"))) \
    .withColumn("email", encrypt_udf(col("email"))) \
    .withColumn("phone", encrypt_udf(col("phone"))) \
    .withColumn("zip", encrypt_udf(col("zip"))) \
    .write.format("delta").saveAsTable("purgo_playground.customer_360_raw_clone")

# Save the encryption key to a JSON file in specified volume
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

# DataType and NULL handling tests
def test_data_types_and_nulls(df):
    assert df.schema["name"].dataType == StringType(), "Name column should be STRING"
    assert df.schema["email"].dataType == StringType(), "Email column should be STRING"
    assert df.filter(col("name").isNull()).count() == 0, "Name column should not be NULL after encryption"
    # Add more assertions as required
    print("Data type and NULL handling tests passed.")

test_data_types_and_nulls(customer_df)

# Delta Lake operations tests
def test_delta_operations():
    spark.sql("MERGE INTO purgo_playground.customer_360_raw_clone USING purgo_playground.customer_360_raw ON purgo_playground.customer_360_raw_clone.id = purgo_playground.customer_360_raw.id WHEN MATCHED THEN DELETE")
    assert spark.sql("SELECT COUNT(*) FROM purgo_playground.customer_360_raw_clone").collect()[0][0] == 0, "Delta MERGE operation failed to DELETE records"
    print("Delta Lake MERGE test passed.")

test_delta_operations()

# Cleanup operations
def cleanup_operations():
    spark.sql("DROP TABLE IF EXISTS purgo_playground.customer_360_raw_clone")
    import os
    if os.path.exists(key_filename):
        os.remove(key_filename)
    print("Cleanup operations completed.")

cleanup_operations()

