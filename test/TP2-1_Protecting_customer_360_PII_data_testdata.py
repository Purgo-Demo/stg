from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, concat, current_timestamp, sha2
from datetime import datetime
import json

# Initialize Spark session
spark = SparkSession.builder.appName("Encrypt PII Data").getOrCreate()

# Define encryption function
def encrypt_column(df, column_name, key):
    return df.withColumn(column_name, sha2(concat(col(column_name), lit(key)), 256))

# Load data from customer_360_raw
customer_raw_df = spark.table("purgo_playground.customer_360_raw")

# Key for pseudo encryption (to be replaced with actual encryption logic)
encryption_key = "sample_encryption_key"

# Encrypt PII columns
encrypted_df = customer_raw_df \
    .transform(lambda df: encrypt_column(df, 'name', encryption_key)) \
    .transform(lambda df: encrypt_column(df, 'email', encryption_key)) \
    .transform(lambda df: encrypt_column(df, 'phone', encryption_key)) \
    .transform(lambda df: encrypt_column(df, 'zip', encryption_key))

# Save encrypted data to cloned table
encrypted_df.write.mode("overwrite").saveAsTable("purgo_playground.customer_360_raw_clone")

# Save encryption key as JSON
encryption_key_path = f"/Volumes/agilisium_playground/purgo_playground/de_dq/encryption_key_{datetime.now().strftime('%Y%m%d%H%M%S')}.json"
with open(encryption_key_path, 'w') as file:
    json.dump({"encryption_key": encryption_key}, file)

# Happy path test data
happy_path_df = spark.createDataFrame([
    (1, "John Doe", "john.doe@example.com", "+1234567890", "Tesla", "Engineer", "123 Main St", "Palo Alto", "CA", "USA", "Automotive", "Jane Smith", "2021-01-01", "2022-01-01", "History1", "Notes", "94301"),
    (2, "Jane Roe", "jane.roe@example.com", "+1987654321", "Google", "Manager", "456 Elm St", "Mountain View", "CA", "USA", "Technology", "John Doe", "2021-06-01", "2022-06-01", "History2", "Important Notes", "94043")
], schema=customer_raw_df.schema)

happy_path_df.write.mode("append").saveAsTable("purgo_playground.customer_360_raw_clone")

# Edge cases test data
edge_cases_df = spark.createDataFrame([
    (3, "", "", "", "Microsoft", "Exec", "789 Pine St", "Redmond", "WA", "USA", "Technology", "Mike Smith", "2020-01-01", "2023-01-01", "Edge History", "Edge Notes", ""),
    (4, "NULL 約", "NULL@null.com", "NULL", "Amazon", "Analyst", "NULL Ave", "Seattle", "WA", "USA", "E-commerce", "NULL", "2022-02-01", "2023-02-01", "Special Hist", "Special Notes", None)
], schema=customer_raw_df.schema)

edge_cases_df.write.mode("append").saveAsTable("purgo_playground.customer_360_raw_clone")

# Error cases test data
error_cases_df = spark.createDataFrame([
    (5, "Alice", "alice@oversized-email.com", "+12345678901234567890", "Oversize Co", "Leader", "Oversize Address", "Oversize City", "OS", "Overland", "Oversize Industry", "Oversize Manager", "2023-03-03", "2023-05-05", "Error Purchase", "Error Notes", "999999999999")
], schema=customer_raw_df.schema)

error_cases_df.write.mode("append").saveAsTable("purgo_playground.customer_360_raw_clone")

# NULL handling sample
null_handling_df = spark.createDataFrame([
    (6, None, None, None, "Company XYZ", "Consultant", None, None, None, None, None, None, None, "2024-03-21", "Null Purchase History", None, None)
], schema=customer_raw_df.schema)

null_handling_df.write.mode("append").saveAsTable("purgo_playground.customer_360_raw_clone")

# Special characters and multi-byte characters
special_chars_df = spark.createDataFrame([
    (7, "Félix", "fè-lix@example.cz", "+õ12-345", "Pâtissèrie", "Chèf", "Rüê Bøúlëvård", "Päriš", "ÎÄ", "France", "Confectionary", "Ólê", "2023-10-10", "2023-12-12", "Special Histories", "Notes with accents", "75000")
], schema=customer_raw_df.schema)

special_chars_df.write.mode("append").saveAsTable("purgo_playground.customer_360_raw_clone")

# Stop Spark session
spark.stop()
