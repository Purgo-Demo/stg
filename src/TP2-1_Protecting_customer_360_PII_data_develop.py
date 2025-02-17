# Required Libraries
from pyspark.sql.functions import col, udf, current_timestamp
from pyspark.sql.types import StringType
from cryptography.fernet import Fernet
import json

# Drop the existing clone table if it exists
spark.sql("DROP TABLE IF EXISTS purgo_playground.customer_360_raw_clone")

# Create a replica of the customer_360_raw table in customer_360_raw_clone
spark.sql("""
CREATE TABLE purgo_playground.customer_360_raw_clone AS 
SELECT * FROM purgo_playground.customer_360_raw
""")

# Generate an encryption key using Fernet
encryption_key = Fernet.generate_key()
cipher = Fernet(encryption_key)

# Define a UDF for encrypting PII columns
@udf(returnType=StringType())
def encrypt(value):
    if value is not None:
        return cipher.encrypt(value.encode()).decode()
    return None

# Load data from customer_360_raw_clone
customer_360_df = spark.table("purgo_playground.customer_360_raw_clone")

# Encrypt the specified PII columns
encrypted_df = customer_360_df.withColumn("name", encrypt(col("name"))) \
    .withColumn("email", encrypt(col("email"))) \
    .withColumn("phone", encrypt(col("phone"))) \
    .withColumn("zip", encrypt(col("zip")))

# Overwrite the clone table with encrypted data
encrypted_df.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("purgo_playground.customer_360_raw_clone")

# Save the encryption key as a JSON file with a timestamp
# Correct the JSON file naming with a suitable method for current_datetime
json_datetime = spark.sql("SELECT CURRENT_TIMESTAMP").collect()[0][0].strftime("%Y%m%d%H%M%S")
json_file_name = f"encryption_key_{json_datetime}.json"
json_file_path = f"/Volumes/agilisium_playground/purgo_playground/de_dq/{json_file_name}"

encryption_key_data = {"encryption_key": encryption_key.decode()}
with open(json_file_path, 'w') as json_file:
    json.dump(encryption_key_data, json_file)
