from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

# Create Spark session
spark = SparkSession.builder \
    .appName("DatabricksTestDataGeneration") \
    .getOrCreate()

# Define the database
database = "purgo_playground"

# Define table names
original_table = f"{database}.d_product_revenue"
clone_table = f"{database}.d_product_revenue_clone"

# Drop the clone table if it exists
spark.sql(f"DROP TABLE IF EXISTS {clone_table}")

# Create a clone of the original table
spark.sql(f"CREATE TABLE {clone_table} AS SELECT * FROM {original_table}")

# Load the cloned table into a DataFrame for processing
df_clone = spark.table(clone_table)

# Mask the last four digits of the invoice_number column
df_masked = df_clone.withColumn("invoice_number", expr("CONCAT(SUBSTRING(invoice_number, 1, LENGTH(invoice_number)-4), '****')"))

# Overwrite the cloned table with masked data
df_masked.write.mode("overwrite").saveAsTable(clone_table)

# Display the result for verification
df_result = spark.table(clone_table)
df_result.show()

# Stop the Spark session
spark.stop()
