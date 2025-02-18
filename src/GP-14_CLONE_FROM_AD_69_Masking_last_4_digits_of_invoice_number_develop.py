from pyspark.sql.functions import expr

# Define database and table names
database = "purgo_playground"
original_table = f"{database}.d_product_revenue"
clone_table = f"{database}.d_product_revenue_clone"

# Drop the clone table if it exists
spark.sql(f"DROP TABLE IF EXISTS {clone_table}")

# Create a clone of the original table
spark.sql(f"CREATE TABLE {clone_table} AS SELECT * FROM {original_table}")

# Load the cloned table into a DataFrame for processing
df_clone = spark.table(clone_table)

# Mask the last 4 digits of invoice_number
df_masked = df_clone.withColumn("invoice_number", expr("CONCAT(SUBSTRING(invoice_number, 1, LENGTH(invoice_number)-4), '****')"))

# Overwrite the cloned table with the masked data
df_masked.write.mode("overwrite").saveAsTable(clone_table)

# Optional: Vacuum the table to clean up old files and optimize
spark.sql(f"VACUUM {clone_table} RETAIN 0 HOURS")

# Optional: Optimize the table by Z-ordering on invoice_number for better read performance
spark.sql(f"OPTIMIZE {clone_table} ZORDER BY (invoice_number)")

