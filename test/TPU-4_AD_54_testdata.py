from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, isnull, count, lit, expr
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, TimestampType
import pyspark.sql.functions as F

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Data Quality File Check") \
    .getOrCreate()

# Define schema for the sales data
schema = StructType([
    StructField("country_cd", StringType(), True),
    StructField("qty_sold", DoubleType(), True),
    StructField("product_id", StringType(), True),
    StructField("Date", StringType(), True)  # Using StringType to perform custom validation
])

# Load sales data from CSV file in DBFS
sales_df = spark.read.format("csv") \
    .option("header", "true") \
    .schema(schema) \
    .load("dbfs:/FileStore/tables/sales_20240611.csv")

# Perform quality checks and identify errors
error_df = sales_df.withColumn("error_message", lit(None).cast(StringType()))
error_df = error_df.withColumn("error_message", when(isnull(col("country_cd")), "Error: country_cd is null").otherwise(col("error_message")))
error_df = error_df.withColumn("error_message", when(~col("qty_sold").cast(DoubleType()).isNotNull(), "Error: qty_sold is not numeric").otherwise(col("error_message")))

# Check for duplicate product_ids
product_id_counts = sales_df.groupBy("product_id").count().filter("count > 1")
error_df = error_df.join(product_id_counts, on="product_id", how="left")
error_df = error_df.withColumn("error_message", when(col("count").isNotNull(), "Error: Duplicate product_id").otherwise(col("error_message")))

# Validate Date format
error_df = error_df.withColumn("error_message", 
                               when(~col("Date").rlike("^\d{4}-\d{2}-\d{2}$"), "Error: Date format is invalid").otherwise(col("error_message")))

# Select records with any errors
exception_records = error_df.filter(col("error_message").isNotNull())

# Drop exception records from the original data
valid_records = sales_df.join(exception_records.select("product_id"), on="product_id", how="left_anti")

# Define schema for exception records
exception_schema = StructType([
    StructField("country_cd", StringType(), True),
    StructField("qty_sold", DoubleType(), True),
    StructField("product_id", StringType(), True),
    StructField("Date", StringType(), True),
    StructField("error_message", StringType(), True)
])

# Write exception records to the exception table
exception_records.write \
    .option("mergeSchema", "true") \
    .format("delta") \
    .mode("append") \
    .saveAsTable("sales_exception_table")

# Write valid records to sales table in Unity Catalog
valid_records.write \
    .option("mergeSchema", "true") \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("sales_table")

