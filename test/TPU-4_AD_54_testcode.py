# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, to_date, row_number, when
from pyspark.sql.window import Window
import pyspark.sql.types as T

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Data Quality and Load") \
    .enableHiveSupport() \
    .getOrCreate()

# Load the sales file from DBFS with mergeSchema option
sales_df = spark.read \
    .option("header", "true") \
    .option("mergeSchema", "true") \
    .csv("dbfs:/FileStore/tables/sales_20240611.csv")

# Define schema for sales table
sales_schema = T.StructType([
    T.StructField("Country_cd", T.StringType(), True),
    T.StructField("Product_id", T.StringType(), True),
    T.StructField("qty_sold", T.DecimalType(10, 0), True),
    T.StructField("sales_date", T.DateType(), True)
])

# Define schema for exceptions table
exceptions_schema = T.StructType([
    T.StructField("Country_cd", T.StringType(), True),
    T.StructField("Product_id", T.StringType(), True),
    T.StructField("qty_sold", T.StringType(), True),
    T.StructField("sales_date", T.StringType(), True),
    T.StructField("validation_errors", T.StringType(), True)
])

# Cast DataFrame to match schema types and perform initial transformations
sales_cast_df = sales_df \
    .withColumn("qty_sold", col("qty_sold").cast(T.DecimalType(10, 0))) \
    .withColumn("sales_date", to_date(col("sales_date"), "yyyy-MM-dd"))

# Window function to check for duplicate Product_id
window_spec = Window.partitionBy("Product_id").orderBy("Product_id")

# Check for null, non-numeric, duplicate and invalid date format conditions
exceptions_df = sales_cast_df \
    .withColumn("row_num", row_number().over(window_spec)) \
    .filter(
        (col("Country_cd").isNull() | col("Country_cd") == "") |
        (col("qty_sold").isNull() | ~col("qty_sold").cast("string").rlike("^[0-9]+$")) |
        (col("sales_date").isNull()) |
        (col("row_num") > 1)
    ) \
    .withColumn("validation_errors", when(col("Country_cd").isNull() | col("Country_cd") == "", "country_cd should not be null")
        .when(~col("qty_sold").cast("string").rlike("^[0-9]+$"), "qty_sold should be numeric")
        .when(col("row_num") > 1, "product_id should not be duplicate")
        .when(col("sales_date").isNull(), "Date should be in yyyy-mm-dd format")
    ) \
    .select("Country_cd", "Product_id", col("qty_sold").cast("string"), col("sales_date").cast("string"), "validation_errors")

# Write exceptions to sales_exceptions table
exceptions_df.write \
    .option("mergeSchema", "true") \
    .mode("overwrite") \
    .saveAsTable("purgo_playground.sales_exceptions")

# Filter out invalid records for final load
valid_sales_df = sales_cast_df.join(exceptions_df, ["Country_cd", "Product_id", "qty_sold", "sales_date"], "left_anti")

# Load valid records to sales table
valid_sales_df.write \
    .option("mergeSchema", "true") \
    .mode("overwrite") \
    .saveAsTable("purgo_playground.sales")
