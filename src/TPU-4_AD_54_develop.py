from pyspark.sql.types import *
from pyspark.sql.functions import col, lit, udf, when, countDistinct
from pyspark.sql.window import Window
from datetime import datetime

# Schema definition for the sales data
schema = StructType([
    StructField("country_cd", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("qty_sold", StringType(), True),
    StructField("sales_date", StringType(), True)
])

# Load data from CSV file
sales_df = spark.read.format("csv") \
    .option("header", "true") \
    .schema(schema) \
    .load("dbfs:/FileStore/tables/sales_20240611.csv")

# Data Quality Checks using withColumn and UDFs
# Check for NULL values in the country_cd column
sales_df = sales_df.withColumn("validation_errors",
    when(col("country_cd").isNull(), lit("country_cd should not be null")).otherwise(lit(None))
)

# Numeric check for qty_sold column using UDF
def is_numeric(value):
    try:
        float(value)
        return True
    except ValueError:
        return False

is_numeric_udf = udf(lambda x: is_numeric(x), BooleanType())

sales_df = sales_df.withColumn("validation_errors",
    when(~is_numeric_udf(col("qty_sold")), lit("qty_sold should be numeric")).otherwise(col("validation_errors"))
)

# Check for duplicate product_id using window function
windowSpec = Window.partitionBy("product_id")
sales_df = sales_df.withColumn("duplicate_check", countDistinct("product_id").over(windowSpec)) \
    .withColumn("validation_errors",
    when(col("duplicate_check") > 1, lit("product_id should not be duplicate")).otherwise(col("validation_errors"))
)

# Date format validation for sales_date
def valid_date_format(date_str):
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
        return True
    except ValueError:
        return False

valid_date_format_udf = udf(lambda x: valid_date_format(x), BooleanType())

sales_df = sales_df.withColumn("validation_errors",
    when(~valid_date_format_udf(col("sales_date")), lit("Date should be in yyyy-mm-dd format")).otherwise(col("validation_errors"))
)

# Separate records with validation errors into exception table
exceptions_df = sales_df.filter(col("validation_errors").isNotNull()) \
    .select("country_cd", "product_id", "qty_sold", "sales_date", "validation_errors")

# Write exceptions into sales_exceptions table with mergeSchema option
exceptions_df.write.format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .saveAsTable("purgo_playground.sales_exceptions")

# Filter valid records to write to sales table
valid_sales_df = sales_df.filter(col("validation_errors").isNull()) \
    .select("country_cd", "product_id", col("qty_sold").cast("decimal(10,0)").alias("qty_sold"), col("sales_date").cast("date").alias("sales_date"))

# Write valid records into sales table with mergeSchema option
valid_sales_df.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable("purgo_playground.sales")
