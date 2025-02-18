from pyspark.sql.types import StructType, StructField, StringType, BooleanType, DecimalType, DateType
from pyspark.sql.functions import col, lit, udf, when, count
from pyspark.sql.window import Window
from datetime import datetime

# Define schema for the sales file
schema = StructType([
    StructField("Country_cd", StringType(), True),
    StructField("Product_id", StringType(), True),
    StructField("qty_sold", StringType(), True),
    StructField("sales_date", StringType(), True)
])

# Load sales file from DBFS
sales_df = spark.read.format("csv") \
    .option("header", "true") \
    .schema(schema) \
    .load("dbfs:/FileStore/tables/sales_20240611.csv")

# Function to check if a value is numeric
def is_numeric(value):
    try:
        float(value)
        return True
    except ValueError:
        return False

# Register UDF for numeric check
is_numeric_udf = udf(lambda x: is_numeric(x), BooleanType())

# Function to check if date is in yyyy-mm-dd format
def valid_date_format(value):
    try:
        datetime.strptime(value, "%Y-%m-%d")
        return True
    except ValueError:
        return False

# Register UDF for date format check
valid_date_format_udf = udf(lambda x: valid_date_format(x), BooleanType())

# Validate country_cd not null
sales_df = sales_df.withColumn("validation_errors",
    when(col("Country_cd").isNull(), lit("country_cd should not be null"))
)

# Validate qty_sold numeric
sales_df = sales_df.withColumn("validation_errors",
    when(~is_numeric_udf(col("qty_sold")), lit("qty_sold should be numeric")).otherwise(col("validation_errors"))
)

# Validate unique product_id
window_spec = Window.partitionBy("Product_id")
sales_df = sales_df.withColumn("duplicate_check", count("Product_id").over(window_spec)) \
    .withColumn("validation_errors",
    when(col("duplicate_check") > 1, lit("product_id should not be duplicate")).otherwise(col("validation_errors"))
)

# Validate date format
sales_df = sales_df.withColumn("validation_errors",
    when(~valid_date_format_udf(col("sales_date")), lit("Date should be in yyyy-mm-dd format")).otherwise(col("validation_errors"))
)

# Extract records with validation errors
exceptions_df = sales_df.filter(col("validation_errors").isNotNull()) \
    .select("Country_cd", "Product_id", "qty_sold", "sales_date", "validation_errors")

# Save exceptions to exception table
exceptions_df.write.format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .saveAsTable("purgo_playground.sales_exceptions")

# Extract valid records
valid_sales_df = sales_df.filter(col("validation_errors").isNull()) \
    .select(
        "Country_cd",
        "Product_id",
        col("qty_sold").cast(DecimalType(10, 0)).alias("qty_sold"),
        col("sales_date").cast(DateType()).alias("sales_date")
    )

# Save valid records to sales table
valid_sales_df.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable("purgo_playground.sales")
