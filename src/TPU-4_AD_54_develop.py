from pyspark.sql.functions import col, lit, count, when
from pyspark.sql.window import Window
from pyspark.sql.types import BooleanType
from datetime import datetime

# Define UDF to check if a value is numeric
def is_numeric(value):
    try:
        float(value)
        return True
    except ValueError:
        return False

is_numeric_udf = udf(lambda x: is_numeric(x), BooleanType())

# Define UDF to check for valid date format
def valid_date_format(value):
    try:
        datetime.strptime(value, "%Y-%m-%d")
        return True
    except ValueError:
        return False

valid_date_format_udf = udf(lambda x: valid_date_format(x), BooleanType())

# Load sales file from DBFS with predefined schema
sales_df = spark.read.format("csv") \
    .option("header", "true") \
    .schema(schema) \
    .load("dbfs:/FileStore/tables/sales_20240611.csv")

# Quality checks on sales data
sales_df = sales_df.withColumn("validation_errors",
    when(col("Country_cd").isNull(), lit("country_cd should not be null"))
    .when(~is_numeric_udf(col("qty_sold")), lit("qty_sold should be numeric"))
    .when(~valid_date_format_udf(col("sales_date")), lit("Date should be in yyyy-mm-dd format"))
)

# Check for duplicate product_id
windowSpec = Window.partitionBy("Product_id")
sales_df = sales_df.withColumn("duplicate_check", count("Product_id").over(windowSpec)) \
    .withColumn("validation_errors",
    when(col("duplicate_check") > 1, lit("product_id should not be duplicate")).otherwise(col("validation_errors"))
)

# Filter records with validation errors to exception table
exceptions_df = sales_df.filter(col("validation_errors").isNotNull()) \
    .select("Country_cd", "Product_id", "qty_sold", "sales_date", "validation_errors")

exceptions_df.write.format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .saveAsTable("purgo_playground.sales_exceptions")

# Filter valid records to sales table
valid_sales_df = sales_df.filter(col("validation_errors").isNull()) \
    .select("Country_cd", "Product_id", col("qty_sold").cast("decimal(10,0)").alias("qty_sold"),
            col("sales_date").cast("date").alias("sales_date"))

valid_sales_df.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable("purgo_playground.sales")
