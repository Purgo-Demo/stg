from pyspark.sql.types import *
from pyspark.sql.functions import col, lit, when, countDistinct
from pyspark.sql.window import Window
from pyspark.sql.functions import udf
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

# Check for null country_cd and add validation message
sales_df = sales_df.withColumn("validation_errors",
    when(col("Country_cd").isNull(), lit("country_cd should not be null"))
)

# UDF to check if a value is numeric
def is_numeric(value):
    try:
        float(value)
        return True
    except ValueError:
        return False

is_numeric_udf = udf(lambda x: is_numeric(x), BooleanType())

# Check for non-numeric qty_sold and add validation message
sales_df = sales_df.withColumn("validation_errors",
    when(~is_numeric_udf(col("qty_sold")), lit("qty_sold should be numeric")).otherwise(col("validation_errors"))
)

# Check for duplicate product_id using window function and add validation message
duplicate_product_ids = sales_df.groupBy("Product_id").count().filter("count > 1").select("Product_id")

sales_df = sales_df.join(duplicate_product_ids, "Product_id", "left_anti") \
    .withColumn("validation_errors",
    when(col("Product_id").isNull(), lit("product_id should not be duplicate")).otherwise(col("validation_errors"))
)

# UDF to check for valid date format
def valid_date_format(value):
    try:
        datetime.strptime(value, "%Y-%m-%d")
        return True
    except ValueError:
        return False

valid_date_format_udf = udf(lambda x: valid_date_format(x), BooleanType())

# Check for incorrect date format and add validation message
sales_df = sales_df.withColumn("validation_errors",
    when(~valid_date_format_udf(col("sales_date")), lit("Date should be in yyyy-mm-dd format")).otherwise(col("validation_errors"))
)

# Filter out records with errors to exception table
exceptions_df = sales_df.filter(col("validation_errors").isNotNull()) \
    .select("Country_cd", "Product_id", "qty_sold", "sales_date", "validation_errors")

# Save exceptions to exception table
exceptions_df.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable("purgo_playground.sales_exceptions")

# Filter out valid records to sales table
valid_sales_df = sales_df.filter(col("validation_errors").isNull()) \
    .select("Country_cd", "Product_id", col("qty_sold").cast("decimal(10,0)").alias("qty_sold"), col("sales_date").cast("date").alias("sales_date"))

# Save valid records to sales table
valid_sales_df.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable("purgo_playground.sales")
