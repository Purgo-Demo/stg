# Loading necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, isnan, expr
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, DateType

# Initialize Spark session
spark = SparkSession.builder.appName("GenerateTestData").getOrCreate()

# Define schema for sales table
sales_schema = StructType([
    StructField("Country_cd", StringType(), True),
    StructField("Product_id", StringType(), True),
    StructField("qty_sold", DecimalType(10, 0), True),
    StructField("sales_date", DateType(), True)
])

# Sample data generation
data = [
    # Happy path
    ("US", "101", "123", "2024-03-21"),  # Valid Record
    ("CA", "102", "50", "2024-03-22"),   # Valid Record
    # Edge cases
    ("", "103", "0", "2024-03-23"),      # Empty country code, qty = 0
    ("MX", "104", "9999999999", "2024-03-24"),  # Max edge qty
    ("JP", None, "100", "2024-03-25"),   # Null product_id
    # Error cases
    (None, "105", "svv", "2024-03-26"),  # Non-numeric qty_sold
    ("IN", "105", "-123", "2024-03-27"), # Negative qty_sold
    ("IN", "105", "123", "03-27-2024"),  # Invalid date format
    # Special characters and multi-byte characters
    ("DE", "106", "10", "2024-03-28"),   # Valid with multi-byte
    ("FR", "107@#$%", "200", "2024-03-29"), # Special characters in Product_id
]

# Create DataFrame from sample data
df = spark.createDataFrame(data, schema=sales_schema)

# Display test data
df.show(truncate=False)

# Define quality checks
exception_conditions = [
    (col("Country_cd").isNull() | (col("Country_cd") == ""), "Missing country_cd"),
    (col("qty_sold").cast(DecimalType(10, 0)).isNull() | isnan(col("qty_sold")), "qty_sold should be numeric"),
    (col("qty_sold").cast(DecimalType(10, 0)) < 0, "qty_sold cannot be negative"),
    (~col("sales_date").rlike("^\d{4}-\d{2}-\d{2}$"), "sales_date should be in 'yyyy-mm-dd' format")
]

# Create exception DataFrame
exceptions_df = df
for condition, error_msg in exception_conditions:
    exceptions_df = exceptions_df.withColumn("validation_errors", when(condition, error_msg).otherwise(None))

# Filter exceptions
exceptions_df = exceptions_df.filter(col("validation_errors").isNotNull())

# Write exceptions to sales_exceptions table with mergeSchema=True
exceptions_df.write.option("mergeSchema", "true").mode("append").format("delta").saveAsTable("purgo_playground.sales_exceptions")

# Filter valid records
valid_df = df.subtract(exceptions_df.drop("validation_errors"))

# Write valid records to sales table with mergeSchema=True
valid_df.write.option("mergeSchema", "true").mode("append").format("delta").saveAsTable("purgo_playground.sales")

