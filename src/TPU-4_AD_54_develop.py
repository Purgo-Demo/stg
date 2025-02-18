# Import necessary libraries
from pyspark.sql.functions import col, when, isnan, count, lit, rlike
from pyspark.sql.types import StructType, StructField, StringType, DecimalType

# Define schema for the sales data
sales_schema = StructType([
    StructField("Country_cd", StringType(), True),
    StructField("Product_id", StringType(), True),
    StructField("qty_sold", StringType(), True),
    StructField("sales_date", StringType(), True)
])

# Load the sales file from DBFS
sales_df = spark.read.option("header", "true").schema(sales_schema).csv("dbfs:/FileStore/tables/sales_20240611.csv")

# Add validation columns for error detection
validations_df = sales_df.withColumn(
    "validation_errors",
    when(col("Country_cd").isNull() | (col("Country_cd") == ""), "Missing country_cd")
    .when(~col("qty_sold").rlike("^[0-9]+$"), "qty_sold should be numeric")
    .when(~col("sales_date").rlike("^\d{4}-\d{2}-\d{2}$"), "sales_date should be in yyyy-mm-dd format")
    .otherwise(lit(None))
)

# Detect duplicate product IDs
duplicate_products_df = sales_df.groupBy("Product_id").count().filter(col("count") > 1).select("Product_id")
duplicates_df = sales_df.join(duplicate_products_df, "Product_id", "inner").withColumn(
    "validation_errors", lit("Duplicate Product_id")
)

# Union validations and duplicates to get exception records
errors_df = validations_df.union(duplicates_df).filter(col("validation_errors").isNotNull()).dropDuplicates()

# Write exceptions to sales_exceptions table, handling schema dynamically
errors_df.write.option("mergeSchema", "true").mode("append").format("delta").saveAsTable("purgo_playground.sales_exceptions")

# Filter valid records by subtracting errors
valid_sales_df = sales_df.join(errors_df.drop("validation_errors"), ["Country_cd", "Product_id", "qty_sold", "sales_date"], "leftanti")

# Convert qty_sold to correct type for valid records
valid_sales_df = valid_sales_df.withColumn("qty_sold", col("qty_sold").cast(DecimalType(10, 0)))

# Write valid sales records to the sales table, handling schema dynamically
valid_sales_df.write.option("mergeSchema", "true").mode("append").format("delta").saveAsTable("purgo_playground.sales")
