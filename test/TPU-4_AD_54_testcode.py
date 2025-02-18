# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, isnan, rlike
from pyspark.sql.types import StructType, StructField, StringType, DecimalType

# Start Spark session
spark = SparkSession.builder.appName("SalesDataValidation").getOrCreate()

# Define schema for the sales data
sales_schema = StructType([
    StructField("Country_cd", StringType(), True),
    StructField("Product_id", StringType(), True),
    StructField("qty_sold", DecimalType(10,0), True),
    StructField("sales_date", StringType(), True)
])

# Load the sales file from DBFS
sales_df = spark.read.option("header", "true").schema(sales_schema).csv("dbfs:/FileStore/tables/sales_20240611.csv")

# Add validation columns for error detection
validations_df = sales_df.withColumn("validation_errors", when(col("Country_cd").isNull() | (col("Country_cd") == ""), "Missing country_cd")
                                                  .when(isnan(col("qty_sold")) | col("qty_sold").cast(DecimalType(10,0)).isNull(), "qty_sold should be numeric")
                                                  .when(~col("sales_date").rlike("^\d{4}-\d{2}-\d{2}$"), "sales_date should be in yyyy-mm-dd format"))

# Detect duplicate product IDs
duplicate_products_df = sales_df.groupBy("Product_id").count().filter(col("count") > 1).select("Product_id")
duplicates_df = sales_df.join(duplicate_products_df, on="Product_id", how="inner").withColumn("validation_errors", when(col("Product_id").isNotNull(), "Duplicate Product_id"))

# Union validations and duplicates to get exception records
errors_df = validations_df.union(duplicates_df).filter(col("validation_errors").isNotNull())

# Write exceptions to sales_exceptions table, handling schema dynamically
errors_df.write.option("mergeSchema", "true").mode("append").format("delta").saveAsTable("purgo_playground.sales_exceptions")

# Filter valid records by subtracting errors
valid_sales_df = sales_df.subtract(errors_df.drop("validation_errors"))

# Write valid sales records to the sales table, handling schema dynamically
valid_sales_df.write.option("mergeSchema", "true").mode("append").format("delta").saveAsTable("purgo_playground.sales")

# Stop Spark session
spark.stop()
