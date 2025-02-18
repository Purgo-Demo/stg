from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, date_format
import json

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Databricks Test Data Generation") \
    .getOrCreate()

# Define sample data
sales_data = [
    {"Country_cd": "USA", "Product_id": "P123", "qty_sold": "10", "sales_date": "2023-06-11"},
    {"Country_cd": "CAN", "Product_id": "P124", "qty_sold": "NotNum", "sales_date": "2023-06-12"},
    {"Country_cd": "GER", "Product_id": "P123", "qty_sold": "5", "sales_date": "2023-06-13"},
    {"Country_cd": "FRA", "Product_id": "P126", "qty_sold": "12", "sales_date": "2023-06-14"},
    {"Country_cd": None, "Product_id": "P127", "qty_sold": "15", "sales_date": "2023-06-15"},
    {"Country_cd": "USA", "Product_id": "P128", "qty_sold": "20", "sales_date": "2023-06-16"},
    {"Country_cd": "UK", "Product_id": "P129", "qty_sold": "NaN", "sales_date": "2023-06-17"},
    {"Country_cd": "AUS", "Product_id": "P130", "qty_sold": "25", "sales_date": "2023-06-18"},
    {"Country_cd": "AUS", "Product_id": "P130", "qty_sold": "30", "sales_date": "2023-06-19"},
    {"Country_cd": "ITA", "Product_id": "P131", "qty_sold": "35", "sales_date": "202306-20"},
    # More data following the same pattern...
]

# Create DataFrame from sample data
df_sales = spark.createDataFrame(sales_data)

# Define rules to identify invalid records
df_with_errors = df_sales.withColumn("validation_errors", lit(None).cast("string"))

# Validate that country_cd is not null
df_with_errors = df_with_errors.withColumn("validation_errors",
    when(col("Country_cd").isNull(), lit("Country_cd is null"))
    .otherwise(col("validation_errors"))
)

# Validate that product_id is unique
duplicates = [row.Product_id for row in df_sales.groupBy("Product_id").count().filter("count > 1").select("Product_id").collect()]
df_with_errors = df_with_errors.withColumn("validation_errors",
    when(col("Product_id").isin(duplicates), lit("Duplicate Product_id"))
    .otherwise(col("validation_errors"))
)

# Validate that qty_sold is numeric
df_with_errors = df_with_errors.withColumn("validation_errors",
    when(~col("qty_sold").cast("decimal(10,0)").isNotNull(), lit("qty_sold is not numeric"))
    .otherwise(col("validation_errors"))
)

# Validate that sales_date is in yyyy-mm-dd format
df_with_errors = df_with_errors.withColumn("validation_errors",
    when(~date_format(col("sales_date"), "yyyy-MM-dd").isNotNull(), lit("sales_date format is incorrect"))
    .otherwise(col("validation_errors"))
)

# Separate valid and error records
df_valid_records = df_with_errors.filter(col("validation_errors").isNull()).drop("validation_errors")
df_error_records = df_with_errors.filter(col("validation_errors").isNotNull())

# Write valid records to the sales table with mergeSchema=true
df_valid_records.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable("purgo_playground.sales")

# Write error records to the sales_exceptions table with mergeSchema=true
df_error_records.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable("purgo_playground.sales_exceptions")

