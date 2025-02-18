from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, date_format, to_date, expr
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, DateType

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Sales Data Quality Check") \
    .enableHiveSupport() \
    .getOrCreate()

# Define schemas for validation
sales_schema = StructType([
    StructField("Country_cd", StringType(), nullable=True),
    StructField("Product_id", StringType(), nullable=False),
    StructField("qty_sold", DecimalType(10, 0), nullable=True),
    StructField("sales_date", DateType(), nullable=False)
])

exception_schema = StructType([
    StructField("Country_cd", StringType(), nullable=True),
    StructField("Product_id", StringType(), nullable=False),
    StructField("qty_sold", StringType(), nullable=True),
    StructField("sales_date", StringType(), nullable=False),
    StructField("validation_errors", StringType(), nullable=False)
])

# Load sales data from CSV file
df_sales = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("dbfs:/FileStore/tables/sales_20240611.csv") \
    .select("Country_cd", "Product_id", "qty_sold", "sales_date")

# Convert sales_date to date type for validation
df_sales = df_sales.withColumn("sales_date", to_date(col("sales_date"), "yyyy-MM-dd"))

# Define rules to identify invalid records
df_with_errors = df_sales.withColumn("validation_errors", lit(None).cast("string"))

# Validate that country_cd is not null
df_with_errors = df_with_errors.withColumn("validation_errors", 
    when(col("Country_cd").isNull(), lit("Country_cd is null"))
    .otherwise(col("validation_errors"))
)

# Validate that product_id is unique
df_duplicates = df_sales.groupBy("Product_id").count().filter(col("count") > 1).select("Product_id")
df_with_errors = df_with_errors.withColumn("validation_errors", 
    when(col("Product_id").isin([row.Product_id for row in df_duplicates.collect()]), lit("Duplicate Product_id"))
    .otherwise(col("validation_errors"))
)

# Validate that qty_sold is numeric
df_with_errors = df_with_errors.withColumn("validation_errors",
    when(~col("qty_sold").cast("decimal(10,0)").isNotNull(), lit("qty_sold is not numeric"))
    .otherwise(col("validation_errors"))
)

# Validate that sales_date is in yyyy-mm-dd format
df_with_errors = df_with_errors.withColumn("validation_errors",
    when(col("sales_date").isNull(), lit("sales_date format is incorrect"))
    .otherwise(col("validation_errors"))
)

# Separate valid and error records
df_valid_records = df_with_errors.filter(col("validation_errors").isNull()).drop("validation_errors").select(*sales_schema.fieldNames())
df_error_records = df_with_errors.filter(col("validation_errors").isNotNull()).select(*exception_schema.fieldNames())

# Write valid records to the sales table with mergeSchema=true
df_valid_records.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable("purgo_playground.sales")

# Write error records to the sales_exceptions table with mergeSchema=true
df_error_records.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable("purgo_playground.sales_exceptions")

# Cleanup operations (drop temporary datasets if required)
spark.catalog.clearCache()
