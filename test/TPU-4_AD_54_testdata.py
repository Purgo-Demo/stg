from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, regexp_extract, when, unix_timestamp
from pyspark.sql.types import StringType, StructType, StructField, DateType, DecimalType

# Initialize Spark session
spark = SparkSession.builder \
    .appName("File Quality Check and Load to Unity Catalog") \
    .getOrCreate()

# Define the schema for the sales file
sales_schema = StructType([
    StructField("Country_cd", StringType(), True),
    StructField("Product_id", StringType(), True),
    StructField("qty_sold", StringType(), True),
    StructField("sales_date", StringType(), True)
])

# Load the sales file with mergeSchema set to True
sales_df = spark.read.format("csv") \
    .option("header", "true") \
    .schema(sales_schema) \
    .load("dbfs:/FileStore/tables/sales_20240611.csv")

# Define a function for the quality checks and add error columns
def perform_quality_checks(df):
    # Check for null values in country_cd
    df = df.withColumn("country_cd_error", when(col("Country_cd").isNull(), "country_cd should not be null"))

    # Check for non-numeric values in qty_sold
    df = df.withColumn("qty_sold_numeric",
                       when(col("qty_sold").cast(DecimalType(10, 0)).isNull(), "qty_sold should be numeric"))

    # Check for date format in sales_date
    df = df.withColumn("sales_date_error",
                       when(~regexp_extract(col("sales_date"), r'^\d{4}-\d{2}-\d{2}$', 0), "Date should be in yyyy-mm-dd format"))

    # Deduplicate product_id and identify duplicates
    window_spec = Window.partitionBy("Product_id")
    df = df.withColumn("product_id_count", count("Product_id").over(window_spec))
    df = df.withColumn("product_id_error", when(col("product_id_count") > 1, "product_id should not be duplicate"))

    # Consolidate error columns into a single error message
    df = df.withColumn("validation_errors",
                       concat_ws(", ",
                                 col("country_cd_error"),
                                 col("qty_sold_numeric"),
                                 col("sales_date_error"),
                                 col("product_id_error")))

    # Filter out rows with any validation errors
    error_df = df.filter(col("validation_errors").isNotNull())

    # Clean records without errors
    clean_df = df.filter(col("validation_errors").isNull()).drop("country_cd_error", "qty_sold_numeric", "sales_date_error", "product_id_error", "validation_errors")

    return clean_df, error_df

# Perform quality checks
valid_sales_df, sales_errors_df = perform_quality_checks(sales_df)

# Load exception records to the sales_exceptions table
sales_errors_df.withColumn("validation_errors", sales_errors_df["validation_errors"].cast(StringType())) \
    .write.format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .saveAsTable("purgo_playground.sales_exceptions")

# Load valid sales records to the sales table
valid_sales_df.select(col("Country_cd"), col("Product_id"), col("qty_sold").cast(DecimalType(10, 0)), col("sales_date").cast(DateType())) \
    .write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable("purgo_playground.sales")

