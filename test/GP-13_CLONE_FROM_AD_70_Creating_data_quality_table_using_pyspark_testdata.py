from pyspark.sql import SparkSession
from pyspark.sql.functions import col, length, expr
from pyspark.sql.types import StructType, StructField, StringType, BigIntType

# Create Spark session
spark = SparkSession.builder \
    .appName("Data Quality Check Table Creation") \
    .getOrCreate()

# Drop and create new DQ table schema
dq_table_schema = StructType([
    StructField("check_no", BigIntType(), True),
    StructField("check_name", StringType(), True),
    StructField("dq_result", StringType(), True)
])

# Create empty DataFrame with DQ table schema
dq_results_df = spark.createDataFrame([], dq_table_schema)
dq_results_df.createOrReplaceTempView("dq_check_table")

# Define column names
expected_columns = [
    'product_id', 'product_name', 'product_type', 'revenue', 'country',
    'customer_id', 'purchased_date', 'invoice_date', 'invoice_number',
    'is_returned', 'customer_satisfaction_score', 'product_details'
]

# Load d_product_revenue table
df_revenue = spark.table("purgo_playground.d_product_revenue")

# Column Name Validation
column_check = (set(expected_columns) == set(df_revenue.columns))
column_check_result = [("1", "Column Name Validation", "Pass" if column_check else "Fail")]

# Revenue Value Check (non-negative)
revenue_negative_check = df_revenue.filter(col("revenue") < 0).count() == 0
revenue_check_result = [("2", "Revenue Value Check", "Pass" if revenue_negative_check else "Fail")]

# Decimal Precision Check (exactly two decimal places)
decimal_check = df_revenue.withColumn(
    "revenue_decimals",
    length(expr("substring(revenue, instr(revenue, '.') + 1)"))
).filter(col("revenue_decimals") != 2).count() == 0
decimal_precision_check_result = [("3", "Decimal Precision Check", "Pass" if decimal_check else "Fail")]

# Collect all results
checks = column_check_result + revenue_check_result + decimal_precision_check_result

# Convert results to DataFrame
check_results_df = spark.createDataFrame(checks, schema=dq_table_schema)

# Write results to the DQ table
check_results_df.write.mode("overwrite").saveAsTable("purgo_playground.dq_check_table")
