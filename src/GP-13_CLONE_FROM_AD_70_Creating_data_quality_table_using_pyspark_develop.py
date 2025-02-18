from pyspark.sql.functions import col, length, expr, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, LongType

# Define schema for the DQ table
dq_table_schema = StructType([
    StructField("check_no", LongType(), True),
    StructField("check_name", StringType(), True),
    StructField("dq_result", StringType(), True)
])

# Create and overwrite the DQ table
spark.createDataFrame([], dq_table_schema).write.mode("overwrite").saveAsTable("purgo_playground.dq_check_table")

# Load the d_product_revenue table
df_revenue = spark.table("purgo_playground.d_product_revenue")

# Define expected column names
expected_columns = [
    "product_id", "product_name", "product_type", "revenue", "country",
    "customer_id", "purchased_date", "invoice_date", "invoice_number",
    "is_returned", "customer_satisfaction_score", "product_details"
]

# Column Name Validation
column_check = (set(expected_columns) == set(df_revenue.columns))
column_check_result = [("1", "Column Name Validation", "Pass" if column_check else "Fail")]

# Revenue Value Check (ensure non-negative revenue)
revenue_negative_check = df_revenue.filter(col("revenue") < 0).count() == 0
revenue_check_result = [("2", "Revenue Value Check", "Pass" if revenue_negative_check else "Fail")]

# Decimal Precision Check (ensure two decimal places for revenue)
decimal_check = df_revenue.withColumn(
    "revenue_decimals",
    expr("length(cast(revenue * 100 as long)) - length(revenue * 100)")
).filter(col("revenue_decimals") != 0).count() == 0
decimal_precision_check_result = [("3", "Decimal Precision Check", "Pass" if decimal_check else "Fail")]

# Compile all results
check_results = column_check_result + revenue_check_result + decimal_precision_check_result

# Convert results to DataFrame
check_results_df = spark.createDataFrame(check_results, schema=dq_table_schema)

# Write results to the DQ table
check_results_df.write.mode("overwrite").saveAsTable("purgo_playground.dq_check_table")

# Handle any failed checks, logging them to the im_data_check table with timestamp
failed_checks = check_results_df.filter(col("dq_result") == "Fail") \
    .withColumn("dq_check_date", current_timestamp()) \
    .select(col("check_name"), col("dq_result").alias("result"), col("dq_check_date"))

if failed_checks.count() > 0:
    failed_checks.write.mode("append").format("delta").saveAsTable("purgo_playground.im_data_check")
