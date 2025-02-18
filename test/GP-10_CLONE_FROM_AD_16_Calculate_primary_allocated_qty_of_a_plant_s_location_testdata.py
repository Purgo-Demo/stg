# Create a Spark DataFrame mimicking the purgo_playground.f_order table and generate test cases

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, DecimalType
from pyspark.sql.functions import col, lit, coalesce, expr

# Initialize a Spark session
spark = SparkSession.builder.appName("Test Data Generation").getOrCreate()

# Define the schema as per purgo_playground.f_order table
f_order_schema = StructType([
    StructField("order_nbr", StringType(), True),
    StructField("order_line_nbr", StringType(), True),
    StructField("primary_qty", DoubleType(), True),
    StructField("open_qty", DoubleType(), True),
    StructField("shipped_qty", DoubleType(), True),
    StructField("cancel_qty", DoubleType(), True)
])

# Generate test data
test_data = [
    # Happy path scenarios
    ("ORDR001", "001", 10.0, 5.0, 3.0, 2.0),  # allocated_qty should be 20.0
    ("ORDR002", "001", 5.0, 2.0, 1.0, 1.0),   # allocated_qty should be 9.0
    
    # Edge cases with NULL handling
    ("ORDR003", "002", 15.0, None, 5.0, None), # allocated_qty should be 20.0 considering NULL as 0.0
    ("ORDR004", "002", None, None, None, None), # allocated_qty should be 0.0
    
    # Special characters in order_nbr
    ("ORDR-#@!", "002", 12.0, 2.0, 3.0, 1.0),  # Special character test
    
    # Error case placeholders (not directly used but for scenario descriptions)
    # e.g., ("ORDR005", "003", None, None, None, None) # Non-existent error placeholder, output should signal no record
]

# Create Spark DataFrame
f_order_df = spark.createDataFrame(test_data, schema=f_order_schema)

# Calculate allocated_qty as per the business logic handling NULLs
f_order_df = f_order_df.withColumn(
    "allocated_qty",
    coalesce(col("primary_qty"), lit(0.0)) +
    coalesce(col("open_qty"), lit(0.0)) +
    coalesce(col("shipped_qty"), lit(0.0)) +
    coalesce(col("cancel_qty"), lit(0.0))
)

# Display the resulting DataFrame
f_order_df.show(truncate=False)

# Simulating a change in 'shipped_qty' to test historical changes handling (Scenario: Data consistency)
historical_changes_df = f_order_df.withColumn("shipped_qty", expr("CASE WHEN order_nbr == 'ORDR003' THEN 10.0 ELSE shipped_qty END"))
historical_changes_df = historical_changes_df.withColumn(
    "updated_allocated_qty",
    coalesce(col("primary_qty"), lit(0.0)) +
    coalesce(col("open_qty"), lit(0.0)) +
    coalesce(col("shipped_qty"), lit(0.0)) +
    coalesce(col("cancel_qty"), lit(0.0))
)

# Show the DataFrame with historical changes
historical_changes_df.show(truncate=False)

# Stop spark session
spark.stop()

