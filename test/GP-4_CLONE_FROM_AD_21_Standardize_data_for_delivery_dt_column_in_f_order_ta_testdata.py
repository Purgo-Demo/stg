from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, DoubleType, TimestampType, LongType, ArrayType, MapType

# Create a SparkSession
spark = SparkSession.builder.appName("Test Data Generation").getOrCreate()

# Define the schema for f_order table
f_order_schema = StructType([
    StructField("order_nbr", StringType(), True),
    StructField("order_type", LongType(), True),
    StructField("delivery_dt", DecimalType(38, 0), True),
    StructField("order_qty", DoubleType(), True),
    StructField("sched_dt", DecimalType(38, 0), True),
    StructField("expected_shipped_dt", DecimalType(38, 0), True),
    StructField("actual_shipped_dt", DecimalType(38, 0), True),
    StructField("order_line_nbr", StringType(), True),
    StructField("loc_tracker_id", StringType(), True),
    StructField("shipping_add", StringType(), True),
    StructField("primary_qty", DoubleType(), True),
    StructField("open_qty", DoubleType(), True),
    StructField("shipped_qty", DoubleType(), True),
    StructField("order_desc", StringType(), True),
    StructField("flag_return", StringType(), True),
    StructField("flag_cancel", StringType(), True),
    StructField("cancel_dt", DecimalType(38, 0), True),
    StructField("cancel_qty", DoubleType(), True),
    StructField("crt_dt", TimestampType(), True),
    StructField("updt_dt", TimestampType(), True)
])

# Generate test data
test_data = [
    # Happy path test data
    ("123456", 1, 20240910, 100.0, 20240915, 20240920, 20240925, "1", "loc_001", "123 Lane, City", 100.0, 10.0, 90.0, "Order Description", "N", "N", None, 0.0, "2024-03-21T00:00:00.000+0000", "2024-03-21T00:00:00.000+0000"),
    ("123457", 1, 20240911, 200.0, 20240916, 20240921, 20240926, "2", "loc_002", "456 Lane, City", 200.0, 20.0, 180.0, "Another Order", "N", "N", None, 0.0, "2024-03-21T00:00:00.000+0000", "2024-03-21T00:00:00.000+0000"),
    
    # Edge cases
    ("123458", 0, None, 0.0, None, None, None, None, None, None, 0.0, 0.0, 0.0, None, None, None, None, 0.0, None, None),
    ("123459", 1, 99999999, 0.1, 20240101, 20241231, 20241231, "3", "loc_003", "789 Lane, City", 0.1, 0.0, 0.1, "Edge Order", "Y", "Y", 20240101, 0.1, "2024-03-21T00:00:00.000+0000", "2024-03-21T00:00:00.000+0000"),
    
    # Error cases
    ("123460", 1, 20231301, 100.0, 20240101, 20241231, 20241231, "4", "loc_004", "101 Lane, City", 100.0, 10.0, 90.0, "Invalid Date Order", "Y", "Y", 20241231, 0.0, "2024-03-21T00:00:00.000+0000", "2024-03-21T00:00:00.000+0000"),
    ("123461", 1, 2024abcd, 100.0, 20240101, 20241231, 20241231, "5", "loc_005", "112 Lane, City", 100.0, 10.0, 90.0, "Invalid Type Order", "Y", "Y", None, 100.0, "2024-03-21T00:00:00.000+0000", "2024-03-21T00:00:00.000+0000"),
    
    # Special characters
    ("#%$@!", 2, 20240912, 300.0, 20240917, 20240922, 20240927, "&@#$%", "!@#", "Special Char, City", 300.0, 30.0, 270.0, "!@#$ Order", "N", "N", None, 0.0, "2024-03-21T00:00:00.000+0000", "2024-03-21T00:00:00.000+0000"),
    
    # Multi-byte characters
    ("订单编号", 3, 20240913, 400.0, 20240918, 20240923, 20240928, "行号", "跟踪器ID", "地址, 市", 400.0, 40.0, 360.0, "描述", "N", "N", None, 0.0, "2024-03-21T00:00:00.000+0000", "2024-03-21T00:00:00.000+0000")
]

# Create DataFrame with the defined schema
f_order_df = spark.createDataFrame(test_data, schema=f_order_schema)

# Show the test data
f_order_df.show(truncate=False)

# Validate delivery_dt is Decimal (38,0) and in yyyymmdd format
validation_df = f_order_df.select(
    "order_nbr", 
    F.when((F.col("delivery_dt").between(10000101, 99991231)), "Valid").otherwise("Invalid").alias("delivery_dt_check")
)

# Show validation results
validation_df.show(truncate=False)
