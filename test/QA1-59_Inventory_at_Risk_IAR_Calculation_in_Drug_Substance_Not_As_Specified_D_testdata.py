from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DecimalType, TimestampType

# Start Spark session
spark = SparkSession.builder.appName("GenerateTestData").getOrCreate()

# Define schema for f_inv_movmnt table using Databricks native data types
f_inv_movmnt_schema = StructType([
    StructField("txn_id", StringType(), True),
    StructField("inv_loc", StringType(), True),
    StructField("financial_qty", DoubleType(), True),
    StructField("net_qty", DoubleType(), True),
    StructField("expired_qt", DecimalType(38, 0), True),
    StructField("item_nbr", StringType(), True),
    StructField("unit_cost", DoubleType(), True),
    StructField("um_rate", DoubleType(), True),
    StructField("plant_loc_cd", StringType(), True),
    StructField("inv_stock_reference", StringType(), True),
    StructField("stock_type", StringType(), True),
    StructField("qty_on_hand", DoubleType(), True),
    StructField("qty_shipped", DoubleType(), True),
    StructField("cancel_dt", DecimalType(38, 0), True),
    StructField("flag_active", StringType(), True),
    StructField("crt_dt", TimestampType(), True),
    StructField("updt_dt", TimestampType(), True)
])

# Generate test data
f_inv_movmnt_data = [
    # Happy path test data
    ("txn1", "L1", 100.0, 95.0, 0, "item1", 10.5, 1.0, "PL1", "REF1", "STK1", 500.0, 5.0, None, "Y", "2024-03-21T00:00:00.000+0000", "2024-03-21T00:00:00.000+0000"),
    ("txn2", "L2", 200.0, 190.0, 0, "item2", 15.0, 1.0, "PL2", "REF2", "STK2", 300.0, 10.0, None, "Y", "2024-03-21T00:00:00.000+0000", "2024-03-21T00:00:00.000+0000"),
    # Edge case: large financial_qty and negative net_qty
    ("txn3", "L3", 1.0E12, -100.0, 0, "item3", 20.0, 1.0, "PL3", "REF3", "STK3", 700.0, 5.0, None, "Y", "2024-03-21T00:00:00.000+0000", "2024-03-21T00:00:00.000+0000"),
    # Error case: out-of-range expired_qt
    ("txn4", "L4", 0.0, 0.0, -1, "item4", 5.0, 1.0, "PL4", "REF4", "STK4", 0.0, 0.0, None, "N", "2024-03-21T00:00:00.000+0000", "2024-03-21T00:00:00.000+0000"),
    # NULL handling and special characters
    ("txn5", "L5", None, None, None, "item5✓", 5.5, 1.0, None, "REF5", "STK✓5", None, None, None, None, "2024-03-21T00:00:00.000+0000", None),
    # Special cases with multi-byte characters
    ("txn6", "L6", 300.0, 295.0, 1, "item6エ", 8.0, 1.0, "PL6", "REF6", "STK6", 400.0, 5.0, None, "Y", "2024-03-21T00:00:00.000+0000", "2024-03-21T00:00:00.000+0000"),
]

# Create DataFrame using the schema and data
f_inv_movmnt_df = spark.createDataFrame(f_inv_movmnt_data, schema=f_inv_movmnt_schema)

# Show DataFrame for verification
f_inv_movmnt_df.show(truncate=False)

# SQL Syntax for calculating Inventory at Risk
f_inv_movmnt_df.createOrReplaceTempView("f_inv_movmnt")

# SQL query for Inventory at Risk calculation
sql_query = """
SELECT 
    SUM(financial_qty) AS inventory_at_risk,
    (SUM(financial_qty) / (SELECT SUM(financial_qty) FROM f_inv_movmnt WHERE flag_active = 'Y')) * 100 AS percentage_risk
FROM 
    f_inv_movmnt
WHERE 
    flag_active = 'Y'
"""

# Execute the SQL query
risk_calculation_df = spark.sql(sql_query)

# Show result
risk_calculation_df.show()
