from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, expr, when
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# Create a Spark session
spark = SparkSession.builder \
    .appName("DatabricksTestDataGeneration") \
    .getOrCreate()

# Define schema for test data
schema = StructType([
    StructField("txn_id", StringType(), True),
    StructField("inv_loc", StringType(), True),
    StructField("financial_qty", DoubleType(), True),
    StructField("net_qty", DoubleType(), True),
    StructField("expired_qt", DoubleType(), True),
    StructField("item_nbr", StringType(), True),
    StructField("unit_cost", DoubleType(), True),
    StructField("um_rate", DoubleType(), True),
    StructField("plant_loc_cd", StringType(), True),
    StructField("inv_stock_reference", StringType(), True),
    StructField("stock_type", StringType(), True),
    StructField("qty_on_hand", DoubleType(), True),
    StructField("qty_shipped", DoubleType(), True),
    StructField("cancel_dt", DoubleType(), True),
    StructField("flag_active", StringType(), True),
    StructField("crt_dt", TimestampType(), True),
    StructField("updt_dt", TimestampType(), True)
])

# Generate test data
data = [
    # Happy Path
    ("txn1001", "loc01", 1000.0, 800.0, 0.0, "item001", 50.0, 1.2, "PL01", "ref001", "A", 1200.0, 200.0, None, "Y", "2024-03-21T00:00:00.000+0000", "2024-03-21T00:00:00.000+0000"),
    ("txn1002", "loc01", 2000.0, 1500.0, 0.0, "item002", 30.0, 1.5, "PL01", "ref002", "B", 2500.0, 100.0, None, "N", "2024-03-22T00:00:00.000+0000", "2024-03-22T00:00:00.000+0000"),
    
    # Edge Cases
    ("txn1003", "loc02", 0.0, 0.0, 0.0, "item003", 10.0, 0.0, "PL02", "ref003", "C", 0.0, 0.0, None, "Y", "2024-03-23T00:00:00.000+0000", "2024-03-23T00:00:00.000+0000"),
    ("txn1004", "loc02", 9999999999.99, 999999999.99, 0.0, "item004", 100000.0, 1000.0, "PL02", "ref004", "D", 10000000000.0, 1000.0, None, "N", "2024-03-24T00:00:00.000+0000", "2024-03-24T00:00:00.000+0000"),
    
    # Error Cases
    ("txn1005", "loc03", -100.0, 50.0, 0.0, "item005", -20.0, 0.5, "PL03", "ref005", "E", -150.0, 0.0, None, "Y", "2024-03-25T00:00:00.000+0000", "2024-03-25T00:00:00.000+0000"),
    ("txn1006", "loc03", 500.0, 500.0, 0.0, "item006", 0.0, 0.0, "PL03", "ref006", "F", 500.0, 500.0, None, "Z", "2024-03-26T00:00:00.000+0000", "2024-03-26T00:00:00.000+0000"), # Unsupported flag
    
    # NULL Handling
    ("txn1007", None, 300.0, None, None, "item007", None, 1.0, None, None, None, None, None, None, None, None, None),
    ("txn1008", "loc04", None, 200.0, 0.0, "item008", 40.0, 1.1, "PL04", "ref008", "G", 250.0, None, None, "Y", None, "2024-03-27T00:00:00.000+0000"),
    
    # Special Characters and Multi-byte Characters
    ("txn1009", "loc05", 700.0, 600.0, 0.0, "item🔥009", 60.0, 1.6, "PL05", "ref🔥009", "H", 1300.0, 700.0, None, "Y", "2024-03-28T00:00:00.000+0000", "2024-03-28T00:00:00.000+0000"),
    ("txn1010", "loc05", 800.0, 700.0, 0.0, "item你好010", 70.0, 1.7, "PL05", "ref你好010", "I", 1500.0, 800.0, None, "N", "2024-03-29T00:00:00.000+0000", "2024-03-29T00:00:00.000+0000")
]

# Create DataFrame
df = spark.createDataFrame(data, schema)

# Show the DataFrame
df.show(truncate=False)

# Calculate Inventory at Risk
inventory_at_risk_df = df.filter(col("flag_active") == "Y") \
    .agg(expr("sum(financial_qty) as inventory_at_risk"))

# Calculate Total Inventory
total_inventory_df = df.agg(expr("sum(financial_qty) as total_inventory"))

# Join the two calculated dataframes for final output
risk_calculation_df = inventory_at_risk_df.crossJoin(total_inventory_df) \
    .withColumn("percentage_inventory_at_risk", expr("inventory_at_risk / total_inventory * 100"))

# Show Risk Calculation
risk_calculation_df.show(truncate=False)

# Output Schema and Data Types check
print("DataFrame Schema:")
df.printSchema()
