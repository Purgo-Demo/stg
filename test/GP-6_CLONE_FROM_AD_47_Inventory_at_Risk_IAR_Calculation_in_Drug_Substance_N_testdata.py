# PySpark setup for generating test data

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DecimalType, TimestampType

spark = SparkSession.builder.appName("Test Data Generation").getOrCreate()

# Define schema for the test data based on the purgo_playground.f_inv_movmnt table
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

# Generate the DataFrame with test data
test_data = [
    # Happy path test data
    ("txn_001", "loc_001", 100.0, 95.0, 0, "item_001", 10.0, 1.0, "loc_cd_001", "ref_001", "type_001", 500.0, 5.0, 20210321, "Y", "2024-03-21T00:00:00.000+0000", "2024-03-21T00:00:00.000+0000"),
    ("txn_002", "loc_002", 200.0, 190.0, 0, "item_002", 20.0, 1.0, "loc_cd_002", "ref_002", "type_002", 700.0, 10.0, 20220321, "Y", "2024-03-21T00:00:00.000+0000", "2024-03-21T00:00:00.000+0000"),
    
    # Edge case data
    ("txn_003", "loc_003", 0.0, -5.0, 0, "item_003", 0.01, 1.0, "loc_cd_003", "ref_003", "type_003", 50.0, 0.0, 20230321, "Y", "2024-03-21T00:00:00.000+0000", "2024-03-21T00:00:00.000+0000"),
    ("txn_004", "loc_004", 999999.99, 999999.99, 0, "item_004", 9999.99, 1.0, "loc_cd_004", "ref_004", "type_004", 50000.0, 0.0, 20240121, "Y", "2024-03-21T00:00:00.000+0000", "2024-03-21T00:00:00.000+0000"),
    
    # Error cases
    ("txn_005", "loc_005", -10.0, 0.0, 0, "item_005", -5.0, 1.0, "loc_cd_005", "ref_005", "type_005", -500.0, -5.0, 20240122, "N", "2024-03-21T00:00:00.000+0000", "2024-03-21T00:00:00.000+0000"),
    
    # NULL handling scenarios
    (None, "loc_006", None, 100.0, None, "item_006", None, None, "loc_cd_006", None, "type_006", None, None, None, None, None, None),
    
    # Special characters and multi-byte characters
    ("txn_007", "loc_漢字", 500.0, 480.0, 0, "アイテム_007", 50.0, 1.0, "loc_cd_汉字", "ref_漢字", "type_漢字", 200.0, 20.0, 20231010, "Y", "2024-03-21T00:00:00.000+0000", "2024-03-21T00:00:00.000+0000")
]

f_inv_movmnt_df = spark.createDataFrame(test_data, schema=f_inv_movmnt_schema)

# Display the data frame
f_inv_movmnt_df.show(truncate=False)
