from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, sum, concat

spark = SparkSession.builder \
    .appName("Test Data Generation for Databricks") \
    .getOrCreate()

# Generate test data for purgo_playground.f_inv_movmnt table
data = [
    # Happy path with valid DNSA flag and financial quantities
    ("txn1", "loc1", 100.0, 120.0, 0, "item1", 10.0, 1.0, "plant1", "ref1", "type1", 1000.0, 100.0, 0, "Y", "2024-03-21T00:00:00.000+0000", "2024-03-21T01:00:00.000+0000"),
    ("txn2", "loc2", 150.0, 140.0, 0, "item2", 15.0, 1.5, "plant2", "ref2", "type2", 800.0, 50.0, 0, "Y", "2024-03-21T00:00:00.000+0000", "2024-03-21T01:00:00.000+0000"),
    # Edge cases with boundary values
    ("txn3", "loc3", 0.0, 0.0, 0, "item3", 0.0, 0.0, "plant3", "ref3", "type3", 0.0, 0.0, 0, "Y", "2024-03-21T00:00:00.000+0000", "2024-03-21T01:00:00.000+0000"),
    ("txn4", "loc4", 9999999999.99, 9999999999.99, 0, "item4", 9999999999.99, 99.9, "plant4", "ref4", "type4", 999999.99, 0.0, 0, "N", "2024-03-21T00:00:00.000+0000", "2024-03-21T01:00:00.000+0000"),
    # Error cases with invalid combinations
    ("txn5", "loc5", -100.0, -120.0, 0, "item5", -10.0, -1.0, "plant5", "ref5", "type5", -1000.0, -100.0, 0, "Y", "2024-03-21T00:00:00.000+0000", "2024-03-21T01:00:00.000+0000"),
    # NULL handling scenarios
    (None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None),
    # Special characters
    ("txn6", "loc@!", 150.0, 160.0, 0, "item!@#", 20.0, 2.0, "plant@#", "ref!@", "type!@", 900.0, 30.0, 0, "Y", "2024-03-21T00:00:00.000+0000", "2024-03-21T01:00:00.000+0000"),
    # Multibyte characters
    ("txn7", "loc7", 200.0, 210.0, 0, "item\u2B50", 25.0, 2.5, "plant\u2764", "ref\u2B50", "type\u2B50", 1100.0, 110.0, 0, "N", "2024-03-21T00:00:00.000+0000", "2024-03-21T01:00:00.000+0000")
]

columns = ["txn_id", "inv_loc", "financial_qty", "net_qty", "expired_qt", "item_nbr", 
           "unit_cost", "um_rate", "plant_loc_cd", "inv_stock_reference", "stock_type", 
           "qty_on_hand", "qty_shipped", "cancel_dt", "flag_active", "crt_dt", "updt_dt"]

df = spark.createDataFrame(data, schema=columns)

df.show(truncate=False)
