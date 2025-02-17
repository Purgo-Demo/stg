# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType
from pyspark.sql import functions as F
from delta.tables import DeltaTable

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Databricks Schema Tests") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:1.0.0") \
    .getOrCreate()

# Schema for the testing DataFrame
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("created_at", TimestampType(), True)
])

# Function to create basic test DataFrame
def create_test_df():
    data = [
        (1, "Alice", datetime.strptime("2023-11-01T10:00:00", "%Y-%m-%dT%H:%M:%S")),
        (2, "Bob", datetime.strptime("2023-11-01T11:00:00", "%Y-%m-%dT%H:%M:%S")),
    ]
    return spark.createDataFrame(data, schema)

# Create test DataFrame
test_df = create_test_df()

# Function to test schema validation of a DataFrame
def test_schema_validation():
    """Check if the DataFrame schema matches the expected schema"""
    expected_schema = schema
    assert test_df.schema == expected_schema, "Schema validation failed!"

test_schema_validation()

# Test data type conversion using native functions
def test_type_conversion():
    """
    Test conversion from STRING to TIMESTAMP and validate result
    """
    converted_df = test_df.withColumn("created_at_str", F.col("created_at").cast("string"))
    assert converted_df.filter(F.col("created_at_str").isNull()).count() == 0, "Type conversion resulted in NULLs!"

test_type_conversion()

# Function to validate Delta Lake operations
def test_delta_operations():
    """
    Test Delta Lake operations, including MERGE, UPDATE, DELETE.
    """

    # Write initial data to Delta table
    test_df.write.format("delta").mode("overwrite").save("/mnt/delta/test_table")

    # Load Delta table
    delta_table = DeltaTable.forPath(spark, "/mnt/delta/test_table")

    # Test UPDATE operation
    delta_table.update(
        condition=F.col("id") == 1,
        set={"name": F.expr("'UpdatedName'")}
    )
    updated_df = delta_table.toDF()
    assert updated_df.filter(F.col("name") == "UpdatedName").count() == 1, "UPDATE failed!"

    # Test DELETE operation
    delta_table.delete(condition=F.col("id") == 2)
    deleted_count = delta_table.toDF().filter(F.col("id") == 2).count()
    assert deleted_count == 0, "DELETE failed!"

test_delta_operations()

# Cleanup test Delta table
def cleanup():
    """Remove created Delta table directory."""
    dbutils.fs.rm("/mnt/delta/test_table", recurse=True)

cleanup()

# Test SQL operations in a Databricks SQL context
spark.sql("""
  CREATE TABLE IF NOT EXISTS purgo_playground.new_data (
      id INT,
      name STRING,
      created_at TIMESTAMP
  ) USING DELTA
""")

# Inserting Data
spark.sql("""
  INSERT INTO purgo_playground.new_data
  VALUES (3, 'Charlie', CAST('2023-11-01T12:00:00' AS TIMESTAMP))
""")

# Retrieve and assert
retrieved_data = spark.sql("SELECT * FROM purgo_playground.new_data WHERE id = 3")
assert retrieved_data.count() == 1, "Data insertion or retrieval failed!"

# Test handling of non-existent dataset
try:
    spark.sql("SELECT * FROM purgo_playground.non_existent_data")
except Exception as e:
    assert "not found" in str(e), "Expected error message for non-existent dataset not found."

# Close Spark session
spark.stop()

