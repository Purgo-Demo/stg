# Import necessary libraries for PySpark and Delta Lake
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType
from pyspark.sql import functions as F
from delta.tables import DeltaTable

# Define schema for the datasets
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("created_at", TimestampType(), True)
])

# Function to create test DataFrame
def create_test_df():
    """Create DataFrame with initial data"""
    data = [
        (1, "Alice", F.to_timestamp("2023-11-01T10:00:00")),
        (2, "Bob", F.to_timestamp("2023-11-01T11:00:00"))
    ]
    return spark.createDataFrame(data, schema)

# Create test DataFrame
test_df = create_test_df()

# Write initial data to Delta table
test_df.write.format("delta").mode("overwrite").saveAsTable("purgo_playground.new_data")

# Test data insertion with proper error handling for duplicates
def test_data_insertion():
    """Test handling of duplicate insertions"""
    try:
        # Attempt to insert a duplicate row
        test_df = create_test_df()  # Assuming df is refreshed or reloaded
        test_df.write.format("delta").mode("append").saveAsTable("purgo_playground.new_data")
        raise AssertionError("Duplicate entry allowed!")

    except Exception as e:
        assert "already exists" in str(e), "Expected duplicate entry error not found."

test_data_insertion()

# Test update operation
def test_update_data():
    """Test update operation on Delta table"""
    delta_table = DeltaTable.forName(spark, "purgo_playground.new_data")
    
    # Update a specific row
    delta_table.update(
        condition=F.col("id") == 1,
        set={"name": F.expr("'UpdatedName'")}
    )
    
    updated_df = delta_table.toDF()
    assert updated_df.filter(F.col("name") == "UpdatedName").count() == 1, "Update failed!"

test_update_data()

# Test deletion operation
def test_delete_data():
    """Test delete operation on Delta table"""
    delta_table = DeltaTable.forName(spark, "purgo_playground.new_data")
    
    # Delete a specific row
    delta_table.delete(condition=F.col("id") == 2)

    deleted_df = delta_table.toDF()
    assert deleted_df.filter(F.col("id") == 2).count() == 0, "Delete failed!"

test_delete_data()

# Test data retrieval and validation
def test_data_retrieval():
    """Test data retrieval from Delta table"""
    result_df = spark.sql("SELECT * FROM purgo_playground.new_data WHERE id = 1")
    assert result_df.count() == 1, "Data retrieval failed!"

test_data_retrieval()

# Cleanup Delta table after tests
def cleanup():
    """Remove all created Delta table data"""
    spark.sql("DROP TABLE IF EXISTS purgo_playground.new_data")

cleanup()
