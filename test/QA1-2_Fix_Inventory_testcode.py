from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr
from pyspark.sql.types import StructType, StructField, StringType, LongType, DateType
import pytest

# Initialize Spark session
spark = SparkSession.builder \
    .appName("DatabricksTesting") \
    .config("spark.databricks.delta.formatCheck.enabled", "false") \
    .getOrCreate()

# Test Setup for employees table with "lastdate" as DATE
def create_employees_table():
    schema = StructType([
        StructField("employee_id", LongType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("lastdate", DateType(), True),
    ])

    data = [
        (1, "John", "Doe", None),
        (2, "Jane", "Smith", "2023-01-15"),
        (3, "Jim", "Beam", "2020-02-29"),
        (4, "Amy", "Winehouse", "2024-03-21"),
        (5, "Null", "User", None),
        (6, "Sp€c!al", "Chåräctèrs", "2022-12-31")
    ]

    return spark.createDataFrame(data, schema=schema)

# Test Setup for customers table with "categoryGroup" as STRING
def create_customers_table():
    schema = StructType([
        StructField("customer_id", LongType(), True),
        StructField("name", StringType(), True),
        StructField("categoryGroup", StringType(), True),
    ])

    data = [
        (1, "ABC Corp", "Tech"),
        (2, "XYZ Inc", None),
        (3, "Max Limit Industries", "X" * 255),
        (4, "Null Category", None),
        (5, "国际组织", "International")
    ]

    return spark.createDataFrame(data, schema=schema)

@pytest.fixture(scope="module")
def test_data():
    return {"employees": create_employees_table(), "customers": create_customers_table()}

# Unit Test for Adding Columns
def test_add_lastdate_column(test_data):
    df = test_data["employees"]
    
    # Test the datatypes
    assert df.schema["lastdate"].dataType == DateType()
    
    # Test correct NULL handling
    null_check = df.filter(col("lastdate").isNull()).count()
    assert null_check == 2  # Expecting two NULL values as per test data

def test_lastdate_format_error(test_data):
    # This would typically be an exception handling test
    with pytest.raises(Exception):
        df = test_data["employees"].withColumn("lastdate_wrong_format", expr("cast('12-31-2023' as date)"))

def test_add_categoryGroup_column(test_data):
    df = test_data["customers"]
    
    # Test the datatypes
    assert df.schema["categoryGroup"].dataType == StringType()
    
    # Test max character length
    max_length_check = df.filter(col("categoryGroup").isNotNull()).withColumn("char_length", expr("length(categoryGroup)")) \
                        .where("char_length > 255").count()
    assert max_length_check == 0  # No values should exceed 255 characters

# Integration Test for backfilling data
def test_backfilling_lastdate():
    df = create_employees_table()
    updated_df = df.fillna({"lastdate": None})
    
    null_count = updated_df.filter(col("lastdate").isNull()).count()
    assert null_count == 2

def test_backfilling_categoryGroup():
    df = create_customers_table()
    updated_df = df.fillna({"categoryGroup": None})
    
    null_count = updated_df.filter(col("categoryGroup").isNull()).count()
    assert null_count == 2

# Cleanup
def test_cleanup(spark_session):
    # Cleanup operations like drop tables
    pass

if __name__ == "__main__":
    pytest.main([__file__])
    
# Stop Spark session at the end of tests
spark.stop()
