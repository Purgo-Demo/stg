from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType, DateType, DoubleType

# Initialize Spark session
spark = SparkSession.builder.appName("TestDataGeneration").getOrCreate()

# Schema definition for employees table with "lastdate" as DATE
employees_schema = StructType([
    StructField("employee_id", LongType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("lastdate", DateType(), True),
])

# Generate test data for the employees table
employees_test_data = [
    # Happy path
    (1, "John", "Doe", None),
    (2, "Jane", "Smith", "2023-01-15"),

    # Edge case: leap year date
    (3, "Jim", "Beam", "2020-02-29"),
    
    # Edge case: near future date
    (4, "Amy", "Winehouse", "2024-03-21"),
    
    # Null handling
    (5, "Null", "User", None),

    # Error case: invalid date format (handled elsewhere, for example via exception handling)
    # Special characters and multi-byte characters
    (6, "Sp€c!al", "Chåräctèrs", "2022-12-31")
]

# Create DataFrame
employees_df = spark.createDataFrame(employees_test_data, schema=employees_schema)
employees_df.show()

# Schema definition for customers table with "categoryGroup" as STRING
customers_schema = StructType([
    StructField("customer_id", LongType(), True),
    StructField("name", StringType(), True),
    StructField("categoryGroup", StringType(), True),
])

# Generate test data for the customers table
customers_test_data = [
    # Happy path
    (1, "ABC Corp", "Tech"),
    (2, "XYZ Inc", None),

    # Edge case: Boundary length for categoryGroup
    (3, "Max Limit Industries", "X" * 255),

    # Error case: Invalid character length (handled elsewhere, for example via exception handling)
    # NULL handling
    (4, "Null Category", None),

    # Special characters and multi-byte characters
    (5, "国际组织", "International")
]

# Create DataFrame
customers_df = spark.createDataFrame(customers_test_data, schema=customers_schema)
customers_df.show()

# Clean up
spark.stop()
