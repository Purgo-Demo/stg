from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType

# Initialize Spark session
spark = SparkSession.builder.appName("Databricks Test Data Generation").getOrCreate()

# Define schema for employees table including the new `lastdate` column
employees_schema = StructType([
    StructField("employee_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("lastdate", DateType(), True)  # New 'lastdate' field
])

# Define schema for customers table including the new `categoryGroup` column
customers_schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("categoryGroup", StringType(), True)  # New 'categoryGroup' field
])

# Generate test data for employees
employees_data = [
    # Valid scenarios
    (1, "John Doe", "2023-01-15"),
    (2, "Jane Smith", "2022-08-30"),
    # Boundary conditions
    (3, "Max Mustermann", None),  # NULL lastdate
    # Invalid scenarios
    (4, "Alice Wonderland", "2025-01-01"),  # Future date
    # Special characters
    (5, "Älîçé Bøb", "2023-03-22"),
    (6, "John\nDoe", "2023-04-15")
]

# Generate test data for customers
customers_data = [
    # Valid scenarios
    (1, "Acme Corp", "Premium"),
    (2, "Globex Inc", "Basic"),
    (3, "Soylent Corp", "Enterprise"),
    # Boundary conditions
    (4, "Initech", "Uncategorized"),  # Default value
    # Invalid scenarios
    (5, "Vandelay Industries", None),  # NULL value
    # Special characters
    (6, "Blüth Cömjëçt", "BV%çÖ"),  # Special chars in group
    (7, "Oscorp", "123456789012345678901234567890123456789012345678901")  # Length exceeding 50
]

# Create DataFrames
employees_df = spark.createDataFrame(employees_data, schema=employees_schema)
customers_df = spark.createDataFrame(customers_data, schema=customers_schema)

# Create or replace test tables
employees_df.write.mode('overwrite').saveAsTable("purgo_playground.employees_test")
customers_df.write.mode('overwrite').saveAsTable("purgo_playground.customers_test")

# Validate 'lastdate' column in employees (should not be in the future)
employees_df.filter(col("lastdate") > lit("2023-12-31")).show()

# Validate 'categoryGroup' column in customers (should not be NULL and length <= 50)
customers_df.filter(col("categoryGroup").isNull() | (col("categoryGroup").getItem(0).rlike("[^a-zA-Z0-9]") | col("categoryGroup").rlike(".{51,}"))).show()

