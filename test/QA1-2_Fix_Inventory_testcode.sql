-- Test SQL for validating schema changes and operations

-- Setup environment for tests
USE SCHEMA purgo_playground;

-- Test: Add lastdate column to employees table
ALTER TABLE employees ADD COLUMN lastdate DATE;

-- Assert: Check if column exists and has correct type
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'employees' AND column_name = 'lastdate'
AND data_type = 'DATE';

-- Test: Add categoryGroup column to customers table
ALTER TABLE customers ADD COLUMN categoryGroup VARCHAR(255);

-- Assert: Check if column exists and has correct type
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'customers' AND column_name = 'categoryGroup'
AND data_type = 'character varying';

-- Test: Insert data into employees and validate lastdate format
INSERT INTO employees (employee_id, first_name, last_name, lastdate) 
VALUES (7, 'Alice', 'Wonderland', '2022-02-28');

-- Assert: Check if date format is correct
SELECT employee_id 
FROM employees 
WHERE employee_id = 7 AND lastdate = '2022-02-28';

-- Test: Insert data into customers and validate categoryGroup length
INSERT INTO customers (customer_id, name, categoryGroup) 
VALUES (6, 'Mega Store', 'Retail');

-- Assert: Validate character length does not exceed 255
SELECT customer_id 
FROM customers 
WHERE customer_id = 6 AND LENGTH(categoryGroup) <= 255;

-- Test: Invalid data scenarios
-- Attempt to insert invalid date format
DO $$
BEGIN
    BEGIN
        INSERT INTO employees (employee_id, first_name, last_name, lastdate)
        VALUES (8, 'Bob', 'Marley', 'invalid-date');
    EXCEPTION
        WHEN others THEN
            RAISE NOTICE 'Error: Invalid date format for lastdate.';
    END;
END;
$$;


# PySpark Testing for Batch and Streaming

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, length
from pyspark.sql.types import StructType, StructField, StringType, DateType, LongType

# Initialize Spark session
spark = SparkSession.builder.appName("DatabricksTest").getOrCreate()

# Employees schema validation test
employees_schema_test = StructType([
    StructField("employee_id", LongType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("lastdate", DateType(), True),
])

# Data validation tests
def test_employees_schema(df):
    assert df.schema == employees_schema_test, "Schema does not match"

def test_lastdate_format(df):
    invalid_formats = df.filter(~col("lastdate").cast(DateType()).isNotNull())
    assert invalid_formats.count() == 0, "Found invalid date formats"

# Sample DataFrame creation
employees_df_test = spark.createDataFrame([
    (9, "Charlie", "Brown", None),
    (10, "Lucy", "Van Pelt", "2023-05-12"),
], schema=employees_schema_test)

# Run tests
test_employees_schema(employees_df_test)
test_lastdate_format(employees_df_test)

# Customers schema validation test
customers_schema_test = StructType([
    StructField("customer_id", LongType(), True),
    StructField("name", StringType(), True),
    StructField("categoryGroup", StringType(), True),
])

# Data validation tests
def test_customers_schema(df):
    assert df.schema == customers_schema_test, "Schema does not match"

def test_categoryGroup_length(df):
    invalid_lengths = df.filter(col("categoryGroup").isNotNull() & (length(col("categoryGroup")) > 255))
    assert invalid_lengths.count() == 0, "Found categoryGroup with invalid length"

# Sample DataFrame creation
customers_df_test = spark.createDataFrame([
    (6, "Global Supply", "Manufacturing"),
    (7, "Tech Innovators", "Innovation"*40),  # Overflow example
], schema=customers_schema_test)

# Run tests
test_customers_schema(customers_df_test)
test_categoryGroup_length(customers_df_test)

# Clean up
spark.stop()
