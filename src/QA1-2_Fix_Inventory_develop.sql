-- Use the Unity Catalog
USE SCHEMA purgo_playground;

-- Add lastdate column to employees table
ALTER TABLE employees 
ADD COLUMN lastdate DATE;

-- Set default value for the new column and backfill existing records
UPDATE employees 
SET lastdate = NULL;

-- Add categoryGroup column to customers table
ALTER TABLE customers 
ADD COLUMN categoryGroup STRING;

-- Set default value for the new column and backfill existing records
UPDATE customers 
SET categoryGroup = 'Uncategorized';

-- Test: Verify columns
SELECT column_name, data_type 
FROM information_schema.columns 
WHERE table_name IN ('employees', 'customers') 
AND column_name IN ('lastdate', 'categoryGroup');

-- Add data validation test for employees
INSERT INTO employees (employee_id, first_name, last_name, lastdate) 
VALUES (7, 'Alice', 'Wonderland', '2022-02-28');

SELECT employee_id 
FROM employees 
WHERE employee_id = 7 
AND lastdate = '2022-02-28';

-- Add data validation test for customers
INSERT INTO customers (customer_id, name, categoryGroup) 
VALUES (6, 'Mega Store', 'Retail');

SELECT customer_id 
FROM customers 
WHERE customer_id = 6 
AND LENGTH(categoryGroup) <= 50;




from pyspark.sql import SparkSession
from pyspark.sql.functions import col, length
from pyspark.sql.types import StructType, StructField, StringType, DateType, LongType
from datetime import datetime

# Initialize Spark session
spark = SparkSession.builder \
    .appName("DatabricksImplementation") \
    .enableHiveSupport() \
    .getOrCreate()

# Schema Definitions
employees_schema = StructType([
    StructField("employee_id", LongType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("lastdate", DateType(), True),
])

customers_schema = StructType([
    StructField("customer_id", LongType(), True),
    StructField("name", StringType(), True),
    StructField("categoryGroup", StringType(), True),
])

# Example DataFrames
employees_df = spark.createDataFrame([
    (1, "John", "Doe", None),
    (2, "Jane", "Smith", datetime.strptime("2022-05-10", "%Y-%m-%d")),
], schema=employees_schema)

customers_df = spark.createDataFrame([
    (1, "Acme Corp", "Business"),
    (2, "Status Inc", "Enterprise"),
], schema=customers_schema)

# Validate Employees DataFrame
def validate_employees(df):
    # This assumes correct casting of date; real validation would be more robust
    invalid_dates = df.filter(col("lastdate").isNull())
    assert invalid_dates.count() == 0, "Invalid lastdate format found"

validate_employees(employees_df)

# Validate Customers DataFrame
def validate_customers(df):
    invalid_category_length = df.filter(length(col("categoryGroup")) > 50)
    assert invalid_category_length.count() == 0, "categoryGroup length exceeded 50 characters"

validate_customers(customers_df)

# Register DataFrames as Temporary Views for SQL access
employees_df.createOrReplaceTempView("employees_view")
customers_df.createOrReplaceTempView("customers_view")

# Example SQL query using registered views
spark.sql("SELECT * FROM employees_view WHERE lastdate IS NOT NULL").show()

# Ensure cleanup
spark.catalog.dropTempView("employees_view")
spark.catalog.dropTempView("customers_view")

spark.stop()
