# Import necessary PySpark modules
# Ensure PySpark is available in the environment
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType

# Create Spark session
spark = SparkSession.builder.appName("TepezzaSalesDataQualityChecks").getOrCreate()

# Define the schema for the sales data based on the provided description
schema = StructType([
    StructField("Order_ID", IntegerType(), True),
    StructField("Product_Name", StringType(), True),
    StructField("Customer_ID", StringType(), True),
    StructField("Order_Date", DateType(), True),
    StructField("Shipment_Date", DateType(), True),
    StructField("Shipment_Status", StringType(), True),
    StructField("Delivery_Scheduled", DateType(), True),
    StructField("Line_Number", DoubleType(), True),
    StructField("Demand", IntegerType(), True),
    StructField("Is_Returned", StringType(), True),
    StructField("Unit_Shipment_Type", StringType(), True),
    StructField("Quantity", IntegerType(), True),
    StructField("Total_Price", IntegerType(), True)
])

# Load the sales data into a DataFrame
# Please replace 'path_to_sales_data' with the actual path to the sales data
sales_df = spark.read.csv('path_to_sales_data.csv', header=True, schema=schema)

# Function to add Data Quality checks for mandatory fields
def check_mandatory_fields(df):
    # Check for NULL in mandatory fields when Shipment_Status is 'Shipped'
    mandatory_fields_check = df.filter((df.Shipment_Status == "Shipped") & 
                                       (F.col("Order_ID").isNull() | 
                                        F.col("Product_Name").isNull() | 
                                        F.col("Customer_ID").isNull() | 
                                        F.col("Shipment_Date").isNull() | 
                                        F.col("Quantity").isNull()))
    return mandatory_fields_check

# Function to perform Date Consistency Check
def check_date_consistency(df):
    date_consistency_check = df.filter((F.col("Order_Date") >= F.col("Shipment_Date")) |
                                       (F.col("Shipment_Date") >= F.col("Delivery_Scheduled")))
    return date_consistency_check

# Function to perform Range Check for Demand and Quantity
def check_range(df):
    # Define acceptable ranges for Demand and Quantity
    demand_range = (10, 100)
    quantity_range = (1, 50)
    
    range_check = df.filter((F.col("Demand") < demand_range[0]) | (F.col("Demand") > demand_range[1]) |
                            (F.col("Quantity") < quantity_range[0]) | (F.col("Quantity") > quantity_range[1]))
    return range_check

# Function to perform Status Consistency Check
def check_status_consistency(df):
    # Check for missing fields based on Shipment_Status
    status_consistency_check = df.filter((df.Shipment_Status == "Shipped") & 
                                         (F.col("Shipment_Date").isNull() | 
                                          F.col("Delivery_Scheduled").isNull() | 
                                          F.col("Line_Number").isNull()) |
                                         (df.Shipment_Status == "On Hold") & 
                                         (F.col("Shipment_Date").isNotNull() | 
                                          F.col("Delivery_Scheduled").isNotNull() | 
                                          F.col("Line_Number").isNotNull()))
    return status_consistency_check

# Function to perform Unique Identifier Check
def check_unique_identifier(df):
    unique_identifier_check = df.groupBy("Order_ID").count().filter("count > 1")
    return unique_identifier_check

# Function to check Returned Product Validation
def check_returned_product(df):
    returned_product_check = df.filter((df.Is_Returned == "Yes") & df.Shipment_Date.isNull())
    return returned_product_check

# Function to check Correct Unit Shipment Type
def check_unit_shipment(df):
    unit_shipment_check = df.filter((df.Unit_Shipment_Type == "Carton") & (df.Quantity < 10))
    return unit_shipment_check

# Function to check Price Calculation Accuracy
def check_price_accuracy(df):
    price_accuracy_check = df.filter(df.Total_Price != (df.Quantity * F.lit(50)))  # Assuming Unit Price is 50
    return price_accuracy_check

# Apply all data quality checks and collect results
mandatory_fields_result = check_mandatory_fields(sales_df)
date_consistency_result = check_date_consistency(sales_df)
range_check_result = check_range(sales_df)
status_consistency_result = check_status_consistency(sales_df)
unique_identifier_result = check_unique_identifier(sales_df)
returned_product_result = check_returned_product(sales_df)
unit_shipment_result = check_unit_shipment(sales_df)
price_accuracy_result = check_price_accuracy(sales_df)

# Log or display results for validation review
print("Mandatory fields check:")
mandatory_fields_result.show()

print("Date consistency check:")
date_consistency_result.show()

print("Range check for Demand and Quantity:")
range_check_result.show()

print("Status consistency check:")
status_consistency_result.show()

print("Unique identifier check:")
unique_identifier_result.show()

print("Returned product validation check:")
returned_product_result.show()

print("Correct unit shipment type check:")
unit_shipment_result.show()

print("Price calculation accuracy check:")
price_accuracy_check.show()

# Note: For real-world use, store results in a delta table or send to a report/dashboard

# Stop the Spark session to free resources
spark.stop()

