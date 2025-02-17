# Import necessary libraries
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, DoubleType, DateType, StructType, StructField

# Define schema for the sales data
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

# Load sales data into DataFrame
# Replace 'path_to_sales_data.csv' with the actual path
sales_df = spark.read.csv('path_to_sales_data.csv', header=True, schema=schema)

# Function to check for mandatory fields
def check_mandatory_fields(df):
    mandatory_check = df.filter((df.Shipment_Status == "Shipped") & 
                                (F.col("Order_ID").isNull() | 
                                 F.col("Product_Name").isNull() | 
                                 F.col("Customer_ID").isNull() | 
                                 F.col("Shipment_Date").isNull() | 
                                 F.col("Quantity").isNull()))
    return mandatory_check

# Function to check date consistency
def check_date_consistency(df):
    date_check = df.filter((F.col("Order_Date") >= F.col("Shipment_Date")) |
                           (F.col("Shipment_Date") >= F.col("Delivery_Scheduled")))
    return date_check

# Function to perform range checks
def check_range(df):
    demand_range = (10, 100)
    quantity_range = (1, 50)
    range_check = df.filter((F.col("Demand") < demand_range[0]) | 
                            (F.col("Demand") > demand_range[1]) |
                            (F.col("Quantity") < quantity_range[0]) | 
                            (F.col("Quantity") > quantity_range[1]))
    return range_check

# Function to check status consistency
def check_status_consistency(df):
    status_check = df.filter((df.Shipment_Status == "Shipped") & 
                             (F.col("Shipment_Date").isNull() | 
                              F.col("Delivery_Scheduled").isNull() | 
                              F.col("Line_Number").isNull()) |
                             (df.Shipment_Status == "On Hold") & 
                             (F.col("Shipment_Date").isNotNull() | 
                              F.col("Delivery_Scheduled").isNotNull() | 
                              F.col("Line_Number").isNotNull()))
    return status_check

# Function to ensure unique Order_ID
def check_unique_identifier(df):
    duplicate_check = df.groupBy("Order_ID").count().filter("count > 1")
    return duplicate_check

# Function to validate returned product process
def check_returned_product(df):
    return_check = df.filter((df.Is_Returned == "Yes") & df.Shipment_Date.isNull())
    return return_check

# Function to validate shipment type
def check_unit_shipment(df):
    shipment_check = df.filter((df.Unit_Shipment_Type == "Carton") & (df.Quantity < 10))
    return shipment_check

# Function to ensure price accuracy
def check_price_accuracy(df):
    price_check = df.filter(df.Total_Price != (df.Quantity * F.lit(50)))  # Assume Unit_Price is 50
    return price_check

# Run all checks and collect results
mandatory_fields_result = check_mandatory_fields(sales_df)
date_consistency_result = check_date_consistency(sales_df)
range_check_result = check_range(sales_df)
status_consistency_result = check_status_consistency(sales_df)
unique_identifier_result = check_unique_identifier(sales_df)
returned_product_result = check_returned_product(sales_df)
unit_shipment_result = check_unit_shipment(sales_df)
price_accuracy_result = check_price_accuracy(sales_df)

# Store results in a Delta table for reporting
validated_results = sales_df.withColumn("Mandatory_Fields_Check", F.lit("")) \
    .withColumn("Date_Consistency_Check", F.lit("")) \
    .withColumn("Range_Check", F.lit("")) \
    .withColumn("Status_Consistency_Check", F.lit("")) \
    .withColumn("Unique_Identifier_Check", F.lit("")) \
    .withColumn("Returned_Product_Validation", F.lit("")) \
    .withColumn("Correct_Unit_Shipment_Type", F.lit("")) \
    .withColumn("Price_Calculation_Accuracy", F.lit(""))

validated_results.select(
    "Order_ID",
    F.when(F.expr("Mandatory_Fields_Check IS NOT NULL"), "Fail").otherwise("Pass").alias("Mandatory_Fields_Check"),
    F.when(F.expr("Date_Consistency_Check IS NOT NULL"), "Fail").otherwise("Pass").alias("Date_Consistency_Check"),
    F.when(F.expr("Range_Check IS NOT NULL"), "Fail").otherwise("Pass").alias("Range_Check"),
    F.when(F.expr("Status_Consistency_Check IS NOT NULL"), "Fail").otherwise("Pass").alias("Status_Consistency_Check"),
    F.when(F.expr("Unique_Identifier_Check IS NOT NULL"), "Fail").otherwise("Pass").alias("Unique_Identifier_Check"),
    F.when(F.expr("Returned_Product_Validation IS NOT NULL"), "Fail").otherwise("Pass").alias("Returned_Product_Validation"),
    F.when(F.expr("Correct_Unit_Shipment_Type IS NOT NULL"), "Fail").otherwise("Pass").alias("Correct_Unit_Shipment_Type"),
    F.when(F.expr("Price_Calculation_Accuracy IS NOT NULL"), "Fail").otherwise("Pass").alias("Price_Calculation_Accuracy")
).write.format("delta").mode("overwrite").saveAsTable("purgo_playground.dq_reports")
