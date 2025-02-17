# Import necessary PySpark modules
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, TimestampType

# Create Spark session
spark = SparkSession.builder.appName("DatabricksTestDataGeneration").getOrCreate()

# Define schema for the sample sales data
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

# Generate test data
data = [
    # Happy path data
    (2001, 'Tepezza', 'C-201', '2024-10-01', '2024-10-03', 'Shipped', '2024-10-10', 1.0, 20, 'No', 'Box', 10, 500),
    (2003, 'Tepezza', 'C-203', '2024-10-04', '2024-10-05', 'Shipped', '2024-10-12', 1.0, 30, 'Yes', 'Box', 10, 600),
    
    # Edge case data
    (2004, 'Tepezza', 'C-204', '2024-10-05', '2024-10-06', 'Shipped', '2024-10-13', 2.0, 100, 'No', 'Carton', 50, 750),
    
    # Error scenarios
    # NULL Values for mandatory fields in 'Shipped' status
    (2002, 'Tepezza', 'C-202', '2024-10-02', None, 'Shipped', None, None, 15, 'No', 'Box', 5, 250),
    
    # Invalid demand and quantity range
    (2005, 'Tepezza', 'C-205', '2024-10-06', '2024-10-09', 'On Hold', None, None, 5, 'No', 'Carton', 60, 300),
    
    # Returning products without shipment date
    (2006, 'Tepezza', 'C-206', '2024-10-07', None, 'Shipped', '2024-10-14', 1.0, 25, 'Yes', 'Box', 10, 500),
    
    # Incorrect Unit_Shipment_Type for small quantities
    (2007, 'Tepezza', 'C-207', '2024-10-08', '2024-10-10', 'Shipped', '2024-10-15', 1.0, 20, 'No', 'Carton', 5, 150),
    
    # Special and multi-byte characters
    (2008, 'Τεπεζα', 'C-208', '2024-10-09', '2024-10-11', 'Shipped', '2024-10-16', 1.0, 20, 'No', 'Box', 10, 500),
    
    # Happy path with maximum boundaries
    (2009, 'Tepezza', 'C-209', '2024-10-01', '2024-10-03', 'Shipped', '2024-10-10', 1.0, 50, 'No', 'Carton', 50, 1000),
    
    # Range checks
    (2010, 'Tepezza', 'C-210', '2024-10-01', '2024-10-06', 'Shipped', '2024-10-12', 1.0, 10, 'No', 'Box', 1, 100)
]

# Create DataFrame
df = spark.createDataFrame(data, schema)

# Display the DataFrame to verify
df.show(truncate=False)


In this code, I've created a PySpark DataFrame with diverse test records addressing the various scenarios specified in your test data requirements. The code includes happy path scenarios, edge cases, error scenarios, NULL handling, and the use of special characters, while matching the specified schema for the sales data.