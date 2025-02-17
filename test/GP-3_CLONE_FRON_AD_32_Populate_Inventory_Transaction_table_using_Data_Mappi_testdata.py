from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType
from datetime import datetime
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("Test Data Generation").getOrCreate()

# Define schema for "purgo_playground" table
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("created_at", TimestampType(), True)
])

# Function to create test data
def create_test_data():
    data = [
        # Happy path test data
        (1, "Alice", datetime.strptime("2024-03-21T10:00:00.000+0000", "%Y-%m-%dT%H:%M:%S.%f%z")),
        (2, "Bob", datetime.strptime("2024-03-21T11:00:00.000+0000", "%Y-%m-%dT%H:%M:%S.%f%z")),
        
        # Edge cases
        (0, "", datetime.strptime("1970-01-01T00:00:00.000+0000", "%Y-%m-%dT%H:%M:%S.%f%z")), # Minimum timestamp
        (2147483647, "MaxIntName", datetime.strptime("2038-01-19T03:14:07.000+0000", "%Y-%m-%dT%H:%M:%S.%f%z")), # Max int edge case

        # NULL handling
        (None, "NullID", datetime.strptime("2024-03-21T12:00:00.000+0000", "%Y-%m-%dT%H:%M:%S.%f%z")),
        (3, None, datetime.strptime("2024-03-21T13:00:00.000+0000", "%Y-%m-%dT%H:%M:%S.%f%z")),
        (4, "NullCreatedAt", None),

        # Special characters and multi-byte characters
        (5, "Special!@#$%", datetime.strptime("2024-03-21T14:00:00.000+0000", "%Y-%m-%dT%H:%M:%S.%f%z")),
        (6, "多字节字符", datetime.strptime("2024-03-21T15:00:00.000+0000", "%Y-%m-%dT%H:%M:%S.%f%z")),

        # Error cases
        (-1, "NegativeID", datetime.strptime("2024-03-21T16:00:00.000+0000", "%Y-%m-%dT%H:%M:%S.%f%z")), # Invalid ID
        (7, "OutOfRange", datetime.strptime("2040-12-31T23:59:59.000+0000", "%Y-%m-%dT%H:%M:%S.%f%z")), # Out-of-range timestamp
    ]

    return spark.createDataFrame(data, schema)

# Create DataFrame with test data
df_test_data = create_test_data()

# Show the test data
df_test_data.show(truncate=False)

# Write the test data to the target table in the Unity Catalog schema
df_test_data.write.mode('overwrite').saveAsTable("purgo_playground.new_data")
