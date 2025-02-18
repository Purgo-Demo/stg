from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType, DoubleType, DecimalType, ArrayType, MapType

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Test Data Generation for Databricks") \
    .getOrCreate()

# Define the schema for the test data
schema = StructType([
    StructField("id", LongType(), True),
    StructField("name", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("amount", DecimalType(10, 2), True),
    StructField("multi_byte", StringType(), True),
    StructField("special_chars", StringType(), True),
    StructField("data_array", ArrayType(DoubleType()), True),
    StructField("data_map", MapType(StringType(), StringType()), True)
])

# Create test data
data = [
    # Happy path test data
    (1, "Alice", "2024-03-21T15:30:00.000+0000", 1234.56, "こんにちは", "@lice&!", [12.34, 56.78], {"key1": "value1", "key2": "value2"}),
    (2, "Bob", "2024-03-22T10:00:00.000+0000", 7890.12, "你好", "B*b#12", [90.12, 34.56], {"keyA": "valueA"}),

    # Edge cases
    (3, None, "1970-01-01T00:00:00.000+0000", 0, "", "", [], {}),     # Null and empty values
    (4, "MaxValue", "9999-12-31T23:59:59.999+0000", 9999999999.99, "A", "%", [999.99, 888.88], {"max": "value"}), # Max values

    # Error cases
    (5, "NegativeAmount", "2024-03-21T15:30:00.000+0000", -1000.00, "안녕하세요", "<Error>", [1.1], {"negative": "value"}), # Negative amount

    # Special characters and multi-byte characters
    (6, "SpecialChars", "2024-03-21T12:00:00.000+0000", 1234.56, "🚀🌟", "!@#$%^&*()", [77.11], {"emoji": "🚀"}),
    (7, "MultiByte", "2024-03-21T14:00:00.000+0000", 4321.00, "感谢", "T^&*()<>?", [111.22], {"cn": "感谢"}),

    # More diverse records
    (8, "Eve", "2024-03-23T11:30:00.000+0000", 4321.00, "Спасибо", "Ev#e07", [222.33], {"rus": "Спасибо"}),
    (9, "Charlie", "2024-03-24T16:45:00.000+0000", 6543.21, "Gracias", "Ch#rl1e", [333.44], {"es": "Gracias"}),
    (10, "Dana", "2024-03-25T08:15:00.000+0000", 9876.54, "Merci", "Da!na*", [444.55], {"fr": "Merci"}),

    # NULL handling in different fields
    (11, "NoTimestamp", None, 123.45, "Null", "Test", [555.66], {"missing": "timestamp"}),
    (12, "NoAmount", "2024-03-21T10:10:00.000+0000", None, "Null", "Test", [0.0], {"missing": "amount"})
]

# Convert to DataFrame
test_df = spark.createDataFrame(data, schema)

# Show the generated test DataFrame
test_df.show(truncate=False)
