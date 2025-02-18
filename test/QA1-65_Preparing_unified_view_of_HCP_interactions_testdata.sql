-- Drop existing view if it exists
DROP VIEW IF EXISTS purgo_playground.hcp_unified_interaction_view;

-- Create a new view for unified HCP interactions
CREATE VIEW purgo_playground.hcp_unified_interaction_view AS
SELECT 
    hcp360.name,
    hcp360.email,
    hcp360.phone,
    interaction.interaction_date,
    interaction.topic,
    interaction.duration_mins,
    interaction.engagement_score,
    CASE WHEN interaction.engagement_score < 3 THEN 1 ELSE 0 END AS is_follow_up,
    -- Calculate follow_up_date only if is_follow_up is TRUE
    CASE WHEN interaction.engagement_score < 3 THEN DATE_ADD(interaction.interaction_date, 30) END AS follow_up_date
FROM 
    purgo_playground.customer_360_raw AS hcp360
INNER JOIN 
    purgo_playground.customer_hcp_interaction AS interaction
    ON (
        CASE 
            -- Join based on phone for In-person channel
            WHEN interaction.channel = 'In-person' THEN hcp360.phone = interaction.phone
            -- Join based on email for other channels
            ELSE hcp360.email = interaction.email
        END
    );

-- Generate test data using PySpark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, date_add
from pyspark.sql.types import StringType, StructType, StructField, TimestampType, BigIntType

# Initialize Spark session
spark = SparkSession.builder.appName("TestDataGeneration").getOrCreate()

# Define schema for customer_360_raw
schema_customer_360_raw = StructType([
    StructField("id", BigIntType(), True),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("company", StringType(), True),
    StructField("job_title", StringType(), True),
    StructField("address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("country", StringType(), True),
    StructField("industry", StringType(), True),
    StructField("account_manager", StringType(), True),
    StructField("creation_date", TimestampType(), True),
    StructField("last_interaction_date", TimestampType(), True),
    StructField("purchase_history", StringType(), True),
    StructField("notes", StringType(), True),
    StructField("zip", StringType(), True)
])

# Create test data for customer_360_raw
data_customer_360_raw = [
    (1, "John Doe", "john@example.com", "1234567890", "Company A", "Sales Manager", "123 Elm St", "New York", "NY", "USA", "Healthcare", "Alice Smith", "2022-01-01T00:00:00.000+0000", "2024-03-21T00:00:00.000+0000", "Purchase history A", "Some notes", "10001"),
    (2, "Jane Roe", "jane@example.com", "9876543210", "Company B", "Marketing Specialist", "456 Oak St", "Los Angeles", "CA", "USA", "Technology", "Bob Jones", "2022-01-15T00:00:00.000+0000", "2024-03-20T00:00:00.000+0000", "Purchase history B", "More notes", "90001"),
    # Include special characters and null values for testing
    (3, "Ana María", "ana@ejemplo.com", "1098765432", "Empresa C", "Analista", None, "Madrid", "M", None, "Finanzas", "Juan Pérez", "2022-07-01T00:00:00.000+0000", None, None, None, None)
]

# Create DataFrame for customer_360_raw
df_customer_360_raw = spark.createDataFrame(data_customer_360_raw, schema=schema_customer_360_raw)

# Define schema for customer_hcp_interaction
schema_customer_hcp_interaction = StructType([
    StructField("interaction_id", BigIntType(), True),
    StructField("hcp_name", StringType(), True),
    StructField("channel", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("interaction_date", TimestampType(), True),
    StructField("topic", StringType(), True),
    StructField("duration_mins", StringType(), True),
    StructField("notes", StringType(), True),
    StructField("engagement_score", BigIntType(), True)
])

# Create test data for customer_hcp_interaction
data_customer_hcp_interaction = [
    (1, "John Doe", "Email", "john@example.com", "1234567890", "2024-03-01T00:00:00.000+0000", "Product Discussion", "30", "Discussed new features", 3),
    (2, "Jane Roe", "In-person", "jane@example.com", "9876543210", "2024-03-02T00:00:00.000+0000", "Demo Presentation", "45", "Presentation at office", 2),
    # Edge case: Negative engagement score
    (3, "Ana María", "Webinar", "ana@ejemplo.com", "1098765432", "2024-03-03T00:00:00.000+0000", "Financial Overview", "60", "Detailed financial report", -1),
    # NULL scenario
    (4, None, None, None, None, None, None, None, None, None)
]

# Create DataFrame for customer_hcp_interaction
df_customer_hcp_interaction = spark.createDataFrame(data_customer_hcp_interaction, schema=schema_customer_hcp_interaction)

# Register DataFrames as temporary views for SQL queries
df_customer_360_raw.createOrReplaceTempView("customer_360_raw")
df_customer_hcp_interaction.createOrReplaceTempView("customer_hcp_interaction")

# Generate SQL query for data validation
query = """
SELECT 
    hcp360.name,
    hcp360.email,
    hcp360.phone,
    interaction.interaction_date,
    interaction.topic,
    interaction.duration_mins,
    interaction.engagement_score,
    CASE WHEN interaction.engagement_score < 3 THEN 1 ELSE 0 END AS is_follow_up,
    CASE WHEN interaction.engagement_score < 3 THEN DATE_ADD(interaction.interaction_date, 30) END AS follow_up_date
FROM customer_360_raw AS hcp360
INNER JOIN customer_hcp_interaction AS interaction
    ON (
        CASE 
            WHEN interaction.channel = 'In-person' THEN hcp360.phone = interaction.phone
            ELSE hcp360.email = interaction.email
        END
    )
"""

# Execute the query
df_result = spark.sql(query)

# Show the result for verification
df_result.show()
