-- /* SQL View Creation */

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
    -- Calculate is_follow_up based on engagement_score
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

-- Test SQL Query
-- Select data from the newly created view
SELECT * FROM purgo_playground.hcp_unified_interaction_view;



# /* PySpark Test Code Section */

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_add
from pyspark.sql.types import StringType, TimestampType, StructType, StructField, BigIntType

# Initialize Spark session
spark = SparkSession.builder.appName("HCPUnifiedInteractionTest").getOrCreate()

# Define schema for customer_360_raw
schema_customer_360_raw = StructType([
    StructField("id", BigIntType(), True),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True)
])

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
    StructField("engagement_score", BigIntType(), True)
])

# Sample data for testing
data_customer_360_raw = [
    (1, "John Doe", "john@example.com", "1234567890"),
    (2, "Jane Roe", "jane@example.com", "9876543210")
]

data_customer_hcp_interaction = [
    (1, "John Doe", "Email", "john@example.com", "1234567890", "2024-03-01 00:00:00", "Product Discussion", "30", 3),
    (2, "Jane Roe", "In-person", "jane@example.com", "9876543210", "2024-03-02 00:00:00", "Demo Presentation", "45", 2)
]

# Create DataFrames
df_customer_360_raw = spark.createDataFrame(data_customer_360_raw, schema=schema_customer_360_raw)
df_customer_hcp_interaction = spark.createDataFrame(data_customer_hcp_interaction, schema=schema_customer_hcp_interaction)

# Register DataFrames as temporary views for SQL queries
df_customer_360_raw.createOrReplaceTempView("customer_360_raw")
df_customer_hcp_interaction.createOrReplaceTempView("customer_hcp_interaction")

# Execute SQL to validate data types and computation in the view
result_df = spark.sql("""
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
""")

result_df.show()

# Assert to validate schema
assert result_df.schema == StructType([
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("interaction_date", TimestampType(), True),
    StructField("topic", StringType(), True),
    StructField("duration_mins", StringType(), True),
    StructField("engagement_score", BigIntType(), True),
    StructField("is_follow_up", BigIntType(), True),
    StructField("follow_up_date", TimestampType(), True)
]), "Schema validation failed!"

# Assert to ensure data correctness
# Note: Casting dates/timestamps to string for comparison since Spark equality comparisons can be tricky with timestamps
expected_data = [
    ("John Doe", "john@example.com", "1234567890", "2024-03-01 00:00:00", "Product Discussion", "30", 3, 0, None),
    ("Jane Roe", "jane@example.com", "9876543210", "2024-03-02 00:00:00", "Demo Presentation", "45", 2, 1, "2024-04-01 00:00:00")
]

for row in expected_data:
    assert row in [(r['name'], r['email'], r['phone'], r['interaction_date'].isoformat(), r['topic'], r['duration_mins'], r['engagement_score'], r['is_follow_up'], r['follow_up_date'].isoformat() if r['follow_up_date'] else None) for r in result_df.collect()], f"Row {row} not found in view!"

# Shutdown Spark session
spark.stop()
