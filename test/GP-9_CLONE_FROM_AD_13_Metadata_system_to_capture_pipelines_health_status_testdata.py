# Using PySpark for generating diverse test data and re-creating tables as per the schema
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, TimestampType

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Databricks Test Data Generation") \
    .getOrCreate()

# Define schemas
batch_schema = StructType([
    StructField("batch_id", IntegerType(), False),
    StructField("batch_name", StringType(), False),
    StructField("description", StringType(), True),
    StructField("active_status", StringType(), True)
])

batchrun_schema = StructType([
    StructField("batchrun_id", IntegerType(), False),
    StructField("batch_id", IntegerType(), False),
    StructField("execution_date", DateType(), False),
    StructField("status", StringType(), False)
])

job_schema = StructType([
    StructField("job_id", IntegerType(), False),
    StructField("job_name", StringType(), False),
    StructField("description", StringType(), True),
    StructField("job_type", StringType(), False),
    StructField("active_status", StringType(), True),
    StructField("batch_id", IntegerType(), False)
])

jobrun_schema = StructType([
    StructField("job_run_id", IntegerType(), False),
    StructField("job_id", IntegerType(), False),
    StructField("batchrun_id", IntegerType(), False),
    StructField("execution_date", DateType(), False),
    StructField("status", StringType(), False),
    StructField("log_message", StringType(), True)
])

jobarguments_schema = StructType([
    StructField("argument_id", IntegerType(), False),
    StructField("job_id", IntegerType(), False),
    StructField("argument_name", StringType(), False),
    StructField("argument_value", StringType(), False)
])

# Create dataframes
batch_data = [
    (1, "Batch A", "Initial Batch", "active"),
    (2, "Batch B", "Secondary Batch", "inactive"),
    # NULL handling
    (3, "Batch C", None, None),
    # Special characters
    (4, "Batch D", "Special char !@#$%", "active")
]

batchrun_data = [
    (101, 1, '2023-10-01', "success"),
    (102, 2, '2023-10-02', "failed"),
    (103, 1, '2024-03-21', None),  # NULL status for testing
    (104, 3, '2024-03-21', "out_of_range")  # Error case scenario
]

job_data = [
    (501, "Job One", "ETL Process", "ETL", "active", 1),
    (502, "Job Two", "Data Analysis", "Analysis", "inactive", 2),
    (503, "Invalid Batch ID", "Data Cleaning", "Cleaning", "active", 99),  # Invalid foreign key
    (504, "Special Job", None, "Special_Char_Job", "active\n\t\"'<>", 1)
]

jobrun_data = [
    (701, 501, 101, '2023-10-01', "success", "Execution completed without errors"),
    (702, 502, 102, '2023-10-02', "failed", "Failure due to XYZ reasons"),
    (703, 501, 101, '2024-03-21', None, None),  # NULL test case
    (704, 503, 104, '2024-03-21', "execution_error", "Invalid scenario")  # Error case scenario
]

jobarguments_data = [
    (801, 501, "input_path", "/data/input/"),
    (802, 502, "output_format", "csv"),
    (803, 504, "special_arg", "\"Special_Value\"<>\n\t")  # Special character handling
]

# Create DataFrames
df_batch = spark.createDataFrame(batch_data, batch_schema)
df_batchrun = spark.createDataFrame(batchrun_data, batchrun_schema)
df_job = spark.createDataFrame(job_data, job_schema)
df_jobrun = spark.createDataFrame(jobrun_data, jobrun_schema)
df_jobarguments = spark.createDataFrame(jobarguments_data, jobarguments_schema)

# Use Spark to manage the tables in the purgo_playground schema (Unity Catalog)
spark.sql("CREATE SCHEMA IF NOT EXISTS purgo_playground")

# Overwrite tables with test data
df_batch.write.mode("overwrite").saveAsTable("purgo_playground.batch")
df_batchrun.write.mode("overwrite").saveAsTable("purgo_playground.batchrun")
df_job.write.mode("overwrite").saveAsTable("purgo_playground.job")
df_jobrun.write.mode("overwrite").saveAsTable("purgo_playground.jobrun")
df_jobarguments.write.mode("overwrite").saveAsTable("purgo_playground.jobarguments")

spark.stop()
