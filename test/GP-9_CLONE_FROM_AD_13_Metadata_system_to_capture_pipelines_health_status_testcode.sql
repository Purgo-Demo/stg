/* Drop and Recreate Tables in purgo_playground schema */
-- Ensure schema exists
CREATE SCHEMA IF NOT EXISTS purgo_playground;

-- Drop existing tables if they exist
DROP TABLE IF EXISTS purgo_playground.batch;
DROP TABLE IF EXISTS purgo_playground.batchrun;
DROP TABLE IF EXISTS purgo_playground.job;
DROP TABLE IF EXISTS purgo_playground.jobrun;
DROP TABLE IF EXISTS purgo_playground.jobarguments;

-- Create Batch Table
CREATE TABLE purgo_playground.batch (
  batch_id INT NOT NULL,
  batch_name STRING NOT NULL,
  description STRING,
  active_status STRING
);

-- Create BatchRun Table
CREATE TABLE purgo_playground.batchrun (
  batchrun_id INT NOT NULL,
  batch_id INT,
  execution_date DATE NOT NULL,
  status STRING
);

-- Create Job Table
CREATE TABLE purgo_playground.job (
  job_id INT NOT NULL,
  job_name STRING NOT NULL,
  description STRING,
  job_type STRING NOT NULL,
  active_status STRING,
  batch_id INT
);

-- Create JobRun Table
CREATE TABLE purgo_playground.jobrun (
  job_run_id INT NOT NULL,
  job_id INT,
  batchrun_id INT,
  execution_date DATE NOT NULL,
  status STRING,
  log_message STRING
);

-- Create JobArguments Table
CREATE TABLE purgo_playground.jobarguments (
  argument_id INT NOT NULL,
  job_id INT,
  argument_name STRING,
  argument_value STRING
);

/* Since Databricks SQL does not support directly adding constraints with ALTER TABLE, these need to be enforced through application-level logic or data engineering frameworks */

/* Sample Data Insertion with Sample Assertions */
-- Insert into Batch Table
INSERT INTO purgo_playground.batch (batch_id, batch_name, description, active_status) VALUES
(1, 'Batch A', 'Initial Batch', 'active'),
(2, 'Batch B', 'Secondary Batch', 'inactive');

-- Insert into BatchRun Table
INSERT INTO purgo_playground.batchrun (batchrun_id, batch_id, execution_date, status) VALUES
(101, 1, DATE('2023-10-01'), 'success'),
(102, 2, DATE('2023-10-02'), 'failed');

-- Insert into Job Table
INSERT INTO purgo_playground.job (job_id, job_name, description, job_type, active_status, batch_id) VALUES
(501, 'Job One', 'ETL Process', 'ETL', 'active', 1),
(502, 'Job Two', 'Data Analysis', 'Analysis', 'inactive', 2);

-- Insert into JobRun Table
INSERT INTO purgo_playground.jobrun (job_run_id, job_id, batchrun_id, execution_date, status, log_message) VALUES
(701, 501, 101, DATE('2023-10-01'), 'success', 'Execution completed without errors'),
(702, 502, 102, DATE('2023-10-02'), 'failed', 'Failure due to XYZ reasons');

-- Insert into JobArguments Table
INSERT INTO purgo_playground.jobarguments (argument_id, job_id, argument_name, argument_value) VALUES
(801, 501, 'input_path', '/data/input/'),
(802, 502, 'output_format', 'csv');

/* Cleanup after tests to ensure environment is reset */
-- Drop tables after tests
DROP TABLE IF EXISTS purgo_playground.batch;
DROP TABLE IF EXISTS purgo_playground.batchrun;
DROP TABLE IF EXISTS purgo_playground.job;
DROP TABLE IF EXISTS purgo_playground.jobrun;
DROP TABLE IF EXISTS purgo_playground.jobarguments;
