-- Code for generating test data in Databricks SQL following the specified requirements

-- Create a temporary database for our test data
CREATE DATABASE IF NOT EXISTS test_db;

-- Create a table for inventory movement dataset
CREATE TABLE IF NOT EXISTS test_db.f_inv_movmnt (
    productId BIGINT,
    plantId STRING,
    inventoryDate TIMESTAMP,
    inventoryLevel DOUBLE
);

-- Insert random test data for the inventory movement dataset
INSERT INTO test_db.f_inv_movmnt VALUES
    (101, 'A1', '2024-03-01T00:00:00.000+0000', 100.0),
    (101, 'A1', '2024-03-15T00:00:00.000+0000', 150.0),
    (101, 'A1', '2024-03-30T00:00:00.000+0000', 120.0),
    (102, 'B2', '2024-03-01T00:00:00.000+0000', 200.0),  -- Normal scenario
    (102, 'B2', '2024-03-31T00:00:00.000+0000', 0.0),    -- Edge case: Zero inventory
    (NULL, 'B2', '2024-03-15T00:00:00.000+0000', NULL);  -- NULL handling

-- Create a table for sales data
CREATE TABLE IF NOT EXISTS test_db.f_sales (
    productId BIGINT,
    plantId STRING,
    saleDate TIMESTAMP,
    salesAmount DOUBLE
);

-- Insert random test data for the sales dataset
INSERT INTO test_db.f_sales VALUES
    (101, 'A1', '2024-03-01T00:00:00.000+0000', 10.0),
    (101, 'A1', '2024-03-15T00:00:00.000+0000', 5.0),
    (101, 'A1', '2024-03-30T00:00:00.000+0000', 8.0),
    (102, 'B2', '2024-03-01T00:00:00.000+0000', 20.0),  -- Valid scenario
    (NULL, 'B2', '2024-03-15T00:00:00.000+0000', NULL); -- NULL handling

-- Happy path test data for DOH calculation
-- Make sure to provide valid entries that will result in successful DOH calculations
-- Edge cases are also included to test computations at boundaries

-- Special characters and multi-byte character test
-- Use Unicode characters in plant IDs (not meaningful scenario but for testing character handling)
INSERT INTO test_db.f_sales VALUES
    (103, 'C¥', '2024-03-01T00:00:00.000+0000', 15.0);

-- Error case: Out-of-range value for inventoryLevel
INSERT INTO test_db.f_inv_movmnt VALUES
    (101, 'A1', '2024-03-20T00:00:00.000+0000', -500.0); -- Out-of-range

-- Ensure diversity in data by manipulating combinations of missing/incomplete records, zero, and null values
-- Each scenario is intended to validate specific aspects of your application's logic for processing inventory and sales data
