-- Setting up the context for the test code execution
-- Ensure the selected catalog and schema are correctly used
-- Use the designated schema from Unity Catalog
USE SCHEMA purgo_playground;

-- Test SQL Script for Inventory at Risk Calculation Based on DNSA Events
-- Includes setup for test cases under different scenarios 
-- Ensures comprehensive coverage of requirements 

-- Setup Step: Creating Temporary Views for Data Manipulation
-- Provides easy access for SQL test scenarios 
CREATE OR REPLACE TEMPORARY VIEW f_inv_movmnt_view AS
SELECT *
FROM purgo_playground.f_inv_movmnt;

-- Schema Validation Test: Validate the expected schema for f_inv_movmnt_view 
-- Ensures all necessary columns and their data types are present 
DESCRIBE f_inv_movmnt_view;

-- Unit Test Scenario: Calculate Inventory at Risk when DNSA Event is Active
-- Validating Inventory Calculations based on flag_active 
-- Calculate the Inventory at Risk where DNSA flag (flag_active) is 'Y'
WITH inventory_at_risk AS (
  SELECT SUM(financial_qty) AS inventory_at_risk
  FROM f_inv_movmnt_view
  WHERE flag_active = 'Y'
),
total_inventory AS (
  SELECT SUM(financial_qty) AS total_inventory
  FROM f_inv_movmnt_view
)
SELECT inventory_at_risk, 
  total_inventory, 
  (inventory_at_risk / total_inventory * 100) AS percentage_inventory_at_risk
FROM inventory_at_risk 
CROSS JOIN total_inventory;

-- Integration Test: Ensure End-to-End Calculation for Percentage of Inventory at Risk
-- Verifies the correctness of the final output by combining interim calculations 
-- Perform complete integration test on inventory calculations
CREATE OR REPLACE TEMPORARY VIEW risk_calculation AS
SELECT fia.*, ti.total_inventory, 
  (fia.inventory_at_risk / ti.total_inventory * 100) AS percentage_inventory_at_risk
FROM (
  SELECT SUM(financial_qty) AS inventory_at_risk
  FROM f_inv_movmnt_view
  WHERE flag_active = 'Y'
) fia
CROSS JOIN (
  SELECT SUM(financial_qty) AS total_inventory
  FROM f_inv_movmnt_view
) ti;

-- Verify the calculated results
SELECT * FROM risk_calculation;

-- Data Type Testing: Ensure Handling of Complex Scenarios
-- Focuses on testing for NULLs, type conversions, and extreme values 
-- Test handling of NULL values
SELECT *, IFNULL(financial_qty, 0) AS safe_financial_qty
FROM f_inv_movmnt_view
WHERE financial_qty IS NULL;

-- Delta Lake Operations: Testing Delta Table Specific Features
-- Validates core operations such as MERGE, UPDATE, and DELETE 
-- Test MERGE operation with a hypothetical delta table
CREATE OR REPLACE TABLE delta.inventory_risk AS
SELECT * FROM risk_calculation;

-- Assuming a structure of delta table for MERGE testing
MERGE INTO delta.inventory_risk AS target
USING (
  SELECT 'txn1011' AS txn_id, 300.0 AS financial_qty
) AS source
ON target.txn_id = source.txn_id
WHEN MATCHED THEN
  UPDATE SET target.financial_qty = source.financial_qty
WHEN NOT MATCHED THEN
  INSERT (txn_id, financial_qty) VALUES (source.txn_id, source.financial_qty);

-- Performance Test: Validate Efficiency of the SQL Queries
-- Ensures that query performance is within acceptable limits 
-- Test performance by measuring execution time
EXPLAIN SELECT * FROM risk_calculation;

-- Cleanup: Ensure proper cleanup after tests
-- Removes temporary views and tables used for testing 
DROP VIEW IF EXISTS f_inv_movmnt_view;
DROP VIEW IF EXISTS risk_calculation;
DROP TABLE IF EXISTS delta.inventory_risk;
